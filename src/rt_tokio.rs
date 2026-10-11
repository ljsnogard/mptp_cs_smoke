//! **tokio** 装配：TCP socket + `buffex_tokio_adapt` 的读写适配器 + 一次 cs 往返。
//!
//! # 为什么 socket 与连接循环要独占一个**宿主线程**
//!
//! `mptp_core` 的收发走 `AsStdRead / AsStdWrite`——同步的 `std::io` 适配器，内部靠
//! `block_on_local_` 把「等数据」变成一次同步等待，而那会**把当前线程占住**。
//!
//! 连接的五个循环（读写泵、解复用等）投递在**本线程的本地队列**上，socket 的 IO 又由
//! 这个 runtime 的 IO driver 驱动。三者若挤在同一条 `current_thread` runtime 上，同步
//! 等待一发生 IO driver 就停摆——socket 的读写永远不会完成，连「把 OPEN 帧发出去」都做
//! 不到（实测：客户端 `open` / `accept` 全部成功，服务端却始终收不到建流请求）。
//!
//! 所以这里分两层，与进程内模式同构：
//!
//! | 线程 | 持有 | 干什么 |
//! | --- | --- | --- |
//! | **宿主线程**（本文件新建） | socket + 适配器 + 连接的五条循环 | `run_until` 一直驱动到进程结束 |
//! | **应用线程**（bin 的 `#[tokio::main]`） | 只持连接对象 | `bind` / `serve` 或 `request` / `recv` |
//!
//! socket 全程不跨 runtime：bind / accept / connect 都在宿主线程里完成，地址与连接经
//! channel 交回应用线程。`MuxConnection` 是 `Send + Sync` 的智能指针，跨线程传递安全。

use std::{
    net::SocketAddr,
    process::ExitCode,
    sync::mpsc::{Receiver, channel},
    time::Duration,
};

use buffex_tokio_adapt::DefaultAllocConfig;
use smux_v1::{
    connection::Dock,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};
use tokio::net::{TcpListener, TcpStream};

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use mm_ptr::{Shared, x_deps::abs_mm};
use smux_v1::x_deps::{abs_art, abs_buff, abs_smux, mm_ptr};

use abs_art::TrLocalScope;
use abs_mm::CoreAlloc;
use abs_smux::conn::TrConnection;
use mptp_core::{serving::server::SessionContext, specs::Status};

use crate::{
    Conn, K_CHANNEL_CAP, PeerOptions, Rx, Tx, assert_runtime_, build_conn_, build_server_,
    cli::{Role, parse_env_args, ready_line, result_line},
    connect_with_retry_, run_client_, serve_one_channel_,
};

/// bin 的入口：解析参数 → 跑一次往返 → 打印结果行 → 给出退出码。
pub async fn run_async() -> ExitCode {
    let opts = match parse_env_args() {
        Result::Ok(opts) => opts,
        Result::Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2u8);
        }
    };
    match run_peer_(&opts).await {
        Result::Ok(()) => ExitCode::SUCCESS,
        Result::Err(err) => {
            eprintln!("运行失败：{err}");
            ExitCode::FAILURE
        }
    }
}

/// 按角色跑一次：服务端监听并服务一条子流，客户端拨号并发一次请求。
async fn run_peer_(opts: &PeerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = smux_v1::connection::default_rt_();
    assert_runtime_(&runtime, opts.runtime)?;

    // 宿主线程包办 socket 与连接；本线程只拿地址与连接对象。
    let (actual_rx, conn_rx) = spawn_host_(
        opts.role,
        opts.listen,
        opts.peer,
        opts.dock,
        opts.tag.clone(),
        opts.runtime,
    );

    // 服务端绑好之后立刻告知编排器：主动端可以起来了。
    if opts.role == Role::Server {
        let actual = recv_on_blocking_(actual_rx)?;
        println!("{}", ready_line(opts, actual));
    }
    let conn = recv_on_blocking_(conn_rx)?;

    match opts.role {
        Role::Server => {
            let mut binding = conn
                .bind_async(Dock::new(opts.dock))
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| format!("绑定 dock {} 失败：{err}", opts.dock))?;
            let mut context = SessionContext;
            let server = build_server_::<Tx, Rx>();
            serve_one_channel_(&server, &mut binding, &mut context).await?;
            Result::Ok(())
        }
        Role::Client => {
            let shared = Shared::new(conn, CoreAlloc);
            let status = run_client_::<Tx, Rx>(shared, opts.dock).await?;
            let ok = status == Status::Ok;
            println!("{}", result_line(opts, status.inner(), ok));
            Result::Ok(())
        }
    }
}

/// 从宿主线程的 channel 上收一个值（用 `spawn_blocking` 收，避免占住 worker）。
fn recv_on_blocking_<T>(rx: Receiver<Result<T, String>>) -> Result<T, Box<dyn std::error::Error>>
where
    T: Send + 'static,
{
    rx.recv()
        .map_err(|_| "宿主线程提前退出".to_string())?
        .map_err(|message| message.into())
}

/// 宿主线程回传的两条 channel：**先**是实际地址（服务端据此打印 `ready`），**再**是连接。
type HostChannels = (
    Receiver<Result<SocketAddr, String>>,
    Receiver<Result<Conn, String>>,
);

/// 在**宿主线程**上完成「socket + 适配器 + 握手 + 建连接」，并一直驱动连接。
///
/// 返回两个 channel：第一个在 `bind` 成功后立刻给出实际监听地址（服务端用它打印 ready
/// 行），第二个给出连接对象。宿主线程此后一直挂在自己的 `run_until` 里，直到进程退出。
fn spawn_host_(
    role: Role,
    listen: SocketAddr,
    peer: SocketAddr,
    dock: u32,
    tag: String,
    runtime_sel: crate::cli::RuntimeSel,
) -> HostChannels {
    let (actual_tx, actual_rx) = channel::<Result<SocketAddr, String>>();
    let (conn_tx, conn_rx) = channel::<Result<Conn, String>>();

    std::thread::spawn(move || {
        let host_opts = PeerOptions {
            role,
            runtime: runtime_sel,
            listen,
            peer,
            dock,
            tag,
        };
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Result::Ok(rt) => rt,
            Result::Err(err) => {
                let message = format!("建宿主线程运行时失败：{err}");
                let _ = actual_tx.send(Result::Err(message.clone()));
                let _ = conn_tx.send(Result::Err(message));
                return;
            }
        };
        rt.block_on(async move {
            let art = smux_v1::connection::default_rt_();
            let scope = art.local_scope();
            scope
                .run_until(async move {
                    // 1. 备好端点。服务端在这里**只 bind**：`ready` 的语义是「已经 bind
                    //    成功」，编排器要等它才起客户端（见 [`Endpoint_`]）。
                    let endpoint = match bind_or_connect_(&host_opts).await {
                        Result::Ok(endpoint) => endpoint,
                        Result::Err(message) => {
                            let _ = actual_tx.send(Result::Err(message.clone()));
                            let _ = conn_tx.send(Result::Err(message));
                            return;
                        }
                    };
                    let actual = endpoint.addr_();
                    if let Result::Err(message) = actual_tx.send(Result::Ok(actual)) {
                        let _ = conn_tx.send(Result::Err(format!("地址无人接收：{message}")));
                        return;
                    }

                    // 2. 服务端到这里才 accept——此刻编排器已经可以放客户端过来了。
                    let stream = match endpoint.into_stream_().await {
                        Result::Ok(stream) => stream,
                        Result::Err(message) => {
                            let _ = conn_tx.send(Result::Err(message));
                            return;
                        }
                    };

                    // 3. 适配器 + 握手 + 建连接。
                    let built = build_conn_on_socket_(stream, host_opts.role).await;
                    match built {
                        Result::Ok(conn) => {
                            let _ = conn_tx.send(Result::Ok(conn));
                        }
                        Result::Err(message) => {
                            let _ = conn_tx.send(Result::Err(message));
                        }
                    }

                    // 4. 连接建成之后五个循环仍要靠这条队列驱动，故一直挂着。
                    core::future::pending::<()>().await;
                })
                .await;
        });
    });

    (actual_rx, conn_rx)
}

/// 一步「备好端点」的结果。
///
/// # 为什么服务端要分成「bind」与「accept」两步
///
/// 编排器（`scripts/run_pairs.py`）的顺序是「起服务端 → 等它的 `ready` 行 → 才起
/// 客户端」。因此 `ready` 的语义必须是「**已经 bind 成功**」，而不是「已经完成一次
/// accept」——后者会让服务端等客户端连、编排器等 `ready`、客户端等编排器放行，三方互等，
/// 对以 tokio 为服务端的每一组配对都表现为永久挂起。
///
/// 客户端方向没有 `ready`，但同样走这个枚举：它的端点一步就绪。
enum Endpoint_ {
    /// 服务端：已 bind 的监听器，以及它的实际地址（**尚未 accept**）。
    Listening(TcpListener, SocketAddr),
    /// 客户端：已连上的流，以及它的本地地址。
    Connected(TcpStream, SocketAddr),
}

impl Endpoint_ {
    /// 本端点要上报给编排器的地址（服务端用它打印 `ready`）。
    fn addr_(&self) -> SocketAddr {
        match self {
            Endpoint_::Listening(_, addr) | Endpoint_::Connected(_, addr) => *addr,
        }
    }

    /// 等到对端就位：服务端在**这里**才 `accept`；客户端直接交出已连的流。
    async fn into_stream_(self) -> Result<TcpStream, String> {
        match self {
            Endpoint_::Listening(listener, _) => {
                let (stream, _peer) = listener
                    .accept()
                    .await
                    .map_err(|err| format!("接受连接失败：{err}"))?;
                stream
                    .set_nodelay(true)
                    .map_err(|err| format!("设置 nodelay 失败：{err}"))?;
                Result::Ok(stream)
            }
            Endpoint_::Connected(stream, _) => Result::Ok(stream),
        }
    }
}

/// 服务端只 bind（**不 accept**）；客户端 connect（带重试）。
async fn bind_or_connect_(opts: &PeerOptions) -> Result<Endpoint_, String> {
    match opts.role {
        Role::Server => {
            let listener = TcpListener::bind(opts.listen)
                .await
                .map_err(|err| format!("监听 {} 失败：{err}", opts.listen))?;
            let actual = listener
                .local_addr()
                .map_err(|err| format!("取监听地址失败：{err}"))?;
            Result::Ok(Endpoint_::Listening(listener, actual))
        }
        Role::Client => {
            let stream = connect_retry_(opts.peer).await.map_err(|err| err.to_string())?;
            stream
                .set_nodelay(true)
                .map_err(|err| format!("设置 nodelay 失败：{err}"))?;
            let actual = stream
                .local_addr()
                .map_err(|err| format!("取本地地址失败：{err}"))?;
            Result::Ok(Endpoint_::Connected(stream, actual))
        }
    }
}

/// 在宿主线程里造适配器、跑握手、建出连接。
async fn build_conn_on_socket_(stream: TcpStream, role: Role) -> Result<Conn, String> {
    let (read_half, write_half) = stream.into_split();
    let rx = Rx::try_new(read_half, K_CHANNEL_CAP, DefaultAllocConfig)
        .map_err(|err| format!("造读半边失败：{err}"))?;
    // 写侧走**自驱动**形态：泵挂在本线程的本地队列上，帧一提交进环就会上网。
    let tx = Tx::try_new_local(write_half, K_CHANNEL_CAP, DefaultAllocConfig)
        .map_err(|err| format!("造写半边失败：{err}"))?;

    let handshake_opts = BasicOpts::default();
    let delivery = match role {
        Role::Server => {
            HandshakeAgent::new(tx, rx)
                .listen_async(&handshake_opts, AcceptAllEntries)
                .await
        }
        Role::Client => {
            HandshakeAgent::new(tx, rx)
                .invite_async(&handshake_opts, AcceptAllEntries)
                .await
        }
    }
    .map_err(|err| format!("握手失败：{err:?}"))?;
    Result::Ok(build_conn_(delivery))
}

/// 带重试地连接对端：两端启动有先后，先起的一端可能还没 bind 上。
async fn connect_retry_(addr: SocketAddr) -> Result<TcpStream, Box<dyn std::error::Error>> {
    connect_with_retry_(
        100usize,
        || TcpStream::connect(addr),
        || tokio::time::sleep(Duration::from_millis(100)),
    )
    .await
    .map_err(|err| Box::new(err) as Box<dyn std::error::Error>)
}
