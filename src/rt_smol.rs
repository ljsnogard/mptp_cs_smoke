//! **smol** 装配：与另两个壳同构，差别在「谁来驱动本地队列」。
//!
//! smol **没有隐式运行时上下文**：本地队列是本线程 `thread_local!` 里的
//! `Rc<LocalExecutor<'static>>`，必须由调用方驱动。因此这里没有 `#[smol::main]` 之类的
//! 入口，bin 侧用 `smol::block_on` 把整段跑起来——它是外部阻塞驱动源，`run_until` 在其
//! 中顺带推进本线程的本地队列。
//!
//! 也因此，运行时值与作用域要在**进入驱动之前**取好再传进来。

use std::process::ExitCode;
use std::time::Duration;

use buffex_smol_adapt::DefaultAllocConfig;
use smux_v1::{
    connection::Dock,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::abs_art,
};

use abs_art::TrLocalScope;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_smux::conn::TrConnection;
use abs_buff::x_deps::abs_cancel;
use mm_ptr::Shared;
use mm_ptr::x_deps::abs_mm;
use smux_v1::x_deps::{abs_buff, abs_smux, mm_ptr};
use mptp_core::{
    serving::server::SessionContext,
    specs::Status,
};
use smol::net::{TcpListener, TcpStream};

use crate::{
    K_CHANNEL_CAP, PeerOptions, Rx, Tx, assert_runtime_, build_conn_, build_server_,
    cli::{Role, parse_env_args, ready_line, result_line},
    connect_with_retry_, run_client_, serve_one_channel_,
};

/// bin 的入口（**同步**：`block_on` 自己就是驱动源）。
pub fn run_blocking() -> ExitCode {
    let opts = match parse_env_args() {
        Result::Ok(opts) => opts,
        Result::Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2u8);
        }
    };
    let runtime = smux_v1::connection::default_rt_();
    if let Result::Err(err) = assert_runtime_(&runtime, opts.runtime) {
        eprintln!("运行失败：{err}");
        return ExitCode::FAILURE;
    }
    let scope = runtime.local_scope();
    match smol::block_on(run_peer_(&scope, &opts)) {
        Result::Ok(()) => ExitCode::SUCCESS,
        Result::Err(err) => {
            eprintln!("运行失败：{err}");
            ExitCode::FAILURE
        }
    }
}

/// 按角色跑一次。
async fn run_peer_<S>(scope: &S, opts: &PeerOptions) -> Result<(), Box<dyn std::error::Error>>
where
    S: TrLocalScope,
{
    match opts.role {
        Role::Server => {
            let listener = TcpListener::bind(opts.listen).await?;
            let actual = listener.local_addr()?;
            println!("{}", ready_line(opts, actual));
            let (stream, _peer) = listener.accept().await?;
            stream.set_nodelay(true)?;
            scope.run_until(serve_conn_(stream, opts.dock)).await
        }
        Role::Client => {
            let stream = connect_retry_(opts.peer).await?;
            stream.set_nodelay(true)?;
            let status = scope.run_until(request_conn_(stream, opts.dock)).await?;
            let ok = status == Status::Ok;
            println!("{}", result_line(opts, status.inner(), ok));
            Result::Ok(())
        }
    }
}

/// 被动端：造适配器 → 握手 `listen` → 建连接 → 监听 dock → 服务一条子流。
async fn serve_conn_(stream: TcpStream, dock: u32) -> Result<(), Box<dyn std::error::Error>> {
    // `async-net` 的 `TcpStream` 没有 `into_split`，用 `smol::io::split` 拆读写半边
    // （内部是一把短临界区的锁，两个半边仍指向同一条全双工连接）。
    let (read_half, write_half) = smol::io::split(stream);
    let rx = Rx::try_new(read_half, K_CHANNEL_CAP, DefaultAllocConfig)?;
    let tx = Tx::try_new_local(write_half, K_CHANNEL_CAP, DefaultAllocConfig)?;

    let handshake_opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(tx, rx)
        .listen_async(&handshake_opts, AcceptAllEntries)
        .await
        .map_err(|err| format!("被动端握手失败：{err:?}"))?;
    let conn = build_conn_(delivery);

    let server = build_server_::<Tx, Rx>();
    let mut binding = conn
        .bind_async(Dock::new(dock))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| format!("绑定 dock {dock} 失败：{err}"))?;
    let mut context = SessionContext;
    serve_one_channel_(&server, &mut binding, &mut context).await?;
    Result::Ok(())
}

/// 主动端：造适配器 → 握手 `invite` → 建连接 → 发一次请求并读回状态。
async fn request_conn_(stream: TcpStream, dock: u32) -> Result<Status, Box<dyn std::error::Error>> {
    let (read_half, write_half) = smol::io::split(stream);
    let rx = Rx::try_new(read_half, K_CHANNEL_CAP, DefaultAllocConfig)?;
    let tx = Tx::try_new_local(write_half, K_CHANNEL_CAP, DefaultAllocConfig)?;

    let handshake_opts = BasicOpts::default();
    let delivery = HandshakeAgent::new(tx, rx)
        .invite_async(&handshake_opts, AcceptAllEntries)
        .await
        .map_err(|err| format!("主动端握手失败：{err:?}"))?;
    let conn = build_conn_(delivery);
    let shared = Shared::new(conn, CoreAlloc);
    let status = run_client_::<Tx, Rx>(shared, dock).await?;
    Result::Ok(status)
}

/// 带重试地连接对端。
async fn connect_retry_(addr: std::net::SocketAddr) -> Result<TcpStream, Box<dyn std::error::Error>> {
    connect_with_retry_(
        100usize,
        || TcpStream::connect(addr),
        || async {
            let _ = smol::Timer::after(Duration::from_millis(100)).await;
        },
    )
    .await
    .map_err(|err| Box::new(err) as Box<dyn std::error::Error>)
}
