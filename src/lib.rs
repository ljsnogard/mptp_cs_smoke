#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

//! `mptp_core` 的端到端演示：进程内环回，以及真实 TCP socket 上跨运行时的互通。
//!
//! # 这个 crate 验什么
//!
//! 两个进程（**同一份源码**按运行时 feature 编译出的两个可执行文件）在**真实 TCP
//! socket** 上互联，一个服务端、一个客户端，跑一次完整的 MPTP 往返：
//!
//! ```text
//! client ──dial──▶ server      握手 invite / listen
//!   └── 开一条子流 ──▶           View /hello
//!   ◀── 响应前缀                   200 OK
//! ```
//!
//! 两侧可以是**不同的运行时**（tokio / compio / smol 的任意组合）——这正是本 demo 要
//! 覆盖的形态。与运行时无关的协议场景全在 [`client_`] / [`server_`] 里，每个后端只有
//! 一份「建 socket、造适配器、握手」的薄壳（[`rt_tokio`] 等）。
//!
//! # 进程内模式
//!
//! [`run_local_roundtrip_`] 保留原来的进程内环回：两条全被动内存环直连两个端点，不碰
//! 网络，用来把 `mptp_core` 的协议行为与传输层的问题隔离开。它固定跑在 tokio 上。
//!
//! # 为什么选运行时是编译期的事
//!
//! 本 crate 的 `rt-tokio` / `rt-compio` / `rt-smol` 是**互斥三选一**：每个恰好打开
//! `smux_v1` 的一个 `test-*-runtime`，而 [`Runtime`]（= `smux_v1::connection::DefaultRt_`）
//! 就按它选出当前装配的后端。多开一个会让该别名没有唯一解，因此换运行时必须带
//! `--no-default-features`。`--runtime` 只用于**自检**：与编译期后端不一致就立刻报错退出
//! （见 [`assert_runtime_`]），不是进程内切换运行时。这也是本 crate 必须是**独立
//! workspace** 的原因——见 README。
//!
//! # 怎么跑
//!
//! ```bash
//! just local          # 进程内环回（不需要网络）
//! just pairs          # 本机 127.0.0.1 环回，3 组运行时配对
//! just one tokio compio
//! ```
//!
//! 手打命令行的方式见 [`cli::K_HELP`] 与 README。

pub mod cli;
pub mod client_;
pub mod mux_;
pub mod server_;

#[cfg(feature = "rt-tokio")]
pub mod rt_tokio;

#[cfg(feature = "rt-compio")]
pub mod rt_compio;

#[cfg(feature = "rt-smol")]
pub mod rt_smol;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 传输的两个半边：由**设备适配器**直接提供，按 feature 各写一份
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 连接侧**写半边**（环 → socket）：`buffex_*_adapt` 的出向适配器。
///
/// 用 `try_new_local`（tokio / smol）或 `try_new`（compio）构造：前者把「环 → 设备」的泵
/// 挂在本线程的本地队列上自驱动，连接侧只要把帧提交进环就一定会上网，不需要谁记得
/// flush。
#[cfg(feature = "rt-tokio")]
pub type Tx = buffex_tokio_adapt::BuffWrite<tokio::net::tcp::OwnedWriteHalf>;

/// 连接侧**读半边**（socket → 环）：`buffex_*_adapt` 的入向适配器。
#[cfg(feature = "rt-tokio")]
pub type Rx = buffex_tokio_adapt::BuffRead<tokio::net::tcp::OwnedReadHalf>;

/// 连接侧**写半边**（环 → socket）。
#[cfg(feature = "rt-compio")]
pub type Tx = buffex_compio_adapt::BuffWrite<compio::net::TcpStream>;

/// 连接侧**读半边**（socket → 环）。
#[cfg(feature = "rt-compio")]
pub type Rx = buffex_compio_adapt::BuffRead<compio::net::TcpStream>;

/// 连接侧**写半边**（环 → socket）。
#[cfg(feature = "rt-smol")]
pub type Tx = buffex_smol_adapt::BuffWrite<smol::io::WriteHalf<smol::net::TcpStream>>;

/// 连接侧**读半边**（socket → 环）。
#[cfg(feature = "rt-smol")]
pub type Rx = buffex_smol_adapt::BuffRead<smol::io::ReadHalf<smol::net::TcpStream>>;

/// 本构建所用运行时的复用连接类型。
pub type Conn = mux_::DemoConn<Tx, Rx>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 编译期守卫：恰好一个运行时后端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(not(any(feature = "rt-tokio", feature = "rt-compio", feature = "rt-smol")))]
compile_error!(
    "mptp_cs_smoke：必须启用恰好一个运行时 feature（rt-tokio / rt-compio / rt-smol）；\
     例如 `cargo build --no-default-features --features rt-tokio --bin peer_tokio`"
);

#[cfg(any(
    all(feature = "rt-tokio", feature = "rt-compio"),
    all(feature = "rt-tokio", feature = "rt-smol"),
    all(feature = "rt-compio", feature = "rt-smol"),
))]
compile_error!(
    "mptp_cs_smoke：三个运行时 feature 互斥，一次构建只能启用一个；\
     换运行时请加 `--no-default-features`"
);

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 对外导出
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub use cli::{K_HELP, PeerOptions, Role, RuntimeSel, parse_env_args};
// `DemoCfgError` 随 `TrClientConfig::Err` 一并移除：当前的 `mptp_core` 已不再有
// 「配置层错误」这个关联类型，本 crate 也就没有理由再导出一个没有使用者的错误类型。
pub use client_::{DemoClientAllocCfg, DemoClientCfg, run_client_};
pub use mux_::{
    DemoConn, DemoMuxCfg, DemoRingBuff, K_CHANNEL_CAP, K_LISTEN_DOCK, K_LISTEN_RESERVE,
    build_conn_,
};
#[cfg(feature = "rt-tokio")]
pub use mux_::{LocalConn, connect_pair_};
pub use server_::{DemoServingAllocCfg, DemoServingCfg, build_server_, serve_one_channel_};

#[cfg(feature = "rt-tokio")]
use anyhow::{Result, anyhow};

use abs_art::TrAsyncRuntime;
#[cfg(feature = "rt-tokio")]
use abs_smux::conn::TrConnection;
use smux_v1::x_deps::abs_art;
#[cfg(feature = "rt-tokio")]
use smux_v1::x_deps::abs_smux;

#[cfg(feature = "rt-tokio")]
use smux_v1::x_deps::abs_buff;

#[cfg(feature = "rt-tokio")]
use abs_buff::x_deps::abs_cancel;
#[cfg(feature = "rt-tokio")]
use abs_cancel::{NonCancellableToken, TrMayCancel};
#[cfg(feature = "rt-tokio")]
use mm_ptr::x_deps::abs_mm;
#[cfg(feature = "rt-tokio")]
use smux_v1::x_deps::mm_ptr;
#[cfg(feature = "rt-tokio")]
use smux_v1::connection::Dock;
#[cfg(feature = "rt-tokio")]
use abs_mm::CoreAlloc;
#[cfg(feature = "rt-tokio")]
use mm_ptr::Shared;
#[cfg(feature = "rt-tokio")]
use mptp_core::{serving::server::SessionContext, specs::Status};

/// 本构建所用运行时的**运行时值**类型。
///
/// 取 `smux_v1` 的默认后端别名：它由 `smux_v1` 的 `test-*-runtime` **三选一**决定，正好
/// 是本 crate 的 `rt-*` feature 打开的那一个。**不能**取 bridge 的裸名——bridge 的缺省
/// 后端（compio）始终在线，裸名在 tokio / smol 装配下会指向另一个运行时。
pub type Runtime = smux_v1::connection::DefaultRt_;

/// 核对「命令行声明的运行时」与「编译期选定的后端」是否一致。
///
/// # 为什么要有这一步
///
/// 运行时是编译期定死的，`--runtime` 无法在进程内切换它；能骗过人的只有「拿错二进制
/// 去跑」这一种情况（例如把 `peer_compio` 当成 `peer_tokio` 传）。因此这里拿运行时值
/// 自报的身份与命令行比一次，不一致就**立刻**失败。
///
/// # Errors
///
/// 命令行声明的运行时与编译期后端不一致时返回说明文本。
pub fn assert_runtime_(
    rt: &Runtime,
    expected: cli::RuntimeSel,
) -> Result<(), Box<dyn std::error::Error>> {
    let actual = rt.about();
    if actual == expected.tag() {
        return Result::Ok(());
    }
    Result::Err(format!(
        "--runtime {} 与编译期后端 {actual:?} 不一致：这个可执行文件是按 {actual:?} 编译的，\
         请改用 `{}`，或用 `--no-default-features --features rt-{}` 重新编译",
        expected.name(),
        expected.bin(),
        expected.name()
    )
    .into())
}

/// 带重试地连接对端：两端启动有先后，先起的一端可能还没 bind 上。
///
/// `attempt` 是「连一次」的闭包，`pause` 是两次尝试之间的等待（由各运行时给出，因为
/// 「等一会儿」在不同运行时上是不同的原语）。
///
/// # Errors
///
/// 用尽 `tries` 次尝试仍然失败时，返回最后一次的错误。
pub async fn connect_with_retry_<F, Fut, T, P, PF, E>(
    tries: usize,
    mut attempt: F,
    mut pause: P,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T, E>>,
    P: FnMut() -> PF,
    PF: core::future::Future<Output = ()>,
{
    let mut last: Option<E> = Option::None;
    for _ in 0..tries.max(1usize) {
        match attempt().await {
            Result::Ok(ok) => return Result::Ok(ok),
            Result::Err(err) => {
                last = Option::Some(err);
                pause().await;
            }
        }
    }
    Result::Err(last.expect("tries >= 1 时至少尝试过一次"))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 进程内模式
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 跑一次完整的**进程内**往返：
/// 建一对连接 → 服务端在 [`K_LISTEN_DOCK`] 上监听并服务一条子流 →
/// 客户端向同一个 dock 发起 `View /hello` → 返回对端给出的状态码。
///
/// # 为什么把两端拆到两个线程
///
/// 服务端一进入同步读就会把它所在的线程占住；若与客户端挤在同一个任务（`join!`）里，
/// 客户端再也等不到建流的最终裁决。而会话的收发半边是 `!Send` 的，`tokio::spawn` 用不了
/// ——`rt.block_on` 只要求 future 在**当前线程**上跑，正好合适。连接本身的五条循环在
/// `mux_` 的宿主线程上跑，与应用线程分开，因此应用侧的同步等待不会把供料方一起憋死。
///
/// 本函数固定跑在 tokio 上（多线程 flavor）；跨运行时的形态见 [`rt_tokio`] 一族。
///
/// # Errors
///
/// 建连、绑定 dock、建流、解码、路由或写回任一步失败都会带上下文返回 `Err`。
#[cfg(feature = "rt-tokio")]
pub async fn run_local_roundtrip_() -> Result<Status> {
    let (conn_a, conn_b) = connect_pair_().await?;
    let server = build_server_::<mux_::LocalTx, mux_::LocalRx>();
    let client_conn = Shared::new(conn_a, CoreAlloc);

    let server_thread = std::thread::spawn(move || -> Result<()> {
        let rt = build_runtime_()?;
        rt.block_on(async move {
            let mut binding = conn_b
                .bind_async(Dock::new(K_LISTEN_DOCK))
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow!("服务端绑定 dock 失败: {err}"))?;
            let mut context = SessionContext;
            serve_one_channel_(&server, &mut binding, &mut context).await
        })
    });
    let client_thread = std::thread::spawn(move || -> Result<Status> {
        let rt = build_runtime_()?;
        rt.block_on(run_client_::<mux_::LocalTx, mux_::LocalRx>(
            client_conn,
            K_LISTEN_DOCK,
        ))
    });

    let server_res = server_thread
        .join()
        .map_err(|_| anyhow!("服务端线程 panic"))?;
    let client_res = client_thread
        .join()
        .map_err(|_| anyhow!("客户端线程 panic"))?;
    server_res?;
    client_res
}

/// 每个应用线程自己的 tokio 运行时。
#[cfg(feature = "rt-tokio")]
fn build_runtime_() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|err| anyhow!("建 tokio 运行时失败: {err}"))
}
