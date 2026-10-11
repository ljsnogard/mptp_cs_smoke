//! 进程内的复用连接装配。
//!
//! 两条全被动传输环分别承载 A→B 与 B→A，四个环半部直接交给两个 `MuxConnection`
//! 当作 `Rx` / `Tx`，中间**没有任何 socket、也没有泵**。这样端到端验证只覆盖
//! 「mptp_core 的协议行为」，不会被传输层的未知问题干扰。
//!
//! 所有依赖都从 `smux_v1::x_deps` 取：本 demo 只声明 `smux_v1` 一个实现依赖，
//! 避免同一个 crate 出现第二个来源、或后端 feature 出现第二个决定点。

use core::mem::MaybeUninit;

use abs_mm::CoreAlloc;
#[cfg(feature = "rt-tokio")]
use anyhow::{Result, anyhow};
use mptp_core::x_deps::mm_ptr;
use mm_ptr::Owned;
use mm_ptr::x_deps::abs_mm;
use smux_v1::x_deps::abs_buff;
#[cfg(feature = "rt-tokio")]
use smux_v1::handshake::{
    agent::{AcceptAllEntries, HandshakeAgent},
    opts::BasicOpts,
};

use smux_v1::{
    connection::{DefaultConnCfg, MuxChanBuffOwnedBy, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::agent::HandshakeDelivery,
};

#[cfg(feature = "rt-tokio")]
use smux_v1::connection::{BufferedRx, BufferedTx, new_buffered_channel};
#[cfg(feature = "rt-tokio")]
use smux_v1::x_deps::abs_art;

use abs_buff::{TrBuffRead, TrBuffWrite};

#[cfg(feature = "rt-tokio")]
use abs_art::TrLocalScope;

/// 传输环容量（字节）：只影响吞吐，不影响正确性。
#[cfg(feature = "rt-tokio")]
const K_TRANSPORT_CAP: usize = 64usize * 1024usize;

/// 连接级帧暂存容量（字节）。
const K_STAGE_CAP: usize = 64usize * 1024usize;

/// 一条子流向一个方向的环容量（字节）。
pub const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 服务端监听的 dock（演示里客户端也打到这个 dock）。
pub const K_LISTEN_DOCK: u32 = 1u32;

/// 入向邀请最多同时挂起多少条。
pub const K_LISTEN_RESERVE: usize = 8usize;

/// 子流 ring 存储的拥有者类型。
pub type DemoRingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 连接级帧暂存的拥有者类型。
type StageBuff = MuxChanBuffOwnedBy<DemoRingBuff>;

/// 本 demo 使用的连接配置。
///
/// 泛型参数就是**传输的两个半边**：进程内环回用 [`BufferedTx`] / [`BufferedRx`]，
/// 真实 socket 用各运行时的设备适配器（`buffex_tokio_adapt::BuffWrite` 等）。三者是
/// 不同的具体类型，因此配置、连接、以及两侧的配置实现都按它参数化——这样同一份协议
/// 场景代码可以在三个后端上逐字复用。
pub type DemoMuxCfg<Tx, Rx> = DefaultConnCfg<Tx, Rx>;

/// 本 demo 使用的复用连接。
pub type DemoConn<Tx, Rx> = MuxConnection<DemoMuxCfg<Tx, Rx>>;

/// 进程内环回用的传输半边（写）。
#[cfg(feature = "rt-tokio")]
pub type LocalTx = BufferedTx;

/// 进程内环回用的传输半边（读）。
#[cfg(feature = "rt-tokio")]
pub type LocalRx = BufferedRx;

/// 进程内环回用的复用连接。
#[cfg(feature = "rt-tokio")]
pub type LocalConn = DemoConn<LocalTx, LocalRx>;

/// 造一条全被动传输环，返回 `(写半边, 读半边)`。
#[cfg(feature = "rt-tokio")]
fn make_transport_ring_() -> Result<(BufferedTx, BufferedRx)> {
    let owner = Owned::new_uninit_slice(K_TRANSPORT_CAP, CoreAlloc);
    new_buffered_channel(owner).map_err(|err| anyhow!("建传输环失败: {err:?}"))
}

/// 造一对连接级帧暂存缓冲。
fn make_stage_buffs_() -> (StageBuff, StageBuff) {
    (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc)),
    )
}

/// 为一条子流造出 `(发送方向, 接收方向)` 两块 ring 内存。
///
/// 客户端与服务端两套分配配置都转发到这里，保证两侧的容量与来源完全一致。
pub fn make_channel_buffs_() -> (DemoRingBuff, DemoRingBuff) {
    (
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
    )
}

/// 在进程内建一对互连的复用连接（A 主动、B 被动）。
///
/// **只在 tokio 构建下存在**：见 [`crate::run_local_roundtrip_`] 的说明。
///
/// # 为什么连接要被托管在**另一个线程**上
///
/// `abs_buff` 的环是全被动的：环里的数据要靠 `smux_v1` 那五个读 / 写循环搬进搬出，
/// 而那些循环经 `TrLocalScope` 投递到**本线程**的本地队列（tokio 下即 `LocalSet`），
/// 只能由同一线程上的 `run_until` 驱动。
///
/// 应用侧读环时走的是 `AsStdRead`——一个同步的 `std::io::Read`，内部 `block_on`
/// 会把当前线程占住。**如果连接循环和应用在同一线程，队列就再也不会被驱动**，
/// 于是「等环里有数据」等的是一个没人喂的环：这是闭合的死锁，`run_until` 嵌套、
/// `park_timeout` 都救不了（都实测过）。
///
/// 所以这里把连接交给一个**专用线程**：它在自己的 tokio 运行时里拿到本地队列，
/// 用 `run_until` 一直驱动到进程结束；连接对象本身（`MuxConnection` 是
/// `Send + Sync` 的智能指针）经 channel 交回调用线程。调用线程此后同步读写，
/// 而循环在宿主线程上继续跑——这正是 `AsStdRead / AsStdWrite` 能工作的前提。
#[cfg(feature = "rt-tokio")]
pub async fn connect_pair_() -> Result<(LocalConn, LocalConn)> {
    tokio::task::spawn_blocking(host_pair_)
        .await
        .map_err(|err| anyhow!("连接的宿主线程 panic: {err}"))?
}

/// 在**专用线程**上建连接并长期托管它的循环，把连接对象交回调用线程。
#[cfg(feature = "rt-tokio")]
fn host_pair_() -> Result<(LocalConn, LocalConn)> {
    let (tx, rx) = std::sync::mpsc::channel::<Result<(LocalConn, LocalConn)>>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("建宿主线程的 tokio 运行时");
        rt.block_on(async move {
            let runtime = smux_v1::connection::default_rt_();
            let scope = runtime.local_scope();
            // 整段都跑在 `run_until` 里：握手要它驱动，连接建成之后的五个循环
            // 也继续靠它驱动（末尾那个 `pending` 就是为了不提前退出）。
            scope
                .run_until(async move {
                    let pair = connect_pair_inner_().await;
                    let _ = tx.send(pair);
                    core::future::pending::<()>().await;
                })
                .await;
        });
    });
    rx.recv().map_err(|_| anyhow!("连接宿主线程提前退出"))?
}

/// 真正的建连：两条全被动传输环 + 本地握手。
#[cfg(feature = "rt-tokio")]
async fn connect_pair_inner_() -> Result<(LocalConn, LocalConn)> {
    // 每条环拆成 (写半边, 读半边)，**交叉**交给两端：A 的写接到 B 的读，反之亦然。
    let (a_tx, b_rx) = make_transport_ring_()?;
    let (b_tx, a_rx) = make_transport_ring_()?;

    let opts = BasicOpts::default();
    let invite = HandshakeAgent::new(a_tx, a_rx).invite_async(&opts, AcceptAllEntries);
    let listen = HandshakeAgent::new(b_tx, b_rx).listen_async(&opts, AcceptAllEntries);
    let (invited, accepted) = tokio::join!(invite, listen);
    let delivery_a = invited.map_err(|err| anyhow!("发起方握手失败: {err:?}"))?;
    let delivery_b = accepted.map_err(|err| anyhow!("等待方握手失败: {err:?}"))?;

    // 运行时值与作用域都由配置自己解决（`TrConnCfg::runtime` + `ScopeHost`），
    // 因此这里只需要给出一个流控策略。
    let (delivery_a, cfg_a) = DefaultConnCfg::new(delivery_a, DefaultPolicy);
    let (delivery_b, cfg_b) = DefaultConnCfg::new(delivery_b, DefaultPolicy);

    let (a_sr, a_sw) = make_stage_buffs_();
    let (b_sr, b_sw) = make_stage_buffs_();
    Result::Ok((
        MuxConnection::new(delivery_a, cfg_a, a_sr, a_sw),
        MuxConnection::new(delivery_b, cfg_b, b_sr, b_sw),
    ))
}

/// 由**已经完成握手**的交付物造出一条复用连接。
///
/// socket 两侧共用：主动端跑 `invite_async`、被动端跑 `listen_async`，各自拿到
/// [`HandshakeDelivery`] 之后走这里。运行时值与作用域由配置自己解决
/// （`TrConnCfg::runtime` + `ScopeHost`），因此这里只需要一个流控策略。
///
/// # Errors
///
/// 暂存缓冲的分配失败时返回错误。
pub fn build_conn_<Tx, Rx>(delivery: HandshakeDelivery<Tx, Rx>) -> DemoConn<Tx, Rx>
where
    Tx: TrBuffWrite<u8> + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
    let (read_buff, write_buff) = make_stage_buffs_();
    MuxConnection::new(delivery, cfg, read_buff, write_buff)
}
