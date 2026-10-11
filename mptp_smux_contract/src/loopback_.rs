//! 进程内环回连接工厂：两条全被动传输环 + 本地握手，两端各一条 `MuxConnection`。
//!
//! # 为什么要自己造连接，而不复用 `mptp_cs_smoke` 的
//!
//! 本 crate 要检验的是「`mptp_core` 与 `smux_v1` 的约定是否一致」，因此**被测对象
//! 在两端都必须可控**：连接的握手协商项（`BasicOpts`）要能按用例调小，才能把「宽限期」
//! 与「空闲超时」压到秒级以内观察。`mptp_cs_smoke` 的 `connect_pair_` 固定用
//! `BasicOpts::default()`（30 秒空闲超时），不适合做这件事。所以这里自己拥有一份最小的
//! 装配逻辑——它是**测试脚手架**，不是第二份产品实现。
//!
//! # 为什么连接要托管在**另一个线程**
//!
//! `abs_buff` 的环是全被动的：数据要靠 `smux_v1` 的读 / 写循环搬进搬出，而那些循环经
//! `TrLocalScope` 投递到**本线程**的本地队列（tokio 下即 `LocalSet`），只能由同一线程上
//! 的 `run_until` 驱动。应用侧读环若走同步适配器会把线程占住，于是队列再也不会被驱动
//! （闭合死锁，`mptp_cs_smoke` 的 `mux_` 与 `rt_tokio` 都记录了这个坑）。
//!
//! 因此：连接交给一个**专用宿主线程**，它在自己的运行时里用 `run_until` 一直驱动到进程
//! 结束；`MuxConnection` 是 `Send + Sync` 的智能指针，经 channel 交回调用线程。

use core::mem::MaybeUninit;

use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow};
use smux_v1::{
    connection::{
        BufferedRx, BufferedTx, DefaultConnCfg, Dock, MuxChanBuffOwnedBy, MuxConnection,
        new_buffered_channel,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent, HandshakeDelivery},
        opts::BasicOpts,
    },
    x_deps::{
        abs_art,
        mm_ptr::{Owned, Shared, x_deps::abs_mm},
    },
};

use abs_art::TrLocalScope;

/// 传输环容量（字节）：只影响吞吐，不影响正确性。
const K_TRANSPORT_CAP: usize = 64usize * 1024usize;

/// 连接级帧暂存容量（字节）。
const K_STAGE_CAP: usize = 64usize * 1024usize;

/// 一条子流向一个方向的环容量（字节）。
///
/// 刻意取小：本 crate 的用例只搬几十字节，小环能让「环满 / 环空」的边界更快暴露。
pub const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 子流 ring 存储的拥有者类型。
pub type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 连接级帧暂存的拥有者类型。
type StageBuff = MuxChanBuffOwnedBy<RingBuff>;

/// 进程内环回用的连接类型。
pub type LocalConn = MuxConnection<DefaultConnCfg<BufferedTx, BufferedRx>>;

/// 一对互连的进程内连接：`.0` 是**主动方**（invite），`.1` 是**被动方**（listen）。
///
/// 与 `mptp_cs_smoke` 的 `(conn_a, conn_b)` 同构，只是带名字，避免用例里搞反方向。
pub struct LocalPair_ {
    /// 握手时跑 `invite_async` 的那一端。
    pub active_: Shared<LocalConn, CoreAlloc>,
    /// 握手时跑 `listen_async` 的那一端。
    pub passive_: Shared<LocalConn, CoreAlloc>,
}

/// 用给定的握手协商项建一对互连的进程内连接。
///
/// `opts_active` 交给跑 `invite_async` 的那一端，`opts_passive` 交给跑 `listen_async` 的
/// 那一端。**两端可以不同**——这不是为了模拟不对称，而是为了让用例能把「一端先超时、
/// 另一端还在等」构造出来：只有当两端的 `max_channel_timeout` 不同，才能把「先超时的一方
/// 补发 `CLOSE` 唤醒对端」与「对端自己等满超时」在时间上分开（见
/// `tests/idle_timeout_.rs`）。
///
/// 协商结果由 `smux_v1` 按两端的声明取交集，因此用例观察到的宽限期 / 空闲超时就是它
/// 自己传进去的那一份。
///
/// # 已知限制
///
/// 目前只在 `rt-tokio` 装配下实现：握手要把 `invite` 与 `listen` 两个 future 合到一次
/// 驱动里（`tokio::join!`），而 compio / smol 各自有等价的驱动方式（compio 靠运行时自带
/// tick、smol 靠 `block_on`），尚未在同一个函数里统一。这一点与 `mptp_cs_smoke` 的
/// `run_local_roundtrip_` 是**同一个**限制，不是本 crate 新引入的。
///
/// # Errors
///
/// 宿主线程建不起来、握手失败或对端在握手期间退出时返回错误。
#[cfg(feature = "rt-tokio")]
pub fn connect_pair_(opts_active: BasicOpts, opts_passive: BasicOpts) -> Result<LocalPair_> {
    let (tx, rx) = std::sync::mpsc::channel::<Result<LocalPair_>>();
    std::thread::Builder::new()
        .name("contract-host".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Result::Ok(rt) => rt,
                Result::Err(err) => {
                    let _ = tx.send(Result::Err(anyhow!("建宿主线程运行时失败: {err}")));
                    return;
                }
            };
            rt.block_on(async move {
                let runtime = smux_v1::connection::default_rt_();
                let scope = runtime.local_scope();
                // 整段跑在 `run_until` 里：握手要它驱动，连接建成之后的五个循环也继续
                // 靠它驱动（末尾那个 `pending` 就是为了不提前退出）。
                scope
                    .run_until(async move {
                        let pair = connect_pair_inner_(opts_active, opts_passive).await;
                        let _ = tx.send(pair);
                        core::future::pending::<()>().await;
                    })
                    .await;
            });
        })
        .map_err(|err| anyhow!("起连接宿主线程失败: {err}"))?;
    rx.recv()
        .map_err(|_| anyhow!("连接宿主线程提前退出"))?
}

/// 真正的建连：两条全被动传输环 + 一次本地握手。
#[cfg(feature = "rt-tokio")]
async fn connect_pair_inner_(opts_active: BasicOpts, opts_passive: BasicOpts) -> Result<LocalPair_> {
    // 每条环拆成 (写半边, 读半边)，**交叉**交给两端：主动方的写接到被动方的读，反之亦然。
    let (active_tx, passive_rx) = make_transport_ring_()?;
    let (passive_tx, active_rx) = make_transport_ring_()?;

    let invite = HandshakeAgent::new(active_tx, active_rx).invite_async(&opts_active, AcceptAllEntries);
    let listen = HandshakeAgent::new(passive_tx, passive_rx).listen_async(&opts_passive, AcceptAllEntries);
    let (invited, accepted) = tokio::join!(invite, listen);
    let delivery_active = invited.map_err(|err| anyhow!("主动方握手失败: {err:?}"))?;
    let delivery_passive = accepted.map_err(|err| anyhow!("被动方握手失败: {err:?}"))?;

    Result::Ok(LocalPair_ {
        active_: Shared::new(build_conn_(delivery_active), CoreAlloc),
        passive_: Shared::new(build_conn_(delivery_passive), CoreAlloc),
    })
}

/// 造一条全被动传输环，返回 `(写半边, 读半边)`。
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
pub fn make_channel_buffs_() -> (RingBuff, RingBuff) {
    (
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
    )
}

/// 由**已经完成握手**的交付物造出一条复用连接。
///
/// 运行时值与作用域由配置自己解决（`TrConnCfg::runtime` + `ScopeHost`），因此这里只需要
/// 一个流控策略。
#[cfg(feature = "rt-tokio")]
fn build_conn_(delivery: HandshakeDelivery<BufferedTx, BufferedRx>) -> LocalConn {
    let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
    let (read_buff, write_buff) = make_stage_buffs_();
    MuxConnection::new(delivery, cfg, read_buff, write_buff)
}

/// 绑定的 dock：用例统一用这一个。
pub const K_DOCK: u32 = 1u32;

/// 本 crate 统一使用的 dock 值。
pub fn dock_() -> Dock {
    Dock::new(K_DOCK)
}
