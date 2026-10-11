//! 拆流契约 ②：`drop(rx)` 的 `RESET` 是否**只**声明「我不再接收」，而不关掉接收方向。
//!
//! # 被检验的两句话
//!
//! - `smux_v1::connection` 模块文档 §7.0 的 `RESET` 一行：声明「**我不再接收**」的接收方
//!   动作是「停止并放弃自己的**发送**方向，**不**关接收环写端」；
//! - `mptp_core::serving::server` 的收尾：`drop(tx)` 之后 `drain_until_eof_(&mut rx)`
//!   一直读到 EOF 才结束——它把「对端结束」整体当成「读端会 EOF」。
//!
//! 两句话是否相容，取决于「收尾时对端到底发的是哪一条」。本用例把两个方向分开验：
//!
//! - `drop(tx)` → `FIN` → 对端**应当**观测到关闭（用例 ① 已通过）；
//! - `drop(rx)` → `RESET` → 对端**不应**因为这一条就认为「对方不再发送」——按协议，
//!   `RESET` 声明的是「我不再接收」，与「我不再发送」是正交的。
//!
//! # 本用例断言什么
//!
//! 主动方 `drop(rx)` 之后，被动方的接收半部**不应**变成「对端不再发送」。若它变成了，
//! 说明实现把 `RESET` 当成了 `FIN` 用——那会让 `mptp_core` 的收尾提前结束，也会让
//! 「半关闭」这一协议特性在实现里失真。

#![cfg(feature = "rt-tokio")]

use std::time::Duration;

use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_smux::{
    chan::{TrChannelHalf, TrChannelHandle},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
    dock::TrDock,
};
use mptp_core::{
    serving::ServingRingPrepare,
    x_deps::{abs_cancel, abs_smux},
};
use mptp_smux_contract::loopback_::{connect_pair_, dock_, make_channel_buffs_};
use smux_v1::handshake::opts::BasicOpts;

/// 空闲超时压到合法下限：于是「对端真的关闭了」与「什么都没发生、只是还没超时」在时间上
/// 量级分明（毫秒级 vs 1 秒级）。
fn tight_opts_() -> BasicOpts {
    BasicOpts {
        max_channel_timeout: Duration::from_secs(1u64),
        max_channel_wait_close: Duration::from_secs(1u64),
        ..BasicOpts::DEFAULT
    }
}

/// 进程内跑一段异步场景（每段自带运行时与本地队列）。
fn current_thread_block_on_<F, T>(body: F) -> anyhow::Result<T>
where
    F: core::future::Future<Output = anyhow::Result<T>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(body)
}

/// 测试目标：主动方 `drop(rx)`（发 `RESET`）之后，被动方的接收半部不应报告「对端不再发送」。
/// - 手段：进程内建一对连接（空闲超时 1 秒）；被动方 bind + accept，主动方 open；
///   随后主动方只丢弃**接收**半边，被动方在 200 毫秒窗口内采样自己 `rx` 的
///   `is_tx_closed()`。窗口远小于 1 秒的空闲超时，因此测到的一定是 `RESET` 的直接影响，
///   而不是空闲计时拆流。
/// - 判断：窗口内 `is_tx_closed()` 必须**始终为假**。若为真，说明实现把 `RESET`
///   当成了 `FIN`，`smux_v1` §7.0 的「两个正交方向」在实现里并不成立。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_rx_sends_reset_that_does_not_close_peer_rx_() -> anyhow::Result<()> {
    let pair = connect_pair_(tight_opts_(), tight_opts_())?;

    // 被动方：bind → listen → accept，然后在短窗口内采样接收半部的「对端不再发送」位。
    let passive = pair.passive_.clone();
    let server = tokio::task::spawn_blocking(move || -> anyhow::Result<(bool, Option<String>)> {
        current_thread_block_on_(async move {
            let mut binding = passive
                .bind_async(dock_())
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("被动方绑定 dock 失败: {err}"))?;
            let mut listener = binding
                .listen_async(8usize)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("被动方进入监听失败: {err}"))?;
            let mut handle = listener
                .income_async()
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("被动方等待入向建流失败: {err}"))?;
            let (tx_buff, rx_buff) = make_channel_buffs_();
            let prepare = ServingRingPrepare::new(tx_buff, rx_buff);
            let mut welcome: &mut [u8] = &mut [];
            let (_stx, srx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("被动方裁决建流失败: {err}"))?;

            // 采样窗口：200 毫秒，远小于 1 秒的空闲超时。
            let deadline = std::time::Instant::now() + Duration::from_millis(200u64);
            let mut ever_closed = false;
            while std::time::Instant::now() < deadline {
                if srx.is_tx_closed() {
                    ever_closed = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5u64)).await;
            }
            let reason = srx.abort_reason().map(|err| format!("{err:?}"));
            anyhow::Result::Ok((ever_closed, reason))
        })
    });

    // 主动方：open 一条子流，然后**只**丢弃接收半边（按协议发 `RESET`）。
    let active = pair.active_.clone();
    let client = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        current_thread_block_on_(async move {
            let mut binding = active
                .bind_async(TrDock::unspecified())
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方绑定本地 dock 失败: {err}"))?;
            let mut empty: &[u8] = &[];
            let mut handle = binding
                .open_channel_async(dock_(), &mut empty)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方开子流失败: {err}"))?;
            let (tx_buff, rx_buff) = make_channel_buffs_();
            let prepare = ServingRingPrepare::new(tx_buff, rx_buff);
            let mut welcome: &mut [u8] = &mut [];
            let (ctx, crx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方建环失败: {err}"))?;
            // **只**丢失接收半边：这正是「我不再接收」的声明。
            drop(crx);
            // 发送半边必须**留着不发 FIN**——否则同时到来的 FIN 会污染本用例的结论，
            // 让「对端不再发送」这件事分不清是 RESET 造成的还是 FIN 造成的。
            // `forget` 就是「本端故意保持发送方向开着」（等价于一个永不关闭的发送端）。
            std::mem::forget(ctx);
            anyhow::Result::Ok(())
        })
    });

    let _ = client;
    let (ever_closed, reason) = server.await??;
    let reason_text = reason
        .as_deref()
        .unwrap_or("<本端没有 abort_reason，即不是本地空闲超时拆的>");
    assert!(
        !ever_closed,
        "主动方 drop(rx)（发 RESET，且**不发** FIN）之后 200 毫秒内，\
         被动方就报告了「对端不再发送」。按 smux_v1 §7.0，RESET 只声明「我不再接收」，\
         与「我不再发送」正交，不应关掉对端接收环的写端。\
         被动方本端 abort_reason = {reason_text}"
    );
    Ok(())
}
