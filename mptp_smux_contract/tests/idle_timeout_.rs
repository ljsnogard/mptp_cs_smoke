//! 拆流契约 ③：空闲超时（`IdleTimeout`）拆一条子流时，有没有向对端**补发**两条 `CLOSE`。
//!
//! # 被检验的那句话
//!
//! `smux_v1::connection` 模块文档 §7.1：
//!
//! > 若在 `max_channel_timeout` 内**一个字节都没收到**（数据与 `PULSE` 都没有），该子流
//! > 判为空闲超时：**只拆该子流**（不拆连接），报 [`MuxError::IdleTimeout`]。拆流时会
//! > **尽力向对端补两条 `CLOSE`（`FIN` + `RESET`）**，让对端不必等它自己的空闲计时。
//!
//! 为什么这条对 `mptp_core` 重要：`mptp_core` 的收尾**依赖对端通知**（`drain_until_eof_`）。
//! 如果本端超时拆流时不对对端补 `CLOSE`，对端就只剩下「等自己的空闲计时」这一条路——
//! 于是双方各自等满一个超时，收尾时间翻倍。
//!
//! # 怎么把「补发了」与「各自超时」区分开
//!
//! 给两端**不同的**空闲超时：
//!
//! - 主动方 `max_channel_timeout = 1s`（先超时的一方，理论上会补发 `CLOSE`；1 秒是协议
//!   允许的下限）；
//! - 被动方 `max_channel_timeout = 10s`（若收不到补发的 `CLOSE`，它最快要 10 秒才自己超时）。
//!
//! 于是被动方在 **3 秒**内被唤醒 ⇒ 那只能是主动方补发的 `CLOSE` 到了；
//! 若被动方安然无事地待过 3 秒，就说明**没有补发**，对端只能干等自己的 10 秒。

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

/// 主动方的超时：先到，触发「空闲超时拆流 +（按文档）补发两条 CLOSE」。
///
/// 取 1 秒是 `BasicOpts` 的**合法下限**：协议要求 `>= 1` 秒，再小会被握手拒绝。
const K_ACTIVE_TIMEOUT: Duration = Duration::from_secs(1u64);

/// 被动方的超时：刻意取长，把「收到补发」与「自己超时」在时间上彻底分开。
const K_PASSIVE_TIMEOUT: Duration = Duration::from_secs(10u64);

/// 观测窗口：远大于主动方超时，又远小于被动方超时。
const K_WATCH: Duration = Duration::from_secs(3u64);

/// 两端的协商项按角色分开给，这是本用例能分辨「补发到达」与「对端自等超时」的关键。
fn opts_for_(timeout: Duration) -> BasicOpts {
    BasicOpts {
        max_channel_timeout: timeout,
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

/// 测试目标：先超时的一方拆流时，应当向对端补发 `CLOSE`，使对端不必等自己的空闲计时。
/// - 手段：主动方空闲超时 500ms、被动方 5s；两端建流后都**不发数据**，让主动方先超时；
///   被动方在 800 毫秒窗口内轮询自己 `rx` 的 `is_tx_closed()`。
/// - 判断：窗口内被动方被唤醒（`is_tx_closed` 为真）⇒ 主动方补发的 `CLOSE` 确实到了；
///   若窗口内始终保持为假 ⇒ 没有补发，对端只能等自己的 5 秒。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_timeout_sends_two_closes_to_peer_() -> anyhow::Result<()> {
    // 两端用**不同**的协商项：主动方 1 秒先超时，被动方 10 秒。
    let pair = connect_pair_(
        opts_for_(K_ACTIVE_TIMEOUT),
        opts_for_(K_PASSIVE_TIMEOUT),
    )?;

    // 被动方：bind → listen → accept，然后在窗口内轮询「对端不再发送」。
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

            let deadline = std::time::Instant::now() + K_WATCH;
            let mut observed = srx.is_tx_closed();
            while !observed && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5u64)).await;
                observed = srx.is_tx_closed();
            }
            // 顺带把「本端认为这条子流为什么结束」记下来，便于分辨是谁拆的。
            let reason = srx.abort_reason().map(|err| format!("{err:?}"));
            anyhow::Result::Ok((observed, reason))
        })
    });

    // 主动方：open 一条子流后**什么都不发**，静待自己的空闲超时。
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
            let (_ctx, _crx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方建环失败: {err}"))?;
            // 刻意什么都不做：等自己的空闲超时（500ms）先到。
            anyhow::Result::Ok(())
        })
    });

    let _ = client;
    let (observed, reason) = server.await??;
    let reason_text = reason.as_deref().unwrap_or("<本端没有 abort_reason，即不是本地拆的>");
    assert!(
        observed,
        "主动方空闲超时（{K_ACTIVE_TIMEOUT:?}）拆流后 {K_WATCH:?} 内，被动方没有被唤醒\
         （被动方自己的超时是 {K_PASSIVE_TIMEOUT:?}，远超此窗口）。\
         按 smux_v1 §7.1，超时拆流时应「尽力向对端补两条 CLOSE（FIN + RESET）」，\
         使对端不必等它自己的空闲计时；这里没等到，说明补发缺失。\
         被动方本端 abort_reason = {reason_text}"
    );
    // 即便被唤醒，也把「怎么醒的」印出来——补发的 CLOSE 与对端自等超时给出的原因不同。
    println!("被动方在窗口内被唤醒；本端 abort_reason = {reason_text}");
    Ok(())
}
