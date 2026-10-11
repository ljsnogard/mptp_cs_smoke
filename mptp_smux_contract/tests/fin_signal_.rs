//! 拆流契约 ①：`drop(tx)` 的 `FIN` 有没有按协议送达对端。
//!
//! # 被检验的两句话
//!
//! - `smux_v1::connection` 模块文档 §7.0：`CLOSE(FIN)` 声明「**我不再发送**」，接收方
//!   关掉自己**接收**环的写端；
//! - `mptp_core::serving::server` 的收尾：`drop(tx)` 之后 `drain_until_eof_` 就该读到
//!   EOF——它假定「对端关闭」这件事**会到**。
//!
//! 缺口的表现就是本用例观测不到关闭：对端一直不被唤醒，直到它自己的空闲计时
//! （`max_channel_timeout`）把子流拆掉。
//!
//! # 观测手段
//!
//! 用例把 `max_channel_timeout` 压到 1 秒（合法下限），于是「信号到了」与「信号没到、
//! 等到超时」在时间上量级分明：前者毫秒级，后者 1 秒级。断言给 3 秒预算。

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

/// 本用例的握手协商项：空闲超时与宽限期都压到合法下限。
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

/// 测试目标：`drop(tx)` 之后，对端应当**很快**观测到「对端不再发送」（`is_tx_closed` 为真）。
/// - 手段：进程内建一对连接（空闲超时压到 1 秒）；被动方绑定 dock 1 并 accept 一条子流；
///   主动方 open 该 dock；随后主动方 `drop(tx)`（按协议发 `FIN`），被动方轮询自己 `rx`
///   的 `is_tx_closed()`。
/// - 判断：3 秒内观测到 `is_tx_closed()` 为真。若始终为假，说明 `FIN` 没有送达对端——
///   这是一处需求缺口，而不是测试写错。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_tx_sends_fin_that_peer_observes_() -> anyhow::Result<()> {
    let pair = connect_pair_(tight_opts_(), tight_opts_())?;

    // 被动方：绑定 → 监听 → accept 一条子流，然后只观察接收半边的关闭态。
    let passive = pair.passive_.clone();
    let server = tokio::task::spawn_blocking(move || -> anyhow::Result<bool> {
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

            // 轮询「对端不再发送」这一位：它是环上的关闭位，反映 `FIN` 是否已到达。
            let deadline = std::time::Instant::now() + Duration::from_secs(3u64);
            let mut observed = srx.is_tx_closed();
            while !observed && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5u64)).await;
                observed = srx.is_tx_closed();
            }
            anyhow::Result::Ok(observed)
        })
    });

    // 主动方：open 一条子流，然后**只**丢弃发送半边（按协议发 `FIN`）。
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
            // 欢迎信息的载荷由对端给出；本端只提供一块可写缓冲接住它。
            let mut welcome: &mut [u8] = &mut [];
            let (ctx, _crx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方建环失败: {err}"))?;
            // 只丢弃发送半边：这正是「我不再发送」的声明。
            drop(ctx);
            anyhow::Result::Ok(())
        })
    });

    let _ = client;
    let observed = server.await??;
    assert!(
        observed,
        "主动方 drop(tx) 之后 3 秒内，被动方仍未观测到 is_tx_closed。\
         按 smux_v1 §7.0，FIN 应当关掉接收方的接收环写端；\
         若只能等到 max_channel_timeout（本用例 1 秒）才关闭，说明 FIN 没有送达对端"
    );
    Ok(())
}
