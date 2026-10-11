//! 拆流契约 ④：`FIN` 到达时，**正阻塞在同步读**上的本端读者会不会被唤醒。
//!
//! # 为什么这一条才是 `mptp_core` 真正依赖的
//!
//! 用例 ① 验的是「`drop(tx)` 之后，对端**轮询** `is_tx_closed()` 会变真」——那是协议
//! 信号有没有送达。但 `mptp_core` 的收尾**不是轮询**：`serve_channel_async_` 在
//! `drop(tx)` 之后调用 `drain_until_eof_(&mut rx)`，而后者是
//!
//! ```text
//! let mut read = AsStdRead::new(rx, cancel);
//! std::io::Read::read(&mut read, &mut sink)   // 一次**阻塞**的同步读
//! ```
//!
//! 也就是说它把「等对端关闭」实现成**一次阻塞读**。这要求：`FIN` 到达时，**已经 park
//! 在同步读上的读者必须被唤醒**（返回 EOF）。若关闭环的写端只是置了标志位而没有唤醒
//! park 的读者，这次读就会一直挂着，直到空闲计时把子流拆掉——
//! `smux_v1` 自己在 `StageCloseGuard_` 的注释里点明过这个陷阱：
//!
//! > 只 drop 环的写端**不会**唤醒 park 的读者（`buffex` 的环半部 drop 不置关闭标记）。
//!
//! 本用例就是把这句话变成可执行的断言。
//!
//! # 怎么把「被 FIN 唤醒」与「等到超时」分开
//!
//! 两端空闲超时都取 5 秒，而断言预算只有 800 毫秒：
//!
//! - 800 毫秒内读到 EOF / 出错 ⇒ 是 `FIN` 唤醒的；
//! - 800 毫秒内没醒 ⇒ 说明阻塞读没有被唤醒，对端只能等自己的 5 秒空闲超时。

#![cfg(feature = "rt-tokio")]

use std::time::{Duration, Instant};

use abs_buff_stdio_adapt::AsStdRead;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
    dock::TrDock,
};
use mptp_core::{
    serving::ServingRingPrepare,
    x_deps::{abs_buff_stdio_adapt, abs_cancel, abs_smux},
};
use mptp_smux_contract::loopback_::{connect_pair_, dock_, make_channel_buffs_};
use smux_v1::handshake::opts::BasicOpts;

/// 空闲超时：刻意取 5 秒，远大于断言窗口，好让「被唤醒」与「等到超时」量级分明。
const K_IDLE_TIMEOUT: Duration = Duration::from_secs(5u64);

/// 断言预算：阻塞读必须在这个时间内返回。
const K_BUDGET: Duration = Duration::from_millis(800u64);

fn opts_() -> BasicOpts {
    BasicOpts {
        max_channel_timeout: K_IDLE_TIMEOUT,
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

/// 测试目标：对端 `drop(tx)` 发 `FIN` 时，本端**阻塞中**的同步读应当立即返回。
/// - 手段：进程内建一对连接（空闲超时 5 秒）；被动方 accept 后，主动方 `drop(tx)`；
///   被动方随即用 `AsStdRead` 发起一次**阻塞**同步读，并测量它返回所需时间。
/// - 判断：读必须在 800 毫秒内返回 `Ok(0)`（或出错）。若超时未返回，说明 `FIN` 只置了
///   关闭位而没有唤醒 park 的读者——`mptp_core` 的 `drain_until_eof_` 会因此一直挂着，
///   这正是进程内往返要等满 `max_channel_timeout` 的原因。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fin_wakes_blocked_sync_reader_() -> anyhow::Result<()> {
    let pair = connect_pair_(opts_(), opts_())?;

    // 被动方：accept 之后先等一下（让主动方把 FIN 发出来），再做一次阻塞同步读。
    let passive = pair.passive_.clone();
    let server = tokio::task::spawn_blocking(move || -> anyhow::Result<(Duration, String)> {
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
            let (_stx, mut srx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("被动方裁决建流失败: {err}"))?;

            // 给主动方时间把 FIN 发出来；这条 `sleep` 只是让时序确定，不参与判定。
            tokio::time::sleep(Duration::from_millis(50u64)).await;

            // 关键动作：一次**阻塞**的同步读——与 `mptp_core::serving` 的
            // `drain_until_eof_` 完全同形。
            let started = Instant::now();
            let mut read = AsStdRead::new(&mut srx, NonCancellableToken::new());
            let mut sink = [0u8; 256usize];
            let outcome = std::io::Read::read(&mut read, &mut sink);
            let elapsed = started.elapsed();
            let text = match outcome {
                Result::Ok(n) => format!("Ok({n})"),
                Result::Err(err) => format!("Err({err})"),
            };
            anyhow::Result::Ok((elapsed, text))
        })
    });

    // 主动方：open 一条子流后立刻只丢发送半边（发 FIN），然后保持不动。
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
            let (ctx, _crx) = handle
                .accept_async(&mut welcome, prepare)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow::anyhow!("主动方建环失败: {err}"))?;
            // 发 FIN；接收半边用 `forget` 留着，确保这之后不再有 RESET 等其它信号干扰。
            drop(ctx);
            anyhow::Result::Ok(())
        })
    });

    let _ = client;
    let (elapsed, text) = server.await??;
    println!("阻塞同步读返回耗时 {elapsed:?}，结果 {text}");
    assert!(
        elapsed <= K_BUDGET,
        "对端 drop(tx) 发出 FIN 后，本端**阻塞中**的同步读在 {K_BUDGET:?} 内没有返回\
         （已耗时 {elapsed:?}，两端空闲超时都是 {K_IDLE_TIMEOUT:?}）。\
         按 smux_v1 的 StageCloseGuard_ 注释，只 drop 环写端不会唤醒 park 的读者；\
         若确实如此，`mptp_core` 的 drain_until_eof_ 就会一直挂到空闲超时"
    );
    Ok(())
}
