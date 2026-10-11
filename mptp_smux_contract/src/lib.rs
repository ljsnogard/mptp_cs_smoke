#![feature(unboxed_closures)]

//! `mptp_core` 与 `smux_v1` 之间的**需求对齐检验**。
//!
//! # 这个 crate 问什么
//!
//! 同 workspace 的 `mptp_cs_smoke` 问的是「整条链路能不能跑通」——端到端冒烟，成功即
//! 通过。本 crate 问的是另一件事：**两边对同一件事的约定是否一致**。
//!
//! 具体做法是把 `mptp_core` 对 `smux_v1` 的依赖逐条写成**可断言、可失败**的用例。因此：
//!
//! - 用例通过 = 该项约定确实对齐；
//! - 用例失败 = 一处**需求缺口**（不是「测试写错了」），失败信息会指出缺口在哪一侧。
//!
//! # 首轮聚焦：拆流（teardown）
//!
//! 起因是一个实测现象：`mptp_cs_smoke` 的进程内往返要 **30.0 秒**才结束，而客户端读到
//! 响应只用了 **0.01 秒**（计时探针的原始输出见该 crate 的对话记录）。这 30 秒正好等于
//! `BasicOpts::DEFAULT.max_channel_timeout`，于是问题收敛成一句话：
//!
//! > `mptp_core` 的收尾依赖「对端关闭 → 读端立刻 EOF」，而 `smux_v1` 把关闭定义成
//! > 两个**正交方向**（`FIN` / `RESET`）。这两句话等价吗？
//!
//! 两边各自的权威说法：
//!
//! - `smux_v1::connection` 模块文档 §7.0：`CLOSE(FIN)` 声明「我不再发送」，接收方关掉
//!   自己**接收**环的写端（应用读到 EOF）；`CLOSE(RESET)` 声明「我不再接收」，接收方
//!   放弃自己**发送**的方向，**不**关接收环写端；
//! - `mptp_core::serving::server` 的 `serve_channel_async_`：写回响应后 `drop(tx)`，
//!   再 `drain_until_eof_(&mut rx)` 一直读到 EOF 才结束。
//!
//! 本 crate 的用例就是照这两句话的**交界处**设计的。
//!
//! # 跑法
//!
//! ```console
//! $ cargo test -p mptp_smux_contract --no-default-features --features rt-tokio
//! ```

pub mod loopback_;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 编译期守卫：恰好一个运行时后端（与 `mptp_cs_smoke` 同构）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(not(any(feature = "rt-tokio", feature = "rt-compio", feature = "rt-smol")))]
compile_error!(
    "mptp_smux_contract：必须启用恰好一个运行时 feature（rt-tokio / rt-compio / rt-smol）"
);

#[cfg(any(
    all(feature = "rt-tokio", feature = "rt-compio"),
    all(feature = "rt-tokio", feature = "rt-smol"),
    all(feature = "rt-compio", feature = "rt-smol"),
))]
compile_error!(
    "mptp_smux_contract：三个运行时 feature 互斥，一次构建只能启用一个；\
     换运行时请加 `--no-default-features`"
);
