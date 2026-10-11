//! `peer_tokio`：**tokio** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-tokio --bin peer_tokio -- \
//!     --runtime tokio --role server --listen 127.0.0.1:9000
//! ```
//!
//! 参数语义见 `--help`。

/// 入口：运行时由 `#[tokio::main]` 提供，业务在库里。
///
/// 用 `current_thread`：本地队列是线程本地的 `LocalSet`，多线程 flavor 下任务可能被搬到
/// 别的 worker 线程，取到的是另一条本地队列（详见 `rt_tokio` 模块文档）。
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    mptp_cs_smoke::rt_tokio::run_async().await
}
