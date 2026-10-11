//! `peer_compio`：**compio** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-compio --bin peer_compio -- \
//!     --runtime compio --role server --listen 127.0.0.1:9000
//! ```
//!
//! 参数语义见 `--help`。

/// 入口：运行时由 `#[compio::main]` 提供。
#[compio::main]
async fn main() -> std::process::ExitCode {
    mptp_cs_smoke::rt_compio::run_async().await
}
