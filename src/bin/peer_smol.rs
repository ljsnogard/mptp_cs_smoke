//! `peer_smol`：**smol** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-smol --bin peer_smol -- \
//!     --runtime smol --role server --listen 127.0.0.1:9000
//! ```
//!
//! 参数语义见 `--help`。
//!
//! smol 没有隐式运行时上下文，因此入口是同步的：整段场景挂在 `smol::block_on` 上（见
//! `rt_smol` 模块文档）。

/// 入口（同步）。
fn main() -> std::process::ExitCode {
    mptp_cs_smoke::rt_smol::run_blocking()
}
