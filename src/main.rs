//! `cargo run` 的入口：跑一次**进程内**环回，把客户端与服务端在同一个进程里接起来。
//!
//! 真正的跨运行时演示是那三个 `peer_*` 可执行文件（`src/bin/` 下）——它们各自按一个
//! 运行时 feature 编译，在真实 TCP socket 上互联。本入口只跑 [`run_local_roundtrip_`]：
//! 两条全被动内存环直连两个端点，不碰网络，不需要第二个终端，也不需要任何命令行参数。
//!
//! # 为什么按后端分派入口
//!
//! [`run_local_roundtrip_`] 目前是 `rt-tokio` 装配独有的：它内部要用 `tokio::join!` 把
//! 发起方与等待方的握手合到一次驱动里，而另外两个后端各自有等价的驱动方式（compio 靠
//! 运行时自带 tick、smol 靠 `block_on`），尚未在同一个函数里统一。因此这里按 feature
//! 给出各自的入口；非 tokio 装配下如实说明，而不是给出一个跑不起来的假入口。
//!
//! ```console
//! $ cargo run --no-default-features --features rt-tokio
//! ```

#[cfg(feature = "rt-tokio")]
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::process::ExitCode {
    println!("== MPTP 进程内 cs 环回演示 ==");
    println!("1. 在进程内建一对互连的复用连接（两条全被动环 + 本地握手，不经过 socket）");
    println!("2. 服务端在 dock 1 上监听；客户端向同一个 dock 发起 View /hello");
    println!("3. 服务端解码请求前缀 → 路由到 /hello → handler 回 200 OK");
    println!("4. 客户端解析响应前缀");

    match mptp_cs_smoke::run_local_roundtrip_().await {
        Result::Ok(status) => {
            println!();
            println!("完成：客户端读到的响应状态 = {}", status.inner());
            std::process::ExitCode::SUCCESS
        }
        Result::Err(err) => {
            eprintln!("运行失败：{err}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// 非 tokio 装配下的入口：说明情况并直接失败，不假装能跑。
#[cfg(not(feature = "rt-tokio"))]
fn main() -> std::process::ExitCode {
    eprintln!(
        "进程内环回目前只在 rt-tokio 装配下提供；本次构建用的是另一个后端。\n\
         要跑它：cargo run --no-default-features --features rt-tokio\n\
         要跑本后端的跨进程往返：cargo run --no-default-features --features rt-<名字> \
         --bin peer_<名字> -- --runtime <名字> --role server --listen 127.0.0.1:9000"
    );
    std::process::ExitCode::FAILURE
}
