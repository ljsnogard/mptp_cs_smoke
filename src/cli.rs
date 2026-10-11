//! peer 的命令行解析与两种报告行。
//!
//! 刻意**不引入 `clap`**：参数只有几个，而本 demo 的依赖已经够重。解析全手工，出错时把
//! [`K_HELP`] 一起打出来，手打命令时照着改即可。
//!
//! # 一个进程只扮演一个角色
//!
//! 这不是为了省事。`AsStdRead / AsStdWrite` 的同步等待要在**同线程**上嵌套驱动本地队列
//! （见 `abs_buff_stdio_adapt` 的 `AsStdRead` / `AsStdWrite`，内部走
//! `TrLocalScope::block_on_local`）；同一个进程里若既有服务端又有客户端，两者的
//! 同步等待会互相锁死——一端 park 住，另一端就永远等不到建流的裁决。拆成两个进程之后，
//! 每个进程里只有**一次**同步等待，嵌套 `run_until` 驱动的就是它自己的收发 future。

use std::net::SocketAddr;

use smux_v1::x_deps::RuntimeTag;

/// 本进程扮演的角色。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// 被动端：监听 TCP、握手 `listen`、服务一条子流。
    Server,
    /// 主动端：拨号、握手 `invite`、发一次请求并读取响应。
    Client,
}

impl Role {
    /// 报告行里用的名字。
    pub const fn name(self) -> &'static str {
        match self {
            Role::Server => "server",
            Role::Client => "client",
        }
    }

    /// 解析名字。
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "server" => Option::Some(Role::Server),
            "client" => Option::Some(Role::Client),
            _ => Option::None,
        }
    }
}

/// 命令行声明的运行时。
///
/// 运行时是**编译期**定死的（三个 `rt-*` feature 互斥），这个字段只服务于自检：拿错
/// 二进制时立刻报错，而不是等出诡异现象再回头查。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeSel {
    /// tokio。
    Tokio,
    /// compio。
    Compio,
    /// smol。
    Smol,
}

impl RuntimeSel {
    /// 命令行里的名字（也是报告里的名字）。
    pub const fn name(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "tokio",
            RuntimeSel::Compio => "compio",
            RuntimeSel::Smol => "smol",
        }
    }

    /// 对应的 peer 可执行文件名。
    pub const fn bin(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "peer_tokio",
            RuntimeSel::Compio => "peer_compio",
            RuntimeSel::Smol => "peer_smol",
        }
    }

    /// 编译期后端自报身份用的标签。
    pub const fn tag(self) -> RuntimeTag {
        match self {
            RuntimeSel::Tokio => RuntimeTag::Tokio,
            RuntimeSel::Compio => RuntimeTag::Compio,
            RuntimeSel::Smol => RuntimeTag::Smol,
        }
    }

    /// 解析名字。
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "tokio" => Option::Some(RuntimeSel::Tokio),
            "compio" => Option::Some(RuntimeSel::Compio),
            "smol" => Option::Some(RuntimeSel::Smol),
            _ => Option::None,
        }
    }
}

/// 解析后的 peer 参数。
#[derive(Debug)]
pub struct PeerOptions {
    /// 本进程的角色。
    pub role: Role,
    /// 命令行声明的运行时（用于自检）。
    pub runtime: RuntimeSel,
    /// 监听地址（`server` 用；`0` 端口表示由内核分配）。
    pub listen: SocketAddr,
    /// 对端地址（`client` 用）。
    pub peer: SocketAddr,
    /// 服务端监听的 dock。
    pub dock: u32,
    /// 报告行里的标签（编排器用来区分同一组配对里的多个用例）。
    pub tag: String,
}

/// 帮助文本。
pub const K_HELP: &str = "\
mptp_cs_smoke peer —— 在两台进程（可跨运行时）之间跑一次 MPTP 客户端 / 服务端往返

用法：
    peer_<runtime> --runtime <tokio|compio|smol> --role <server|client> [选项]

选项：
    --runtime <名字>   必填。只用于自检：与编译期后端不一致时立刻失败退出
    --role <角色>      必填。server = 被动端（先起），client = 主动端（后起）
    --listen <地址>    server 用，默认 127.0.0.1:0（由内核挑一个空闲端口）
    --peer <地址>      client 用，必填，指服务端的实际地址
    --dock <u32>       服务端监听的 dock，默认 1
    --tag <文本>       报告行里的标签，默认空
    -h, --help         打印本帮助

示例（手打命令行，两个终端）：
    # 终端 1（compio 侧当服务端）
    cargo run --no-default-features --features rt-compio --bin peer_compio -- \\
        --runtime compio --role server --listen 127.0.0.1:9000

    # 终端 2（tokio 侧当客户端）
    cargo run --no-default-features --features rt-tokio --bin peer_tokio -- \\
        --runtime tokio --role client --peer 127.0.0.1:9000

就绪时打印一行 `{\"event\":\"ready\",...}`（编排器靠它决定何时起主动端），结束时打印一行
`{\"event\":\"result\",...}`。";

/// 从环境参数解析出 [`PeerOptions`]。
///
/// # Errors
///
/// 缺少必填项、取值无法解析或出现未知参数时，返回给用户看的说明文本（已含 [`K_HELP`]）。
pub fn parse_env_args() -> Result<PeerOptions, String> {
    parse_args(std::env::args().skip(1))
}

/// 解析一个参数序列（与 [`parse_env_args`] 分开，便于单元测试）。
///
/// # Errors
///
/// 同 [`parse_env_args`]。
pub fn parse_args<I>(args: I) -> Result<PeerOptions, String>
where
    I: IntoIterator<Item = String>,
{
    let mut runtime: Option<RuntimeSel> = Option::None;
    let mut role: Option<Role> = Option::None;
    let mut listen: Option<SocketAddr> = Option::None;
    let mut peer: Option<SocketAddr> = Option::None;
    let mut dock: u32 = 1u32;
    let mut tag = String::new();

    let mut iter = args.into_iter();
    while let Option::Some(arg) = iter.next() {
        let mut value_of = |name: &str| -> Result<String, String> {
            iter.next()
                .ok_or_else(|| format!("{name} 后面缺少取值\n\n{K_HELP}"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Result::Err(K_HELP.to_string()),
            "--runtime" => {
                let text = value_of("--runtime")?;
                runtime = Option::Some(
                    RuntimeSel::parse(&text)
                        .ok_or_else(|| format!("未知运行时 `{text}`\n\n{K_HELP}"))?,
                );
            }
            "--role" => {
                let text = value_of("--role")?;
                role = Option::Some(
                    Role::parse(&text).ok_or_else(|| format!("未知角色 `{text}`\n\n{K_HELP}"))?,
                );
            }
            "--listen" => {
                let text = value_of("--listen")?;
                listen = Option::Some(
                    text.parse()
                        .map_err(|err| format!("--listen 无法解析：{err}\n\n{K_HELP}"))?,
                );
            }
            "--peer" => {
                let text = value_of("--peer")?;
                peer = Option::Some(
                    text.parse()
                        .map_err(|err| format!("--peer 无法解析：{err}\n\n{K_HELP}"))?,
                );
            }
            "--dock" => {
                let text = value_of("--dock")?;
                dock = text
                    .parse()
                    .map_err(|err| format!("--dock 无法解析：{err}\n\n{K_HELP}"))?;
            }
            "--tag" => {
                tag = value_of("--tag")?;
            }
            other => return Result::Err(format!("未知参数 `{other}`\n\n{K_HELP}")),
        }
    }

    let runtime = runtime.ok_or_else(|| format!("缺少 --runtime\n\n{K_HELP}"))?;
    let role = role.ok_or_else(|| format!("缺少 --role\n\n{K_HELP}"))?;
    let listen = match role {
        Role::Server => listen.unwrap_or_else(|| "127.0.0.1:0".parse().expect("字面量合法")),
        Role::Client => listen.unwrap_or_else(|| "127.0.0.1:0".parse().expect("字面量合法")),
    };
    let peer = peer.unwrap_or_else(|| "127.0.0.1:0".parse().expect("字面量合法"));

    Result::Ok(PeerOptions {
        role,
        runtime,
        listen,
        peer,
        dock,
        tag,
    })
}

/// 就绪事件行：编排器收到它才会去起主动端。
pub fn ready_line(opts: &PeerOptions, actual: SocketAddr) -> String {
    format!(
        "{{\"event\":\"ready\",\"runtime\":\"{}\",\"role\":\"{}\",\"listen\":\"{}\",\"actual\":\"{}\",\"tag\":\"{}\"}}",
        opts.runtime.name(),
        opts.role.name(),
        opts.listen,
        actual,
        opts.tag
    )
}

/// 结果行：一次往返的判定结果。
pub fn result_line(opts: &PeerOptions, status: u16, ok: bool) -> String {
    format!(
        "{{\"event\":\"result\",\"runtime\":\"{}\",\"role\":\"{}\",\"status\":{status},\"ok\":{ok},\"tag\":\"{}\"}}",
        opts.runtime.name(),
        opts.role.name(),
        opts.tag
    )
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试目标：命令行解析在「服务端」与「客户端」两种角色下都能给出正确的默认值。
    /// - 手段：直接喂参数序列给 [`parse_args`]，绕开进程环境。
    /// - 判断：服务端未给 `--listen` 时落到 `127.0.0.1:0`；客户端解析出 `--peer`。
    #[test]
    fn parse_args_fills_defaults_for_both_roles_() {
        let server = parse_args(
            ["--runtime", "compio", "--role", "server"]
                .iter()
                .map(|s| s.to_string()),
        )
        .expect("服务端参数应可解析");
        assert_eq!(server.runtime, RuntimeSel::Compio);
        assert_eq!(server.dock, 1u32);
        assert!(server.listen.ip().is_loopback());

        let client = parse_args(
            [
                "--runtime", "tokio", "--role", "client", "--peer", "127.0.0.1:9000",
            ]
            .iter()
            .map(|s| s.to_string()),
        )
        .expect("客户端参数应可解析");
        assert_eq!(client.peer.port(), 9000u16);
        assert_eq!(client.role, Role::Client);
    }

    /// 测试目标：缺少必填项与未知取值都必须被拒绝（而不是静默用默认值跑下去）。
    /// - 手段：分别省掉 `--runtime`、省略 `--role`、给出无法解析的运行时名。
    /// - 判断：三次都返回 `Err`。
    #[test]
    fn parse_args_rejects_missing_and_unknown_values_() {
        let only_role = ["--role", "server"].iter().map(|s| s.to_string());
        assert!(parse_args(only_role).is_err());

        let only_runtime = ["--runtime", "tokio"].iter().map(|s| s.to_string());
        assert!(parse_args(only_runtime).is_err());

        let bad_runtime = ["--runtime", "go", "--role", "server"]
            .iter()
            .map(|s| s.to_string());
        assert!(parse_args(bad_runtime).is_err());
    }
}
