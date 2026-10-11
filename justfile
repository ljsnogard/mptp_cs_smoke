# mptp_cs_smoke 的配方。
#
# 本 crate 是**独立 workspace**：三个运行时 feature 互斥，三格各编一次。
# 超时纪律：单元测试 60 秒；本机环回全量 300 秒。
set shell := ["bash", "-uc"]

smoke := justfile_directory()
# 需要离线（本机 cargo 缓存只读 / 断网）时：`SMOKE_CARGO_OFFLINE=--offline just pairs`
offline := env('SMOKE_CARGO_OFFLINE', '')

default:
    @just --list

# 三格各编译一次（feature 互斥，必须分开编）。每格连 peer 与 `cargo run` 入口一起编，
# 后者按装配分派（见 `src/main.rs`）。
build:
    cargo build --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio --bin peer_tokio --bin mptp_cs_smoke
    cargo build --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-compio --bin peer_compio --bin mptp_cs_smoke
    cargo build --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-smol --bin peer_smol --bin mptp_cs_smoke

# 单元测试（命令行解析、进程内环回），60 秒上限
test:
    timeout 60 cargo test --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio -p mptp_cs_smoke

# `mptp_core` × `smux_v1` 的需求对齐检验（拆流契约），60 秒上限
#
# 与 `test` 分开是有意的：那一个问「链路通不通」，这一个问「两边的约定一致不一致」。
# 后者的失败**本身就是结论**（一处需求缺口），因此它值得单独一条命令、单独看。
contract:
    timeout 60 cargo test --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio -p mptp_smux_contract

# 同上，但把每个契约用例的观测值也打出来（调缺口时看诊断信息）
contract-verbose:
    timeout 60 cargo test --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio -p mptp_smux_contract -- --nocapture --test-threads=1

# 冒烟 + 契约，一次跑完（60 秒上限）
check:
    timeout 60 cargo test --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio -p mptp_cs_smoke -p mptp_smux_contract

# 静态检查
clippy:
    cargo clippy --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio --all-targets

# 进程内环回（不碰网络）
local:
    timeout 60 cargo test --manifest-path {{smoke}}/Cargo.toml {{offline}} --no-default-features --features rt-tokio --test local_e2e -- --nocapture

# 本机 127.0.0.1 环回：3 组运行时配对（tokio-compio / tokio-smol / compio-smol），300 秒上限
pairs:
    timeout 300 python3 {{smoke}}/scripts/run_pairs.py {{offline}}

# 只跑一组配对：`just one tokio compio`
one a b:
    timeout 300 python3 {{smoke}}/scripts/run_pairs.py {{offline}} --pairs {{a}}-{{b}}

# 自定义端口跑一组（手打命令行的对照见 README）
run a b port:
    timeout 300 python3 {{smoke}}/scripts/run_pairs.py {{offline}} --pairs {{a}}-{{b}} --base-port {{port}}
