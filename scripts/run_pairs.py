#!/usr/bin/env python3
"""本机环回编排：按运行时配对起两个 peer 进程，跑一次 MPTP cs 往返。

一次「用例」= 两个进程（**不同运行时**）+ 一条 TCP 连接 + 一次请求 / 响应。本脚本负责：

1. 按配对逐格编译 peer（`--no-default-features --features rt-<名字>`）；
2. 各挑一个空闲端口，**先起服务端**、等它打出 `ready` 事件，再起客户端；
3. 收两边各一行 `result` JSON，做**跨进程核对**（客户端拿到的状态码应为 200）；
4. 汇总成控制台表格，全部通过才退出 0。

用法示例：

    python3 scripts/run_pairs.py                      # 3 组配对
    python3 scripts/run_pairs.py --pairs tokio-compio # 只跑一组
    python3 scripts/run_pairs.py --skip-build         # 复用已编好的二进制
"""

from __future__ import annotations

import argparse
import json
import pathlib
import socket
import subprocess
import sys
import time
from typing import Any

# 三组「取两个运行时」的配对。
PAIRS: list[tuple[str, str]] = [
    ("tokio", "compio"),
    ("tokio", "smol"),
    ("compio", "smol"),
]

DEMO_DIR = pathlib.Path(__file__).resolve().parent.parent
TARGET_DIR = DEMO_DIR / "target" / "debug"


def bin_name(runtime: str) -> str:
    """某个运行时的 peer 可执行文件名。"""
    return f"peer_{runtime}"


def pick_free_port() -> int:
    """让内核挑一个空闲端口（先占后放，仍有极小竞态，环回场景足够）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_peer(runtime: str, offline: str) -> None:
    """按 feature 编译一个 peer。三个 feature 互斥，因此每格单独编。"""
    argv = [
        "cargo",
        "build",
        "--manifest-path",
        str(DEMO_DIR / "Cargo.toml"),
        "--no-default-features",
        "--features",
        f"rt-{runtime}",
        "--bin",
        bin_name(runtime),
    ]
    if offline:
        argv.append(offline)
    subprocess.run(argv, cwd=str(DEMO_DIR), check=True)


class Peer:
    """一个 peer 子进程：收它的 stdout 行。"""

    def __init__(self, runtime: str, role: str, argv: list[str]) -> None:
        self.runtime = runtime
        self.role = role
        self.argv = argv
        self.proc: subprocess.Popen[str] | None = None
        self.ready: dict[str, Any] | None = None
        self.result: dict[str, Any] | None = None
        self.stdout_lines: list[str] = []
        self.stderr_text = ""

    def start(self) -> None:
        self.proc = subprocess.Popen(
            self.argv,
            cwd=str(DEMO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def wait_ready(self, timeout: float = 20.0) -> dict[str, Any]:
        """等服务端打出 `ready` 事件；顺带把 stdout 里其它行攒起来。"""
        assert self.proc is not None and self.proc.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            self.stdout_lines.append(line)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") == "ready":
                self.ready = event
                return event
        raise TimeoutError(f"{self.runtime} 服务端未在 {timeout}s 内就绪")

    def wait_result(self, timeout: float = 30.0) -> dict[str, Any]:
        """等 `result` 事件，并收集剩余输出。"""
        assert self.proc is not None and self.proc.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            self.stdout_lines.append(line)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") == "result":
                self.result = event
                return event
        raise TimeoutError(f"{self.runtime} 客户端未在 {timeout}s 内给出结果")

    def finish(self, timeout: float = 10.0) -> int:
        """收尾：取回 stderr，返回退出码（超时则杀掉并返回 -1）。"""
        assert self.proc is not None
        try:
            _, err = self.proc.communicate(timeout=timeout)
            self.stderr_text = err or ""
            return int(self.proc.returncode or 0)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            _, err = self.proc.communicate()
            self.stderr_text = err or ""
            return -1


def run_case(server_rt: str, client_rt: str, offline: str) -> dict[str, Any]:
    """跑一组配对，返回判定结果。"""
    print(f"\n=== {server_rt} 服务端 + {client_rt} 客户端 ===")
    build_peer(server_rt, offline)
    build_peer(client_rt, offline)

    port = pick_free_port()
    addr = f"127.0.0.1:{port}"
    server = Peer(
        server_rt,
        "server",
        [
            str(TARGET_DIR / bin_name(server_rt)),
            "--runtime",
            server_rt,
            "--role",
            "server",
            "--listen",
            addr,
        ],
    )
    client = Peer(
        client_rt,
        "client",
        [
            str(TARGET_DIR / bin_name(client_rt)),
            "--runtime",
            client_rt,
            "--role",
            "client",
            "--peer",
            addr,
        ],
    )

    outcome: dict[str, Any] = {
        "server": server_rt,
        "client": client_rt,
        "addr": addr,
        "ok": False,
        "detail": "",
    }
    server.start()
    try:
        server.wait_ready()
        client.start()
        result = client.wait_result()
        status = int(result.get("status", 0))
        outcome["ok"] = bool(result.get("ok")) and status == 200
        outcome["status"] = status
        outcome["detail"] = "客户端拿到 200"
    except TimeoutError as err:
        outcome["detail"] = str(err)
    finally:
        client_code = client.finish()
        server_code = server.finish()
        outcome["client_exit"] = client_code
        outcome["server_exit"] = server_code

    if not outcome["ok"]:
        if client.stderr_text.strip():
            outcome["detail"] += f"；客户端 stderr: {client.stderr_text.strip()[:300]}"
        if server.stderr_text.strip():
            outcome["detail"] += f"；服务端 stderr: {server.stderr_text.strip()[:300]}"
    print(f"  → {'通过' if outcome['ok'] else '失败'}：{outcome['detail']}")
    return outcome


def main() -> int:
    parser = argparse.ArgumentParser(description="MPTP cs 跨运行时本机环回编排")
    parser.add_argument(
        "--pairs",
        help="只跑指定配对，形如 tokio-compio（可用逗号分隔多个）",
    )
    parser.add_argument("--skip-build", action="store_true", help="跳过编译")
    parser.add_argument("--offline", action="store_true", help="cargo --offline")
    args = parser.parse_args()

    offline = "--offline" if args.offline else ""

    if args.pairs:
        pairs: list[tuple[str, str]] = []
        for item in args.pairs.split(","):
            left, _, right = item.partition("-")
            if not left or not right:
                print(f"配对写法应为 <a>-<b>，收到 {item!r}", file=sys.stderr)
                return 2
            pairs.append((left, right))
    else:
        pairs = PAIRS

    if args.skip_build:
        globals()["build_peer"] = lambda runtime, offline: None  # type: ignore[assignment]

    outcomes = [run_case(server, client, offline) for server, client in pairs]

    print("\n=== 汇总 ===")
    for item in outcomes:
        mark = "通过" if item["ok"] else "失败"
        print(
            f"  {item['server']:>6} → {item['client']:<6} {mark}"
            f"  ({item['detail']})"
        )
    failed = [item for item in outcomes if not item["ok"]]
    if failed:
        print(f"\n{len(failed)}/{len(outcomes)} 组失败", file=sys.stderr)
        return 1
    print(f"\n全部 {len(outcomes)} 组通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
