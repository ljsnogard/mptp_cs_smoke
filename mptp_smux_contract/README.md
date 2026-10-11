# mptp_smux_contract

`mptp_core` 与 `smux_v1` 之间的**需求对齐检验**。

同 workspace 的 [`mptp_cs_smoke`](../README.md) 问的是「整条链路能不能跑通」（端到端冒烟，
成功即通过）。本 crate 问的是另一件事：**两边对同一件事的约定是否一致**。

做法是把 `mptp_core` 对 `smux_v1` 的依赖逐条写成**可断言、可失败**的用例：

- 用例通过 = 该项约定确实对齐；
- 用例失败 = 一处**需求缺口**（不是「测试写错了」），失败信息会指出缺口在哪一侧。

---

## 1. 为什么要有它

起因是一个实测现象：`mptp_cs_smoke` 的进程内往返要 **30.0 秒**才结束，而客户端读到响应
只用了 **0.01 秒**。30.0 秒正好等于 `BasicOpts::DEFAULT.max_channel_timeout`，于是问题
收敛成一句话：

> `mptp_core` 的收尾依赖「对端关闭 → 读端立刻 EOF」，而 `smux_v1` 把关闭定义成
> 两个**正交方向**（`FIN` / `RESET`）。这两句话等价吗？

推理回答不了这个问题，所以把它变成用例。

## 2. 跑法

前置：**Nightly 工具链**，以及同仓的 `mptp_rpc/mptp_core` / `smux_v1` 已在位。

```console
# 在 workspace 根（也就是 mptp_cs_smoke/）执行
$ just contract                 # 只跑契约检验
$ just contract-verbose         # 连每个用例的观测值一起打出来
$ just check                    # 冒烟 + 契约一起跑

# 或者直接 cargo
$ cargo test -p mptp_smux_contract --no-default-features --features rt-tokio
```

断网 / 本机 cargo 缓存只读时，前面加 `SMOKE_CARGO_OFFLINE=--offline`。

## 3. 首轮覆盖的契约（拆流）

| 用例文件 | 检验的契约 | 判据 |
| --- | --- | --- |
| [`tests/fin_signal_.rs`](tests/fin_signal_.rs) | `drop(tx)` 发 `FIN`，对端**轮询**能观测到「对端不再发送」 | 3 秒窗口内 `is_tx_closed()` 为真 |
| [`tests/reset_signal_.rs`](tests/reset_signal_.rs) | `drop(rx)` 发 `RESET`，**不**关对端接收环写端（§7.0 的正交性） | 200 毫秒窗口内 `is_tx_closed()` **始终为假** |
| [`tests/idle_timeout_.rs`](tests/idle_timeout_.rs) | 空闲超时拆流时，**补发两条 `CLOSE`** 唤醒对端（§7.1） | 主动方 1 秒超时后，被动方（自己 10 秒才超时）在 3 秒内被唤醒 |
| [`tests/fin_wakes_reader_.rs`](tests/fin_wakes_reader_.rs) | `FIN` 到达时能唤醒**已经 park 在同步读上**的读者 | 阻塞的 `AsStdRead::read` 在 800 毫秒内返回 |

第 4 条是这一组里最关键的一条：`mptp_core::serving` 的收尾**不是轮询**，而是
`drain_until_eof_(&mut rx)` —— 一次**阻塞**的同步读（`AsStdRead`）。因此「FIN 能不能唤醒
park 的读者」才是它真正依赖的行为。`smux_v1` 自己在 `StageCloseGuard_` 的注释里点明过这个
陷阱（只 drop 环写端不会唤醒 park 的读者），本用例就是把那句话变成断言。

## 4. 当前结论（2026-10-11 实测）

**四条全部通过。** 因此：

1. `drop(tx)` → `FIN` → 对端观测到关闭：**即时**（实测 0.01 秒）；
2. `drop(rx)` → `RESET` 与「我不再发送」正交：**成立**（对端接收环未被误关）；
3. 空闲超时拆流补发 `CLOSE`：**成立**（对端被唤醒，且本端 `abort_reason` 为 `None`，
   说明不是它自己拆的）；
4. `FIN` 唤醒阻塞中的同步读：**成立**（实测 110 微秒返回 `Ok(0)`）。

也就是说，**`smux_v1` 的拆流契约与 `mptp_core` 的假设在我能构造的这几条路径上是自洽的**。
最初那个 30 秒延迟**不能**归因于「`FIN` 没发」或「`RESET` 被当成 `FIN`」——这两条已被
上面的用例证伪。剩下的可疑面因此收敛到 `mptp_core` **如何使用**这些原语，而不是原语本身。

> 写用例时踩到过一个假阳性，值得记下来：最初版本的 `reset_signal_` 用例在 `drop(rx)` 之后
> 紧接着 `drop(tx)`，于是对端同时收到了 `RESET` **和** `FIN`，用例失败并「指认」实现把
> `RESET` 当成了 `FIN`。把发送半边用 `std::mem::forget` 留活之后，用例通过。
> **构造契约用例时，必须把被检验的信号与其它信号隔离开**，否则失败结论会指向错误的一侧。

## 5. 已知限制

- **只在 `rt-tokio` 装配下实现**：连接工厂（`src/loopback_.rs`）的握手要把 `invite` 与
  `listen` 合到一次驱动里（`tokio::join!`）。compio / smol 各自有等价的驱动方式，但尚未在
  同一个函数里统一——这与 `mptp_cs_smoke` 的 `run_local_roundtrip_` 是**同一个**限制。
- **只做进程内环回**：所有用例都跑两条全被动内存环，不经 socket。传输层的问题留给
  `mptp_cs_smoke` 的 `just pairs`。
- **宽限期「清理注册项」还没有直接覆盖**：`smux_v1` 的注册表是私有的，没有公开的
  「某 dock 对是否仍在宽限态」查询接口，而它的自动分配**刻意避免**刚释放的 dock 对，因此
  从外部无法稳定构造「复用同一个 dock 对」来观察墓碑回收。要覆盖它需要 `smux_v1` 提供
  观测点（或暴露一个 `#[doc(hidden)]` 的查询）。
- 用例刻意把两端的 `max_channel_timeout` 压到协议下限（1 秒）或拉开差距（1 秒 vs 10 秒），
  这样「信号到了」与「等到超时」在时间上量级分明。**断言的时间预算本身就是判据的一部分**，
  改这些常量等于改语义。
