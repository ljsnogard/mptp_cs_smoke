# mptp_cs_smoke

给 [`mptp_core`](../mptp_rpc/mptp_core) 做**端到端验证**的演示程序：在**一个进程内**把客户端与
服务端接起来，跑一次完整的 MPTP 往返。

它不属于 `mptp_core` 的依赖面，因此可以自由依赖具体的复用实现（[`smux_v1`](../smux_v1)）。
反过来把这些重依赖塞进 `mptp_core` 的 `dev-dependencies`，就会让「核心协议定义」这个
crate 被迫跟着传输实现一起演进。

---

## 1. 最简单的一条命令

前置：**Nightly 工具链**（本 crate 用到 `#![feature(impl_trait_in_assoc_type)]` 等），
以及同仓的 `smux_v1` / `abs_smux` / `abs_buff` 等路径依赖已在位。

在 `mptp_cs_smoke/` 目录下执行（本 crate 是独立 workspace；进程内环回目前只在 tokio
装配下提供，因此要显式指定 feature）：

```console
$ cargo run --no-default-features --features rt-tokio
== MPTP 进程内 cs 环回演示 ==
1. 在进程内建一对互连的复用连接（两条全被动环 + 本地握手，不经过 socket）
2. 服务端在 dock 1 上监听；客户端向同一个 dock 发起 View /hello
3. 服务端解码请求前缀 → 路由到 /hello → handler 回 200 OK
4. 客户端解析响应前缀


完成：客户端读到的响应状态 = 200
```

看到最后一行 `响应状态 = 200` 就说明整条链路都通了。**不需要开第二个终端**：这一条
命令里同时跑了客户端与服务端（见下面「为什么只有一个进程」）。

## 2. 跑测试

同一场景也有一份集成测试，断言比上面那句输出更严格（状态码必须等于 `Status::Ok`）：

```console
$ cargo test --no-default-features --features rt-tokio
running 1 test
test local_roundtrip_returns_ok_ ... ok

test result: ok. 1 passed; 0 failed; ...
```

## 3. 这一次往返覆盖了哪些路径

| 步骤 | 由谁负责 |
| --- | --- |
| 建一对互连的复用连接（本地握手 + 两条全被动环） | `smux_v1` |
| 客户端绑 dock、开子流、交出两块 ring 内存 | `mptp_core::client` |
| 服务端监听、接受子流、交出两块 ring 内存 | `mptp_core::serving` |
| 请求前缀（method / location / headers）的编码与解码 | `mptp_core::messaging` |
| 路由到 `/hello`、handler 返回响应、把响应前缀写回 | `mptp_core::serving` |
| 客户端解析响应前缀 | `mptp_core::client` |

**不覆盖**：任何真实传输（socket / 网络）。两条全被动环直连两个端点，中间没有泵——
这样验证的只是 `mptp_core` 的协议行为，不会被传输层的未知问题干扰。

## 4. 为什么只有一个进程

早先的 demo 是 `local-server` / `local-client` 两个子命令、两个进程、经 iroh 直连，
外加一套 relay 模式。那套东西的存在理由是「复用的流从哪来」在当时没有一个稳定的实现，
只能借 iroh 的 stream。

现在 `abs_smux` 的契约（`TrConnection` → `DockBinding` → `ChannelHandle`）已经稳定，
`smux_v1` 就是它的实现，而**两条全被动环可以直接当成传输**，于是同一个进程里就能放下
两个端点：建连、建流、收发全部走真实路径，只是没有 socket。这也是「最简单」的形态。

想跨进程时，要换的只是传输那一段：把 `src/mux_.rs` 里的两条内存环换成真实设备
（UNIX socket 等）加两条调用方驱动的泵，参考 `smux_v1/examples/active_passive.rs`。
其余部分（配置、handler、路由、客户端调用）一行都不用动。

## 5. 目录结构与各自的职责

```
src/
├── mux_.rs      进程内连接装配：两条全被动环 + 本地握手 + 连接配置
├── client_.rs   TrClientConfig 的实现，以及一次 View /hello 请求
├── server_.rs   TrServingConfig 的实现、HelloHandler、路由装配、服务一条子流
├── lib.rs       run_local_roundtrip_()：把两侧拼成一次完整往返
├── main.rs      cargo run 的入口（跑进程内环回；按装配分派）
└── bin/         peer_tokio / peer_compio / peer_smol：三个后端的跨进程 peer
tests/
└── local_e2e.rs 端到端断言（仅在 rt-tokio 装配下参与编译）
```

`main.rs` 在 `rt-tokio` 装配下跑一次进程内环回并打印状态码；另外两个装配下它如实
说明「进程内环回目前只在 rt-tokio 下提供」并退出，跨进程往返请用同装配的 `peer_*`
可执行文件。**进程内环回只在 tokio 下提供**的原因见 `main.rs` 的模块文档：三个后端
驱动本地队列的方式不同，尚未在同一个函数里统一。

注意两个配置类型是**彼此独立**的：`DemoClientCfg` 实现 `TrClientConfig`，
`DemoServingCfg` 实现 `TrServingConfig`，它们不认识对方。服务端与客户端可能跑在两个
进程里、由两份代码编译，请求 / 响应类型甚至不必来自同一个 crate——这个 demo 里两者
恰好同源，但类型上并没有绑在一起。

## 6. 改点东西试试

最快的几个实验：

- **换个路径**：改 [`server_.rs`](src/server_.rs) 里 `build_server_()` 的
  `router.add_target("/hello", chain)`，同时改 [`client_.rs`](src/client_.rs) 里
  `Request::new(AccessMethod::View, "/hello")` 的第二项。路径不匹配时会走
  `ServeError::NotFound`。
- **换个方法**：把 `AccessMethod::View` 换成别的取值，看 handler 拿到什么。
- **换个响应状态**：改 [`server_.rs`](src/server_.rs) 里 `HelloHandler` 返回的
  `Response::<Nothing, Nothing>::new(Status::Ok)`。
- **加一个 handler**：往链上多 `add_handler` 一个实现 `TrReqHandler<DemoServingCfg>`
  的类型；返回 `FlowCtrl::CallNext` 就放行给下一个，返回
  `FlowCtrl::Ceased(Some(resp))` 就终止链并回该响应。

## 7. 排错

- **卡在握手不动**：确认整段场景由 `scope.run_until(...)` 驱动。连接的读 / 写循环被
  投递到**本地作用域**（tokio 下即 `LocalSet`），不驱动就永远不会推进。
- **`abs_art-bridge` 报「必须启用一个 backend feature」**：本 crate 用
  `smux_v1` 的 `test-tokio-runtime` feature 选定后端；不要同时打开
  `smux_v1` 的 compio 侧 feature（两个后端会撞在 `abs_art-bridge` 的守护上）。
- **编排脚本卡在等 `ready`**：服务端的 `ready` 必须在 **bind 之后、accept 之前**打印。
  放到 `accept` 之后就会变成「服务端等客户端连、编排器等 `ready`、客户端等编排器放行」
  的三方互等——`scripts/run_pairs.py` 的 `readline` 又是阻塞的，表现为永久挂住而不是
  报错（`rt_tokio.rs` 的 [`Endpoint_`](src/rt_tokio.rs) 记录了这个坑）。
- **服务端跑完一次往返后不退出**：收尾要等对端 EOF，而这条信号靠**读泵退出时显式封口**
  连接读环（`smux_v1` 的 `StageCloseGuard_` 里的 `stage.close()`）唤醒 demux。只 drop
  环的写端**不会**唤醒 park 的读者（`buffex` 的环半部 drop 不置关闭标记）。
