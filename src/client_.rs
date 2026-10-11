//! 基于 `mptp_core::client` 的客户端侧。
//!
//! 这里唯一的实质内容是**两套配置的具体实现**：一整套分配约定、一份客户端配置。
//! 服务端那一份在 [`crate::server_`]，两者**类型上彼此独立**——这正是 `mptp_core`
//! 把客户端与服务端配置分开的用意。
//!
//! 配置按**传输的两个半边**泛型化：进程内环回用 `BufferedTx/BufferedRx`，三个后端的
//! socket 用各自的设备适配器，都是不同的具体类型。这样同一份协议场景代码能在
//! tokio / compio / smol 上逐字复用。

use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::TrProducerState,
    x_deps::abs_cancel,
};
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow};
use mm_ptr::Shared;
use mm_ptr::x_deps::abs_mm;
use mptp_core::{
    access_method::AccessMethod,
    client::{Client, TrClienAllocConfig, TrClient, TrSession, config::TrClientConfig},
    messaging::{Nothing, Request, Response},
    specs::Status,
};
use smux_v1::{
    connection::Dock,
    x_deps::{abs_buff, mm_ptr},
};

use crate::mux_::{DemoConn, DemoRingBuff, K_CHANNEL_CAP, make_channel_buffs_};

/// 本 demo 的资源分配约定（客户端侧）。
pub struct DemoClientAllocCfg;

impl TrClienAllocConfig for DemoClientAllocCfg {
    type SharedConnAlloc = CoreAlloc;

    type RingAlloc = CoreAlloc;

    type RingBuff = DemoRingBuff;

    const RING_CAPACITY: usize = K_CHANNEL_CAP;

    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff) {
        make_channel_buffs_()
    }
}

/// 本 demo 的客户端配置。
///
/// 泛型参数是传输的两个半边，见模块文档。
pub struct DemoClientCfg<Tx, Rx>(PhantomData<fn() -> (Tx, Rx)>);

impl<Tx, Rx> TrClientConfig for DemoClientCfg<Tx, Rx>
where
    Tx: 'static + TrBuffWrite<u8> + TrProducerState,
    Rx: 'static + TrBuffRead<u8>,
{
    type MuxConn = DemoConn<Tx, Rx>;

    type AllocCfg = DemoClientAllocCfg;

    type SharedConn = Shared<DemoConn<Tx, Rx>, CoreAlloc>;

    type Request = Request<Nothing, Nothing>;

    type Response = Response<Nothing, Nothing>;
}

/// 向 `remote_dock` 发一次 `View /hello` 请求，返回对端给出的状态码。
///
/// 取消令牌用 [`NonCancellableToken`]：本 demo 只验证一次成功往返，不涉及取消。
///
/// # Errors
///
/// 建流、发送请求前缀或接收响应前缀任一步失败都会带上下文返回 `Err`。
pub async fn run_client_<Tx, Rx>(
    conn: Shared<DemoConn<Tx, Rx>, CoreAlloc>,
    remote_dock: u32,
) -> Result<Status>
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    let client = Client::<DemoClientCfg<Tx, Rx>>::new(conn, Dock::new(remote_dock));
    let request: Request<Nothing, Nothing> = Request::new(AccessMethod::View, "/hello");

    let mut session = client
        .request_async(request)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("发起请求失败: {err:?}"))?;

    let prefix = session
        .recv_resp_header_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("接收响应失败: {err}"))?;

    Result::Ok(prefix.status())
}
