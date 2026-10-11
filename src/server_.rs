//! 基于 `mptp_core::serving` 的服务端侧。
//!
//! 服务端配置与客户端配置是两个**互不相识**的类型（见 `mptp_core::serving::config`
//! 的模块文档），本文件实现的就是服务端那一份。与 [`crate::client_`] 一样，它按传输的
//! 两个半边泛型化。

use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::TrProducerState,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrChannelListener, TrDockBinding},
};
use anyhow::{Result, anyhow};
use mptp_core::{
    access_method::AccessMethod,
    messaging::{Nothing, Request, Response},
    routing::prefix_router::Router,
    serving::{
        ServingRingPrepare, TrServingAllocConfig, TrServingConfig,
        config::{ChannelRx, ChannelTx, DockBinding},
        handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
        server::{Server, SessionContext},
    },
    specs::{Headers, Status},
};
use mm_ptr::x_deps::abs_mm;
use smux_v1::x_deps::{abs_buff, abs_smux, mm_ptr};

use crate::mux_::{DemoConn, DemoRingBuff, K_CHANNEL_CAP, K_LISTEN_RESERVE, make_channel_buffs_};

/// 本 demo 的资源分配约定（服务端侧）。
pub struct DemoServingAllocCfg;

impl TrServingAllocConfig for DemoServingAllocCfg {
    type SharedConnAlloc = CoreAlloc;

    type RingAlloc = CoreAlloc;

    type RingBuff = DemoRingBuff;

    const RING_CAPACITY: usize = K_CHANNEL_CAP;

    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff) {
        make_channel_buffs_()
    }
}

/// 本 demo 的服务端配置。
pub struct DemoServingCfg<Tx, Rx>(PhantomData<fn() -> (Tx, Rx)>);

impl<Tx, Rx> TrServingConfig for DemoServingCfg<Tx, Rx>
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    type MuxConn = DemoConn<Tx, Rx>;

    type AllocCfg = DemoServingAllocCfg;

    type Request = Request<Nothing, Nothing>;

    type Response = Response<Nothing, Nothing>;
}

/// 一个最简单的 handler：无论请求什么，都回 `200 OK` 并终止链。
///
/// 它本身**不泛型**——泛型只出现在它实现的那份配置上（`impl TrReqHandler<DemoServingCfg<Tx, Rx>>`），
/// 这样 `gen_may_cancel_future` 只需处理函数自己的泛型参数。
pub struct HelloHandler;

// handler 的入参本来就宽（请求信息 + 双向半边 + 上下文 + 令牌），拆结构体只会让
// 每个 handler 多一层解构。
#[allow(clippy::too_many_arguments)]
#[gen_may_cancel_future(HandleHello, pub, new(pub(crate)))]
async fn handle_hello_async_<'h, Tx, Rx, TyTok>(
    _handler: &'h HelloHandler,
    _method: AccessMethod,
    _location: &'h str,
    _headers: &'h mut Headers,
    _tx: &'h mut ChannelTx<DemoServingCfg<Tx, Rx>>,
    _rx: &'h mut ChannelRx<DemoServingCfg<Tx, Rx>>,
    _context: &'h mut SessionContext,
    cancel: TyTok,
) -> Result<FlowCtrl<Response<Nothing, Nothing>>, HandlerError>
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    Result::Ok(FlowCtrl::Ceased(Option::Some(Response::<
        Nothing,
        Nothing,
    >::new(Status::Ok))))
}

impl<Tx, Rx> TrReqHandler<DemoServingCfg<Tx, Rx>> for HelloHandler
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    fn handle_async<'h>(
        &'h self,
        method: AccessMethod,
        location: &'h str,
        headers: &'h mut Headers,
        tx: &'h mut ChannelTx<DemoServingCfg<Tx, Rx>>,
        rx: &'h mut ChannelRx<DemoServingCfg<Tx, Rx>>,
        context: &'h mut SessionContext,
    ) -> impl TrMayCancel<
        'h,
        MayCancelOutput = Result<FlowCtrl<Response<Nothing, Nothing>>, HandlerError>,
    > {
        HandleHelloAsync::new(self, method, location, headers, tx, rx, context)
    }
}

/// 装配路由与服务端：把 `/hello` 指向只装了 [`HelloHandler`] 的链。
pub fn build_server_<Tx, Rx>() -> Server<DemoServingCfg<Tx, Rx>>
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    let mut chain = HandlerChain::new();
    chain.add_handler(HelloHandler);

    let mut router = Router::new();
    router.add_target("/hello", chain);
    Server::new(router)
}

/// 服务端：在 `binding` 上监听，接受**一条**子流并处理一个请求。
///
/// 这里没有直接用 [`Server::serve_listener_async`]，因为它是无限的 accept 循环，而本
/// demo 的每次运行只跑一次往返。两者在「`listen` → `income` → `accept` → 处理」这条
/// 路径上完全一致，只是本函数在一条子流之后返回。
///
/// # Errors
///
/// 监听、等待入向、裁决建流或处理子流任一步失败都会带上下文返回 `Err`。
pub async fn serve_one_channel_<Tx, Rx>(
    server: &Server<DemoServingCfg<Tx, Rx>>,
    binding: &mut DockBinding<DemoServingCfg<Tx, Rx>>,
    context: &mut SessionContext,
) -> Result<()>
where
    Tx: TrBuffWrite<u8> + TrProducerState + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    let mut listener = binding
        .listen_async(K_LISTEN_RESERVE)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("进入监听失败: {err}"))?;

    let mut handle = listener
        .income_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("等待入向建流失败: {err}"))?;

    // 最终裁决：本端当场交出两块 ring 内存。
    let (tx_buff, rx_buff) = <DemoServingAllocCfg as TrServingAllocConfig>::make_ring_buffs();
    let prepare = ServingRingPrepare::new(tx_buff, rx_buff);
    // 欢迎信息当前不由 MPTP 使用（上游按空载荷发出）。
    let mut welcome: &mut [u8] = &mut [];
    let (tx, rx) = handle
        .accept_async(&mut welcome, prepare)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("裁决建流失败: {err}"))?;

    server
        .serve_channel_async(tx, rx, context)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("处理子流失败: {err}"))?;

    Result::Ok(())
}
