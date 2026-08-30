use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arc_swap::ArcSwap;
use fault_model::FaultSpec;
use fault_model::TransportProtocol;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use super::FaultChain;
use super::InjectionContext;
use super::stream::Bidirectional;
use super::stream::StreamLayer;
use crate::EngineError;

pub(crate) struct ActiveFaults {
    specs: Vec<FaultSpec>,
    chain: FaultChain,
    changed: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct FaultRuntime {
    active: Arc<ArcSwap<ActiveFaults>>,
    protocol: TransportProtocol,
}

impl FaultRuntime {
    pub(crate) fn new(
        specs: &[FaultSpec],
        protocol: TransportProtocol,
    ) -> Result<Self, EngineError> {
        fault_model::validate_faults(specs)?;
        validate_protocol_faults(protocol, specs)?;
        let active = ActiveFaults {
            specs: specs.to_vec(),
            chain: FaultChain::from_specs(specs)?,
            changed: CancellationToken::new(),
        };
        Ok(Self { active: Arc::new(ArcSwap::from_pointee(active)), protocol })
    }

    pub(crate) fn set_faults(
        &self,
        specs: &[FaultSpec],
    ) -> Result<(), EngineError> {
        let next = self.prepare_specs(specs)?;
        self.apply(next);
        Ok(())
    }

    pub(crate) fn prepare(
        specs: &[FaultSpec],
    ) -> Result<Arc<ActiveFaults>, EngineError> {
        fault_model::validate_faults(specs)?;
        Ok(Arc::new(ActiveFaults {
            specs: specs.to_vec(),
            chain: FaultChain::from_specs(specs)?,
            changed: CancellationToken::new(),
        }))
    }

    pub(crate) fn prepare_specs(
        &self,
        specs: &[FaultSpec],
    ) -> Result<Arc<ActiveFaults>, EngineError> {
        validate_protocol_faults(self.protocol, specs)?;
        Self::prepare(specs)
    }

    pub(crate) fn apply(&self, next: Arc<ActiveFaults>) {
        let previous = self.active.swap(next);
        previous.changed.cancel();
    }

    fn snapshot(&self) -> Arc<ActiveFaults> {
        self.active.load_full()
    }

    pub(crate) fn active_specs(&self) -> Vec<FaultSpec> {
        self.snapshot().specs.clone()
    }
}

fn validate_protocol_faults(
    protocol: TransportProtocol,
    specs: &[FaultSpec],
) -> Result<(), EngineError> {
    for spec in specs {
        let supported = matches!(
            (protocol, spec),
            (TransportProtocol::Tcp, FaultSpec::Latency { .. })
                | (TransportProtocol::Tcp, FaultSpec::Jitter { .. })
                | (TransportProtocol::Tcp, FaultSpec::Bandwidth { .. })
                | (TransportProtocol::Tcp, FaultSpec::Blackhole { .. })
                | (TransportProtocol::Tcp, FaultSpec::ConnectionReset { .. })
                | (TransportProtocol::Udp, FaultSpec::Latency { .. })
                | (TransportProtocol::Udp, FaultSpec::Jitter { .. })
                | (TransportProtocol::Udp, FaultSpec::Blackhole { .. })
                | (TransportProtocol::Udp, FaultSpec::Dns { .. })
        );
        if !supported {
            return Err(EngineError::InvalidFaultConfig(format!(
                "fault {spec:?} is not supported by {} proxies",
                protocol.as_str()
            )));
        }
    }
    Ok(())
}

pub(crate) struct DynamicFaultStream {
    runtime: FaultRuntime,
    active: Arc<ActiveFaults>,
    changed: Pin<Box<dyn Future<Output = ()> + Send>>,
    context: InjectionContext,
    stream: Option<Box<dyn Bidirectional>>,
}

impl DynamicFaultStream {
    pub(crate) fn new(
        stream: Box<dyn Bidirectional>,
        runtime: FaultRuntime,
        context: InjectionContext,
    ) -> Result<Self, EngineError> {
        let active = runtime.snapshot();
        let stream = active.chain.wrap_stream(&context, stream)?;
        let changed = Box::pin(active.changed.clone().cancelled_owned());
        Ok(Self { runtime, active, changed, context, stream: Some(stream) })
    }

    fn refresh(&mut self, context: &mut Context<'_>) -> std::io::Result<()> {
        while self.changed.as_mut().poll(context).is_ready() {
            let next = self.runtime.snapshot();
            if Arc::ptr_eq(&self.active, &next) {
                break;
            }

            let stream = self.stream.take().expect("dynamic stream is present");
            let base = peel_to_base(stream);
            self.stream = Some(
                next.chain
                    .wrap_stream(&self.context, base)
                    .map_err(std::io::Error::other)?,
            );
            self.changed = Box::pin(next.changed.clone().cancelled_owned());
            self.active = next;
        }
        Ok(())
    }

    fn stream(&mut self) -> &mut Box<dyn Bidirectional> {
        self.stream.as_mut().expect("dynamic stream is present")
    }
}

fn peel_to_base(mut stream: Box<dyn Bidirectional>) -> Box<dyn Bidirectional> {
    loop {
        match stream.peel() {
            StreamLayer::Inner(inner) => stream = inner,
            StreamLayer::Base(base) => return base,
        }
    }
}

impl AsyncRead for DynamicFaultStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.refresh(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream()).poll_read(context, buffer)
    }
}

impl AsyncWrite for DynamicFaultStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Err(error) = self.refresh(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream()).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.refresh(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream()).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.refresh(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream()).poll_shutdown(context)
    }
}
