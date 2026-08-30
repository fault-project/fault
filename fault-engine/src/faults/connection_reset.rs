use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use fault_model::TrafficFlow;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use super::stream::Bidirectional;
use super::stream::StreamLayer;
use crate::observation::TransportMetrics;

pub(crate) struct ConnectionResetStream {
    inner: Box<dyn Bidirectional>,
    flow: TrafficFlow,
    should_reset: bool,
    metrics: Arc<TransportMetrics>,
}

impl ConnectionResetStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        flow: TrafficFlow,
        should_reset: bool,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self { inner, flow, should_reset, metrics }
    }

    fn reset_error() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by fault injection",
        )
    }
}

impl Bidirectional for ConnectionResetStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Inner(self.inner)
    }
}

impl AsyncRead for ConnectionResetStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buffer.remaining() > 0
            && self.should_reset
            && matches!(self.flow, TrafficFlow::ToUpstream | TrafficFlow::Both)
        {
            self.metrics.record_reset();
            self.inner.reset()?;
            return Poll::Ready(Err(Self::reset_error()));
        }

        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for ConnectionResetStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !buffer.is_empty()
            && self.should_reset
            && matches!(self.flow, TrafficFlow::ToClient | TrafficFlow::Both)
        {
            self.metrics.record_reset();
            self.inner.reset()?;
            return Poll::Ready(Err(Self::reset_error()));
        }

        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}
