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

pub(crate) struct BlackholeStream {
    inner: Box<dyn Bidirectional>,
    flow: TrafficFlow,
    metrics: Arc<TransportMetrics>,
}

impl BlackholeStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        flow: TrafficFlow,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self { inner, flow, metrics }
    }
}

impl Bidirectional for BlackholeStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Inner(self.inner)
    }
}

impl AsyncRead for BlackholeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buffer.remaining() == 0
            || !matches!(self.flow, TrafficFlow::ToUpstream | TrafficFlow::Both)
        {
            return Pin::new(&mut self.inner).poll_read(context, buffer);
        }

        self.metrics.record_blackhole(true);
        Poll::Pending
    }
}

impl AsyncWrite for BlackholeStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buffer.is_empty()
            || !matches!(self.flow, TrafficFlow::ToClient | TrafficFlow::Both)
        {
            return Pin::new(&mut self.inner).poll_write(context, buffer);
        }

        self.metrics.record_blackhole(false);
        Poll::Pending
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
