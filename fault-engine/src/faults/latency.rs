use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use fault_model::TrafficFlow;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::time::Sleep;
use tokio::time::sleep;

use super::delay::DelaySampler;
use super::stream::Bidirectional;
use super::stream::StreamLayer;
use crate::observation::TransportMetrics;

pub(crate) struct LatencyStream {
    inner: Box<dyn Bidirectional>,
    sampler: DelaySampler,
    flow: TrafficFlow,
    metrics: Arc<TransportMetrics>,
    read_delay: Option<Pin<Box<Sleep>>>,
    write_delay: Option<Pin<Box<Sleep>>>,
    read_delayed: bool,
    write_delayed: bool,
}

impl LatencyStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        sampler: DelaySampler,
        flow: TrafficFlow,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self {
            inner,
            sampler,
            flow,
            metrics,
            read_delay: None,
            write_delay: None,
            read_delayed: false,
            write_delayed: false,
        }
    }

    fn poll_delay(
        delay: &mut Option<Pin<Box<Sleep>>>,
        sampler: &DelaySampler,
        metrics: &TransportMetrics,
        context: &mut Context<'_>,
    ) -> Poll<()> {
        let sleep = delay.get_or_insert_with(|| {
            let duration = sampler.sample();
            metrics.record_latency(duration);
            Box::pin(sleep(duration))
        });
        match sleep.as_mut().poll(context) {
            Poll::Ready(()) => {
                *delay = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Bidirectional for LatencyStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Inner(self.inner)
    }
}

impl AsyncRead for LatencyStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buffer.remaining() == 0 {
            return Pin::new(&mut self.inner).poll_read(context, buffer);
        }

        if matches!(self.flow, TrafficFlow::ToUpstream | TrafficFlow::Both)
            && !self.read_delayed
        {
            let sampler = self.sampler.clone();
            let metrics = self.metrics.clone();
            if Self::poll_delay(
                &mut self.read_delay,
                &sampler,
                &metrics,
                context,
            )
            .is_pending()
            {
                return Poll::Pending;
            }
            self.read_delayed = true;
        }

        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for LatencyStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if buffer.is_empty() {
            return Pin::new(&mut self.inner).poll_write(context, buffer);
        }

        if matches!(self.flow, TrafficFlow::ToClient | TrafficFlow::Both)
            && !self.write_delayed
        {
            let sampler = self.sampler.clone();
            let metrics = self.metrics.clone();
            if Self::poll_delay(
                &mut self.write_delay,
                &sampler,
                &metrics,
                context,
            )
            .is_pending()
            {
                return Poll::Pending;
            }
            self.write_delayed = true;
        }

        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}
