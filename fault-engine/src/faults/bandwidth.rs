use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use fault_model::TrafficFlow;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::time::Sleep;
use tokio::time::sleep;

use super::stream::Bidirectional;
use super::stream::StreamLayer;
use crate::observation::TransportMetrics;

const MAX_CHUNK_BYTES: usize = 1024;

pub(crate) struct BandwidthStream {
    inner: Box<dyn Bidirectional>,
    bytes_per_second: u64,
    flow: TrafficFlow,
    metrics: Arc<TransportMetrics>,
    read_delay: Option<Pin<Box<Sleep>>>,
    write_delay: Option<Pin<Box<Sleep>>>,
}

impl BandwidthStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        bytes_per_second: u64,
        flow: TrafficFlow,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self {
            inner,
            bytes_per_second,
            flow,
            metrics,
            read_delay: None,
            write_delay: None,
        }
    }

    fn poll_delay(
        delay: &mut Option<Pin<Box<Sleep>>>,
        context: &mut Context<'_>,
    ) -> Poll<()> {
        let Some(timer) = delay else {
            return Poll::Ready(());
        };
        match timer.as_mut().poll(context) {
            Poll::Ready(()) => {
                *delay = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn delay_for(bytes: usize, bytes_per_second: u64) -> Pin<Box<Sleep>> {
        let duration =
            Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64);
        Box::pin(sleep(duration))
    }
}

impl Bidirectional for BandwidthStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Inner(self.inner)
    }
}

impl AsyncRead for BandwidthStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !matches!(self.flow, TrafficFlow::ToUpstream | TrafficFlow::Both)
            || buffer.remaining() == 0
        {
            return Pin::new(&mut self.inner).poll_read(context, buffer);
        }

        if Self::poll_delay(&mut self.read_delay, context).is_pending() {
            return Poll::Pending;
        }

        let limit = buffer.remaining().min(MAX_CHUNK_BYTES);
        let mut limited = buffer.take(limit);
        let result = Pin::new(&mut self.inner).poll_read(context, &mut limited);
        match result {
            Poll::Ready(Ok(())) => {
                let filled = limited.filled().len();
                buffer.advance(filled);
                if filled > 0 {
                    self.metrics.record_bandwidth(filled);
                    self.read_delay =
                        Some(Self::delay_for(filled, self.bytes_per_second));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for BandwidthStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !matches!(self.flow, TrafficFlow::ToClient | TrafficFlow::Both)
            || buffer.is_empty()
        {
            return Pin::new(&mut self.inner).poll_write(context, buffer);
        }

        if Self::poll_delay(&mut self.write_delay, context).is_pending() {
            return Poll::Pending;
        }

        let limit = buffer.len().min(MAX_CHUNK_BYTES);
        let result =
            Pin::new(&mut self.inner).poll_write(context, &buffer[..limit]);
        match result {
            Poll::Ready(Ok(written)) => {
                if written > 0 {
                    self.metrics.record_bandwidth(written);
                    self.write_delay =
                        Some(Self::delay_for(written, self.bytes_per_second));
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
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
