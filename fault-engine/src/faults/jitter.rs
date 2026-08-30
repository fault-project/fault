use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use fault_model::TrafficFlow;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::time::Sleep;
use tokio::time::sleep;

use super::delay::DelaySampler;
use super::stream::Bidirectional;
use super::stream::StreamLayer;
use crate::observation::TransportMetrics;

pub(crate) struct JitterStream {
    inner: Box<dyn Bidirectional>,
    sampler: DelaySampler,
    probability: f64,
    flow: TrafficFlow,
    metrics: Arc<TransportMetrics>,
    rng: SmallRng,
    read_delay: Option<Pin<Box<Sleep>>>,
    write_delay: Option<Pin<Box<Sleep>>>,
    read_ready: bool,
    write_ready: bool,
}

impl JitterStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        sampler: DelaySampler,
        probability: f64,
        flow: TrafficFlow,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self {
            inner,
            sampler,
            probability,
            flow,
            metrics,
            rng: SmallRng::from_os_rng(),
            read_delay: None,
            write_delay: None,
            read_ready: false,
            write_ready: false,
        }
    }

    fn poll_jitter(
        delay: &mut Option<Pin<Box<Sleep>>>,
        ready: &mut bool,
        sampler: &DelaySampler,
        probability: f64,
        rng: &mut SmallRng,
        metrics: &TransportMetrics,
        context: &mut Context<'_>,
    ) -> Poll<()> {
        if *ready {
            return Poll::Ready(());
        }

        if delay.is_none() {
            if !rng.random_bool(probability) {
                *ready = true;
                return Poll::Ready(());
            }
            let duration = sampler.sample();
            metrics.record_jitter(duration);
            *delay = Some(Box::pin(sleep(duration)));
        }

        let timer = delay.as_mut().expect("jitter timer was just initialized");
        match timer.as_mut().poll(context) {
            Poll::Ready(()) => {
                *delay = None;
                *ready = true;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Bidirectional for JitterStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Inner(self.inner)
    }
}

impl AsyncRead for JitterStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();
        if matches!(this.flow, TrafficFlow::ToUpstream | TrafficFlow::Both) {
            let sampler = this.sampler.clone();
            let probability = this.probability;
            if Self::poll_jitter(
                &mut this.read_delay,
                &mut this.read_ready,
                &sampler,
                probability,
                &mut this.rng,
                &this.metrics,
                context,
            )
            .is_pending()
            {
                return Poll::Pending;
            }
        }

        let result = Pin::new(&mut this.inner).poll_read(context, buffer);
        if result.is_ready() {
            this.read_ready = false;
        }
        result
    }
}

impl AsyncWrite for JitterStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if matches!(this.flow, TrafficFlow::ToClient | TrafficFlow::Both) {
            let sampler = this.sampler.clone();
            let probability = this.probability;
            if Self::poll_jitter(
                &mut this.write_delay,
                &mut this.write_ready,
                &sampler,
                probability,
                &mut this.rng,
                &this.metrics,
                context,
            )
            .is_pending()
            {
                return Poll::Pending;
            }
        }

        let result = Pin::new(&mut this.inner).poll_write(context, buffer);
        if result.is_ready() {
            this.write_ready = false;
        }
        result
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
