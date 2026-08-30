use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use chrono::Utc;
use fault_model::{
    DelayRecord, FaultRecord, FaultStatus, TcpStreamOutcome, TcpStreamRecord,
    TcpStreamStatus, TransportFailure, TransportProtocol, TransportRecord,
    TransportStatus, TransportSummary, UdpExchangeOutcome, UdpExchangeRecord,
    UdpExchangeStatus,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::EngineError;
use crate::faults::stream::{Bidirectional, StreamLayer};

pub(crate) struct TransportMetrics {
    live: Arc<LiveMetrics>,
    protocol: TransportProtocol,
    impacted: AtomicBool,
    bytes_to_upstream: AtomicU64,
    bytes_to_client: AtomicU64,
    latency_applications: AtomicU64,
    latency_delay_micros: AtomicU64,
    jitter_applications: AtomicU64,
    jitter_delay_micros: AtomicU64,
    bandwidth_bytes_limited: AtomicU64,
    blackhole_to_upstream: AtomicBool,
    blackhole_to_client: AtomicBool,
    reset_injected: AtomicBool,
    dns_interventions: AtomicU64,
}

impl TransportMetrics {
    fn new(live: Arc<LiveMetrics>, protocol: TransportProtocol) -> Self {
        Self {
            live,
            protocol,
            impacted: AtomicBool::new(false),
            bytes_to_upstream: AtomicU64::new(0),
            bytes_to_client: AtomicU64::new(0),
            latency_applications: AtomicU64::new(0),
            latency_delay_micros: AtomicU64::new(0),
            jitter_applications: AtomicU64::new(0),
            jitter_delay_micros: AtomicU64::new(0),
            bandwidth_bytes_limited: AtomicU64::new(0),
            blackhole_to_upstream: AtomicBool::new(false),
            blackhole_to_client: AtomicBool::new(false),
            reset_injected: AtomicBool::new(false),
            dns_interventions: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_latency(&self, duration: Duration) {
        self.mark_impacted();
        self.latency_applications.fetch_add(1, Ordering::Relaxed);
        let micros = micros(duration);
        self.latency_delay_micros.fetch_add(micros, Ordering::Relaxed);
        self.live.latency_applications.fetch_add(1, Ordering::Relaxed);
        self.live.latency_delay_micros.fetch_add(micros, Ordering::Relaxed);
    }

    pub(crate) fn record_jitter(&self, duration: Duration) {
        self.mark_impacted();
        self.jitter_applications.fetch_add(1, Ordering::Relaxed);
        let micros = micros(duration);
        self.jitter_delay_micros.fetch_add(micros, Ordering::Relaxed);
        self.live.jitter_applications.fetch_add(1, Ordering::Relaxed);
        self.live.jitter_delay_micros.fetch_add(micros, Ordering::Relaxed);
    }

    pub(crate) fn record_bandwidth(&self, bytes: usize) {
        self.mark_impacted();
        self.bandwidth_bytes_limited.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_blackhole(&self, to_upstream: bool) {
        self.mark_impacted();
        let flag = if to_upstream {
            &self.blackhole_to_upstream
        } else {
            &self.blackhole_to_client
        };
        flag.store(true, Ordering::Relaxed);
    }

    pub(crate) fn record_reset(&self) {
        self.mark_impacted();
        self.reset_injected.store(true, Ordering::Relaxed);
    }

    pub(crate) fn record_dns_intervention(&self) {
        self.mark_impacted();
        self.dns_interventions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_bytes_to_upstream(&self, bytes: usize) {
        self.bytes_to_upstream.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_bytes_to_client(&self, bytes: usize) {
        self.bytes_to_client.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn was_reset_injected(&self) -> bool {
        self.reset_injected.load(Ordering::Relaxed)
    }

    fn mark_impacted(&self) {
        if self
            .impacted
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.live.mark_impacted(self.protocol);
        }
    }

    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_to_upstream: self.bytes_to_upstream.load(Ordering::Relaxed),
            bytes_to_client: self.bytes_to_client.load(Ordering::Relaxed),
            faults: FaultRecord {
                latency: DelayRecord {
                    applications: self
                        .latency_applications
                        .load(Ordering::Relaxed),
                    total_delay_ms: milliseconds(
                        self.latency_delay_micros.load(Ordering::Relaxed),
                    ),
                },
                jitter: DelayRecord {
                    applications: self
                        .jitter_applications
                        .load(Ordering::Relaxed),
                    total_delay_ms: milliseconds(
                        self.jitter_delay_micros.load(Ordering::Relaxed),
                    ),
                },
                bandwidth_bytes_limited: self
                    .bandwidth_bytes_limited
                    .load(Ordering::Relaxed),
                blackhole_activations: u64::from(
                    self.blackhole_to_upstream.load(Ordering::Relaxed),
                ) + u64::from(
                    self.blackhole_to_client.load(Ordering::Relaxed),
                ),
                connection_resets: u64::from(
                    self.reset_injected.load(Ordering::Relaxed),
                ),
                dns_interventions: self
                    .dns_interventions
                    .load(Ordering::Relaxed),
            },
        }
    }
}

#[derive(Default)]
struct ProtocolMetrics {
    active: AtomicU64,
    started: AtomicU64,
    active_impacted: AtomicU64,
    impacted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    bytes_to_upstream: AtomicU64,
    bytes_to_client: AtomicU64,
}

#[derive(Default)]
struct LiveMetrics {
    tcp: ProtocolMetrics,
    udp: ProtocolMetrics,
    dropped_records: AtomicU64,
    last_failure: ArcSwapOption<TransportFailure>,
    latency_applications: AtomicU64,
    latency_delay_micros: AtomicU64,
    jitter_applications: AtomicU64,
    jitter_delay_micros: AtomicU64,
}

impl LiveMetrics {
    fn protocol(&self, protocol: TransportProtocol) -> &ProtocolMetrics {
        match protocol {
            TransportProtocol::Tcp => &self.tcp,
            TransportProtocol::Udp => &self.udp,
        }
    }

    fn opened(&self, protocol: TransportProtocol) {
        let metrics = self.protocol(protocol);
        metrics.active.fetch_add(1, Ordering::Relaxed);
        metrics.started.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_impacted(&self, protocol: TransportProtocol) {
        let metrics = self.protocol(protocol);
        metrics.active_impacted.fetch_add(1, Ordering::Relaxed);
        metrics.impacted.fetch_add(1, Ordering::Relaxed);
    }

    fn completed(
        &self,
        protocol: TransportProtocol,
        snapshot: &MetricsSnapshot,
        impacted: bool,
        failure: Option<&TransportFailure>,
    ) {
        let metrics = self.protocol(protocol);
        metrics.active.fetch_sub(1, Ordering::Relaxed);
        metrics.completed.fetch_add(1, Ordering::Relaxed);
        metrics
            .bytes_to_upstream
            .fetch_add(snapshot.bytes_to_upstream, Ordering::Relaxed);
        metrics
            .bytes_to_client
            .fetch_add(snapshot.bytes_to_client, Ordering::Relaxed);
        if impacted {
            metrics.active_impacted.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(failure) = failure {
            metrics.failed.fetch_add(1, Ordering::Relaxed);
            self.last_failure.store(Some(Arc::new(failure.clone())));
        }
    }

    fn status(&self) -> TransportStatus {
        let tcp_completed = self.tcp.completed.load(Ordering::Relaxed);
        let udp_completed = self.udp.completed.load(Ordering::Relaxed);
        let latency_applications =
            self.latency_applications.load(Ordering::Relaxed);
        let jitter_applications =
            self.jitter_applications.load(Ordering::Relaxed);
        TransportStatus {
            tcp: TcpStreamStatus {
                active: self.tcp.active.load(Ordering::Relaxed),
                opened: self.tcp.started.load(Ordering::Relaxed),
                active_impacted: self
                    .tcp
                    .active_impacted
                    .load(Ordering::Relaxed),
                impacted: self.tcp.impacted.load(Ordering::Relaxed),
                completed: tcp_completed,
                failed: self.tcp.failed.load(Ordering::Relaxed),
                average_bytes_to_upstream: average(
                    self.tcp.bytes_to_upstream.load(Ordering::Relaxed),
                    tcp_completed,
                ),
                average_bytes_to_client: average(
                    self.tcp.bytes_to_client.load(Ordering::Relaxed),
                    tcp_completed,
                ),
            },
            udp: UdpExchangeStatus {
                active: self.udp.active.load(Ordering::Relaxed),
                started: self.udp.started.load(Ordering::Relaxed),
                active_impacted: self
                    .udp
                    .active_impacted
                    .load(Ordering::Relaxed),
                impacted: self.udp.impacted.load(Ordering::Relaxed),
                completed: udp_completed,
                failed: self.udp.failed.load(Ordering::Relaxed),
                average_request_bytes: average(
                    self.udp.bytes_to_upstream.load(Ordering::Relaxed),
                    udp_completed,
                ),
                average_response_bytes: average(
                    self.udp.bytes_to_client.load(Ordering::Relaxed),
                    udp_completed,
                ),
            },
            effects: FaultStatus {
                latency_applications,
                average_latency_ms: average_micros(
                    self.latency_delay_micros.load(Ordering::Relaxed),
                    latency_applications,
                ),
                jitter_applications,
                average_jitter_ms: average_micros(
                    self.jitter_delay_micros.load(Ordering::Relaxed),
                    jitter_applications,
                ),
            },
            dropped_records: self.dropped_records.load(Ordering::Relaxed),
            last_failure: self.last_failure.load_full().as_deref().cloned(),
        }
    }
}

struct MetricsSnapshot {
    bytes_to_upstream: u64,
    bytes_to_client: u64,
    faults: FaultRecord,
}

struct Entry {
    protocol: TransportProtocol,
    proxy: String,
    peer: String,
    upstream: String,
    started_at: chrono::DateTime<Utc>,
    metrics: Arc<TransportMetrics>,
}

enum ObservationOutcome {
    Tcp(TcpStreamOutcome),
    Udp(UdpExchangeOutcome),
}

enum Message {
    Open {
        id: Uuid,
        entry: Entry,
    },
    Close {
        id: Uuid,
        completed_at: chrono::DateTime<Utc>,
        outcome: ObservationOutcome,
        failure: Option<TransportFailure>,
    },
    Snapshot {
        response: oneshot::Sender<TransportSummary>,
    },
    Stop {
        response: oneshot::Sender<TransportSummary>,
    },
}

#[derive(Clone)]
pub(crate) struct ObservationRecorder {
    sender: mpsc::Sender<Message>,
    live: Arc<LiveMetrics>,
}

impl ObservationRecorder {
    pub(crate) fn start(
        retain_completed: bool,
        completed_sender: Option<mpsc::Sender<TransportRecord>>,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(1_024);
        let live = Arc::new(LiveMetrics::default());
        let task = tokio::spawn(collect(
            receiver,
            Arc::clone(&live),
            retain_completed,
            completed_sender,
        ));
        (Self { sender, live }, task)
    }

    pub(crate) async fn open(
        &self,
        id: Uuid,
        protocol: TransportProtocol,
        proxy: String,
        peer: String,
        upstream: String,
        metrics: Arc<TransportMetrics>,
    ) -> Result<ObservationGuard, EngineError> {
        self.live.opened(protocol);
        let entry = Entry {
            protocol,
            proxy,
            peer,
            upstream,
            started_at: Utc::now(),
            metrics,
        };
        self.sender
            .send(Message::Open { id, entry })
            .await
            .map_err(|_| recorder_stopped())?;
        Ok(ObservationGuard { id: Some(id), protocol, recorder: self.clone() })
    }

    pub(crate) fn metrics(
        &self,
        protocol: TransportProtocol,
    ) -> Arc<TransportMetrics> {
        Arc::new(TransportMetrics::new(Arc::clone(&self.live), protocol))
    }

    pub(crate) async fn snapshot(
        &self,
    ) -> Result<TransportSummary, EngineError> {
        let (response, receive) = oneshot::channel();
        self.sender
            .send(Message::Snapshot { response })
            .await
            .map_err(|_| recorder_stopped())?;
        receive.await.map_err(|_| recorder_stopped())
    }

    pub(crate) fn status(&self) -> TransportStatus {
        self.live.status()
    }

    pub(crate) async fn stop(&self) -> Result<TransportSummary, EngineError> {
        let (response, receive) = oneshot::channel();
        self.sender
            .send(Message::Stop { response })
            .await
            .map_err(|_| recorder_stopped())?;
        receive.await.map_err(|_| recorder_stopped())
    }

    async fn close(
        &self,
        id: Uuid,
        outcome: ObservationOutcome,
        failure: Option<TransportFailure>,
    ) {
        let _ = self
            .sender
            .send(Message::Close {
                id,
                completed_at: Utc::now(),
                outcome,
                failure,
            })
            .await;
    }
}

pub(crate) struct ObservationGuard {
    id: Option<Uuid>,
    protocol: TransportProtocol,
    recorder: ObservationRecorder,
}

impl ObservationGuard {
    pub(crate) async fn finish_tcp(
        mut self,
        outcome: TcpStreamOutcome,
        failure: Option<TransportFailure>,
    ) {
        debug_assert_eq!(self.protocol, TransportProtocol::Tcp);
        if let Some(id) = self.id.take() {
            self.recorder
                .close(id, ObservationOutcome::Tcp(outcome), failure)
                .await;
        }
    }

    pub(crate) async fn finish_udp(
        mut self,
        outcome: UdpExchangeOutcome,
        failure: Option<TransportFailure>,
    ) {
        debug_assert_eq!(self.protocol, TransportProtocol::Udp);
        if let Some(id) = self.id.take() {
            self.recorder
                .close(id, ObservationOutcome::Udp(outcome), failure)
                .await;
        }
    }
}

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else { return };
        let outcome = match self.protocol {
            TransportProtocol::Tcp => {
                ObservationOutcome::Tcp(TcpStreamOutcome::Cancelled)
            }
            TransportProtocol::Udp => {
                ObservationOutcome::Udp(UdpExchangeOutcome::Cancelled)
            }
        };
        let _ = self.sender().try_send(Message::Close {
            id,
            completed_at: Utc::now(),
            outcome,
            failure: None,
        });
    }
}

impl ObservationGuard {
    fn sender(&self) -> &mpsc::Sender<Message> {
        &self.recorder.sender
    }
}

async fn collect(
    mut receiver: mpsc::Receiver<Message>,
    live: Arc<LiveMetrics>,
    retain_completed: bool,
    mut completed_sender: Option<mpsc::Sender<TransportRecord>>,
) {
    let mut entries = HashMap::new();
    let mut completed = Vec::new();
    while let Some(message) = receiver.recv().await {
        match message {
            Message::Open { id, entry } => {
                entries.insert(id, entry);
            }
            Message::Close { id, completed_at, outcome, failure } => {
                let Some(entry) = entries.remove(&id) else { continue };
                let snapshot = entry.metrics.snapshot();
                let record = transport_record(
                    id,
                    &entry,
                    Some(completed_at),
                    outcome,
                    failure,
                );
                live.completed(
                    entry.protocol,
                    &snapshot,
                    entry.metrics.impacted.load(Ordering::Relaxed),
                    record_failure(&record),
                );
                if let Some(sender) = &completed_sender {
                    match sender.try_send(record.clone()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            live.dropped_records
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            completed_sender = None;
                        }
                    }
                }
                if retain_completed {
                    completed.push(record);
                }
            }
            Message::Snapshot { response } => {
                let _ = response.send(snapshot(&entries, &completed, &live));
            }
            Message::Stop { response } => {
                let _ = response.send(snapshot(&entries, &completed, &live));
                break;
            }
        }
    }
}

fn snapshot(
    entries: &HashMap<Uuid, Entry>,
    completed: &[TransportRecord],
    live: &LiveMetrics,
) -> TransportSummary {
    let mut records = completed.to_vec();
    records.extend(entries.iter().map(|(id, entry)| {
        let outcome = match entry.protocol {
            TransportProtocol::Tcp => {
                ObservationOutcome::Tcp(TcpStreamOutcome::Active)
            }
            TransportProtocol::Udp => {
                ObservationOutcome::Udp(UdpExchangeOutcome::Active)
            }
        };
        transport_record(*id, entry, None, outcome, None)
    }));
    let mut tcp_streams = Vec::new();
    let mut udp_exchanges = Vec::new();
    for record in records {
        match record {
            TransportRecord::TcpStream { stream } => tcp_streams.push(stream),
            TransportRecord::UdpExchange { exchange } => {
                udp_exchanges.push(exchange)
            }
        }
    }
    tcp_streams.sort_by_key(|record| (record.opened_at, record.stream_id));
    udp_exchanges.sort_by_key(|record| (record.started_at, record.exchange_id));
    TransportSummary { status: live.status(), tcp_streams, udp_exchanges }
}

fn transport_record(
    id: Uuid,
    entry: &Entry,
    completed_at: Option<chrono::DateTime<Utc>>,
    outcome: ObservationOutcome,
    failure: Option<TransportFailure>,
) -> TransportRecord {
    let metrics = entry.metrics.snapshot();
    match outcome {
        ObservationOutcome::Tcp(outcome) => TransportRecord::TcpStream {
            stream: TcpStreamRecord {
                stream_id: id,
                proxy: entry.proxy.clone(),
                peer: entry.peer.clone(),
                upstream: entry.upstream.clone(),
                opened_at: entry.started_at,
                closed_at: completed_at,
                bytes_to_upstream: metrics.bytes_to_upstream,
                bytes_to_client: metrics.bytes_to_client,
                faults: metrics.faults,
                outcome,
                failure,
            },
        },
        ObservationOutcome::Udp(outcome) => TransportRecord::UdpExchange {
            exchange: UdpExchangeRecord {
                exchange_id: id,
                proxy: entry.proxy.clone(),
                peer: entry.peer.clone(),
                upstream: entry.upstream.clone(),
                started_at: entry.started_at,
                completed_at,
                request_bytes: metrics.bytes_to_upstream,
                response_bytes: metrics.bytes_to_client,
                faults: metrics.faults,
                outcome,
                failure,
            },
        },
    }
}

fn record_failure(record: &TransportRecord) -> Option<&TransportFailure> {
    match record {
        TransportRecord::TcpStream { stream } => stream.failure.as_ref(),
        TransportRecord::UdpExchange { exchange } => exchange.failure.as_ref(),
    }
}

fn recorder_stopped() -> EngineError {
    EngineError::ObservationRecorderStopped
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn milliseconds(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn average(total: u64, count: u64) -> u64 {
    total.checked_div(count).unwrap_or(0)
}

fn average_micros(total: u64, count: u64) -> f64 {
    if count == 0 { 0.0 } else { total as f64 / count as f64 / 1_000.0 }
}

pub(crate) struct ObservedStream {
    inner: Box<dyn Bidirectional>,
    metrics: Arc<TransportMetrics>,
}

impl ObservedStream {
    pub(crate) fn new(
        inner: Box<dyn Bidirectional>,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self { inner, metrics }
    }
}

impl Bidirectional for ObservedStream {
    fn reset(&self) -> std::io::Result<()> {
        self.inner.reset()
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Base(self)
    }
}

impl AsyncRead for ObservedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = &result {
            let read = buffer.filled().len().saturating_sub(before);
            self.metrics.record_bytes_to_upstream(read);
        }
        result
    }
}

impl AsyncWrite for ObservedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = result {
            self.metrics.record_bytes_to_client(written);
            Poll::Ready(Ok(written))
        } else {
            result
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
