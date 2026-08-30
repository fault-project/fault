use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fault_model::DnsCase;
use fault_model::FaultSpec;
use fault_model::TrafficFlow;
use fault_model::TransportFailure;
use fault_model::TransportFailureCategory;
use fault_model::TransportFailureStage;
use fault_model::TransportProtocol;
use fault_model::UdpExchangeOutcome;
use rand::Rng as _;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::EngineError;
use crate::RuntimeFailure;
use crate::faults::FaultRuntime;
use crate::faults::delay::DelaySampler;
use crate::observation::ObservationRecorder;
use crate::observation::TransportMetrics;

const DATAGRAM_CAPACITY: usize = 65_535;
const UPSTREAM_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct RunningUdpProxy {
    address: SocketAddr,
    task: JoinHandle<Result<(), EngineError>>,
}

impl RunningUdpProxy {
    pub(crate) async fn bind(
        listen_address: SocketAddr,
        remote_address: String,
        name: String,
        cancellation: CancellationToken,
        fault_runtime: FaultRuntime,
        observations: ObservationRecorder,
        failures: tokio::sync::broadcast::Sender<RuntimeFailure>,
    ) -> Result<Self, EngineError> {
        let socket = Arc::new(UdpSocket::bind(listen_address).await?);
        let address = socket.local_addr()?;
        let task = tokio::spawn(async move {
            let result = run(
                socket,
                remote_address,
                name,
                cancellation,
                fault_runtime,
                observations,
            )
            .await;
            if let Err(error) = &result {
                let _ = failures.send(RuntimeFailure {
                    proxy: address,
                    message: error.to_string(),
                });
            }
            result
        });
        Ok(Self { address, task })
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) async fn join(self) -> Result<(), EngineError> {
        self.task
            .await
            .map_err(|error| EngineError::ProxyTask(error.to_string()))?
    }
}

async fn run(
    socket: Arc<UdpSocket>,
    remote_address: String,
    name: String,
    cancellation: CancellationToken,
    fault_runtime: FaultRuntime,
    observations: ObservationRecorder,
) -> Result<(), EngineError> {
    let mut exchanges = JoinSet::new();
    let mut buffer = vec![0; DATAGRAM_CAPACITY];
    let context = ExchangeContext {
        listener: socket,
        remote_address,
        proxy: name,
        cancellation,
        fault_runtime,
        observations,
    };
    loop {
        tokio::select! {
            _ = context.cancellation.cancelled() => break,
            Some(completed) = exchanges.join_next(), if !exchanges.is_empty() => {
                completed.map_err(|error| EngineError::ProxyTask(error.to_string()))??;
            }
            received = context.listener.recv_from(&mut buffer) => {
                let (length, peer) = received?;
                let packet = buffer[..length].to_vec();
                exchanges.spawn(exchange(context.clone(), packet, peer));
            }
        }
    }
    exchanges.abort_all();
    while exchanges.join_next().await.is_some() {}
    Ok(())
}

#[derive(Clone)]
struct ExchangeContext {
    listener: Arc<UdpSocket>,
    remote_address: String,
    proxy: String,
    cancellation: CancellationToken,
    fault_runtime: FaultRuntime,
    observations: ObservationRecorder,
}

async fn exchange(
    context: ExchangeContext,
    packet: Vec<u8>,
    peer: SocketAddr,
) -> Result<(), EngineError> {
    let exchange_id = uuid::Uuid::new_v4();
    let metrics = context.observations.metrics(TransportProtocol::Udp);
    let guard = context
        .observations
        .open(
            exchange_id,
            TransportProtocol::Udp,
            context.proxy.clone(),
            peer.to_string(),
            context.remote_address.clone(),
            metrics.clone(),
        )
        .await?;

    match apply_datagram_faults(
        TrafficFlow::ToUpstream,
        &context.fault_runtime,
        &metrics,
        &context.cancellation,
    )
    .await?
    {
        DatagramAction::Pass => {}
        DatagramAction::Drop => {
            guard.finish_udp(UdpExchangeOutcome::FaultDropped, None).await;
            return Ok(());
        }
        DatagramAction::Cancelled => {
            guard.finish_udp(UdpExchangeOutcome::Cancelled, None).await;
            return Ok(());
        }
    }

    match apply_dns_fault(
        &packet,
        &context.fault_runtime,
        &metrics,
        &context.cancellation,
    )
    .await
    {
        DnsAction::Drop => {
            guard.finish_udp(UdpExchangeOutcome::FaultDropped, None).await;
            return Ok(());
        }
        DnsAction::Respond(response) => {
            match apply_datagram_faults(
                TrafficFlow::ToClient,
                &context.fault_runtime,
                &metrics,
                &context.cancellation,
            )
            .await?
            {
                DatagramAction::Pass => {}
                DatagramAction::Drop => {
                    guard
                        .finish_udp(UdpExchangeOutcome::FaultDropped, None)
                        .await;
                    return Ok(());
                }
                DatagramAction::Cancelled => {
                    guard.finish_udp(UdpExchangeOutcome::Cancelled, None).await;
                    return Ok(());
                }
            }
            let sent = context.listener.send_to(&response, peer).await?;
            metrics.record_bytes_to_client(sent);
            guard.finish_udp(UdpExchangeOutcome::Completed, None).await;
            return Ok(());
        }
        DnsAction::Pass => {}
        DnsAction::Cancelled => {
            guard.finish_udp(UdpExchangeOutcome::Cancelled, None).await;
            return Ok(());
        }
    }

    let upstream_address = resolve_upstream(&context.remote_address).await?;
    let bind_address =
        if upstream_address.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
    let upstream = UdpSocket::bind(bind_address).await?;
    upstream.connect(upstream_address).await?;
    upstream.send(&packet).await?;
    metrics.record_bytes_to_upstream(packet.len());

    let mut response = vec![0; DATAGRAM_CAPACITY];
    let received = tokio::select! {
        _ = context.cancellation.cancelled() => {
            guard.finish_udp(UdpExchangeOutcome::Cancelled, None).await;
            return Ok(());
        }
        received = tokio::time::timeout(
            UPSTREAM_RESPONSE_TIMEOUT,
            upstream.recv(&mut response),
        ) => received,
    };
    let length = match received {
        Ok(Ok(length)) => length,
        Ok(Err(error)) => {
            guard
                .finish_udp(
                    UdpExchangeOutcome::TransferFailed,
                    Some(transfer_failure(&context.remote_address, &error)),
                )
                .await;
            return Ok(());
        }
        Err(_) => {
            let error = std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "UDP upstream response timed out",
            );
            guard
                .finish_udp(
                    UdpExchangeOutcome::TransferFailed,
                    Some(transfer_failure(&context.remote_address, &error)),
                )
                .await;
            return Ok(());
        }
    };
    match apply_datagram_faults(
        TrafficFlow::ToClient,
        &context.fault_runtime,
        &metrics,
        &context.cancellation,
    )
    .await?
    {
        DatagramAction::Pass => {}
        DatagramAction::Drop => {
            guard.finish_udp(UdpExchangeOutcome::FaultDropped, None).await;
            return Ok(());
        }
        DatagramAction::Cancelled => {
            guard.finish_udp(UdpExchangeOutcome::Cancelled, None).await;
            return Ok(());
        }
    }
    let sent = context.listener.send_to(&response[..length], peer).await?;
    metrics.record_bytes_to_client(sent);
    guard.finish_udp(UdpExchangeOutcome::Completed, None).await;
    Ok(())
}

enum DatagramAction {
    Pass,
    Drop,
    Cancelled,
}

async fn apply_datagram_faults(
    direction: TrafficFlow,
    runtime: &FaultRuntime,
    metrics: &TransportMetrics,
    cancellation: &CancellationToken,
) -> Result<DatagramAction, EngineError> {
    for fault in runtime.active_specs() {
        match fault {
            FaultSpec::Latency { flow, distribution }
                if flow_matches(flow, direction) =>
            {
                let duration = DelaySampler::new(&distribution)?.sample();
                metrics.record_latency(duration);
                if sleep_or_cancel(duration, cancellation).await {
                    return Ok(DatagramAction::Cancelled);
                }
            }
            FaultSpec::Jitter {
                flow,
                min_delay_ms,
                max_delay_ms,
                probability,
            } if flow_matches(flow, direction)
                && rand::rng().random_bool(probability) =>
            {
                let duration = Duration::from_secs_f64(
                    rand::rng().random_range(min_delay_ms..=max_delay_ms)
                        / 1_000.0,
                );
                metrics.record_jitter(duration);
                if sleep_or_cancel(duration, cancellation).await {
                    return Ok(DatagramAction::Cancelled);
                }
            }
            FaultSpec::Blackhole { flow } if flow_matches(flow, direction) => {
                metrics.record_blackhole(direction == TrafficFlow::ToUpstream);
                return Ok(DatagramAction::Drop);
            }
            _ => {}
        }
    }
    Ok(DatagramAction::Pass)
}

fn flow_matches(configured: TrafficFlow, direction: TrafficFlow) -> bool {
    configured == TrafficFlow::Both || configured == direction
}

async fn sleep_or_cancel(
    duration: Duration,
    cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

enum DnsAction {
    Pass,
    Drop,
    Respond(Vec<u8>),
    Cancelled,
}

async fn apply_dns_fault(
    packet: &[u8],
    runtime: &FaultRuntime,
    metrics: &TransportMetrics,
    cancellation: &CancellationToken,
) -> DnsAction {
    let Some((case, delay_ms)) =
        runtime.active_specs().into_iter().find_map(|fault| match fault {
            FaultSpec::Dns { case, delay_ms } => Some((case, delay_ms)),
            _ => None,
        })
    else {
        return DnsAction::Pass;
    };

    metrics.record_dns_intervention();
    if let Some(delay_ms) = delay_ms {
        tokio::select! {
            _ = cancellation.cancelled() => return DnsAction::Cancelled,
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
        }
    }
    match case {
        DnsCase::Delay => DnsAction::Pass,
        DnsCase::Timeout => DnsAction::Drop,
        DnsCase::Truncated => dns_response(packet, 0, true)
            .map_or(DnsAction::Pass, DnsAction::Respond),
        DnsCase::Refused => dns_response(packet, 5, false)
            .map_or(DnsAction::Pass, DnsAction::Respond),
        DnsCase::ServFail => dns_response(packet, 2, false)
            .map_or(DnsAction::Pass, DnsAction::Respond),
        DnsCase::NxDomain => dns_response(packet, 3, false)
            .map_or(DnsAction::Pass, DnsAction::Respond),
        DnsCase::EmptyAnswer => dns_response(packet, 0, false)
            .map_or(DnsAction::Pass, DnsAction::Respond),
        DnsCase::RandomA => dns_random_a_response(packet)
            .map_or(DnsAction::Pass, DnsAction::Respond),
    }
}

fn dns_response(
    packet: &[u8],
    response_code: u8,
    truncated: bool,
) -> Option<Vec<u8>> {
    let end = dns_question_end(packet)?;
    let mut response = packet[..end].to_vec();
    response[2] = 0x80 | (packet[2] & 0x79);
    if truncated {
        response[2] |= 0x02;
    }
    response[3] = 0x80 | response_code;
    response[6..12].fill(0);
    Some(response)
}

fn dns_random_a_response(packet: &[u8]) -> Option<Vec<u8>> {
    let end = dns_question_end(packet)?;
    let mut response = dns_response(packet, 0, false)?;
    let query_type = u16::from_be_bytes([packet[end - 4], packet[end - 3]]);
    let query_class = u16::from_be_bytes([packet[end - 2], packet[end - 1]]);
    if query_type != 1 || query_class != 1 {
        return Some(response);
    }
    response[6..8].copy_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&[
        0xc0,
        0x0c,
        0x00,
        0x01,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x1e,
        0x00,
        0x04,
        rand::random(),
        rand::random(),
        rand::random(),
        rand::random(),
    ]);
    Some(response)
}

fn dns_question_end(packet: &[u8]) -> Option<usize> {
    if packet.len() < 12 || u16::from_be_bytes([packet[4], packet[5]]) != 1 {
        return None;
    }
    let mut position = 12;
    loop {
        let length = *packet.get(position)? as usize;
        position += 1;
        if length == 0 {
            break;
        }
        if length & 0xc0 == 0xc0 {
            position += 1;
            break;
        }
        if length > 63 || position.checked_add(length)? > packet.len() {
            return None;
        }
        position += length;
    }
    position.checked_add(4).filter(|end| *end <= packet.len())
}

async fn resolve_upstream(input: &str) -> Result<SocketAddr, EngineError> {
    tokio::net::lookup_host(input)
        .await
        .map_err(|source| EngineError::InvalidUpstreamAddress {
            input: input.to_owned(),
            source,
        })?
        .next()
        .ok_or_else(|| EngineError::InvalidUpstreamAddress {
            input: input.to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "the host resolved to no addresses",
            ),
        })
}

fn transfer_failure(
    upstream: &str,
    error: &std::io::Error,
) -> TransportFailure {
    let category = match error.kind() {
        std::io::ErrorKind::TimedOut => TransportFailureCategory::TimedOut,
        std::io::ErrorKind::NetworkUnreachable
        | std::io::ErrorKind::HostUnreachable => {
            TransportFailureCategory::NetworkUnreachable
        }
        _ => TransportFailureCategory::Other,
    };
    TransportFailure {
        stage: TransportFailureStage::Exchange,
        category,
        message: format!("UDP exchange {upstream}: {error}"),
    }
}
