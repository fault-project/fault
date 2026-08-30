use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use fault_engine::EngineError;
use fault_engine::FaultEngine;
use fault_model::DelayDistribution;
use fault_model::DnsCase;
use fault_model::FaultSpec;
use fault_model::HumanDuration;
use fault_model::Phase;
use fault_model::Proxy;
use fault_model::Run;
use fault_model::RunOutcome;
use fault_model::SCHEMA_VERSION;
use fault_model::TcpStreamOutcome;
use fault_model::TrafficFlow;
use fault_model::TransportFailureCategory;
use fault_model::TransportProtocol;
use fault_model::TransportRecord;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::time::timeout;

struct TestRoutes {
    schema_version: u32,
    proxies: Vec<TestProxy>,
}

struct TestProxy {
    name: String,
    protocol: TransportProtocol,
    listen: String,
    upstream: String,
    faults: Vec<FaultSpec>,
}

fn test_engine(config: TestRoutes) -> FaultEngine {
    let proxies = config
        .proxies
        .iter()
        .map(|proxy| Proxy {
            name: proxy.name.clone(),
            protocol: proxy.protocol,
            listen: proxy.listen.clone(),
            upstream: proxy.upstream.clone(),
        })
        .collect();
    let phase_proxies = config
        .proxies
        .into_iter()
        .map(|proxy| fault_model::ProxyFaults {
            proxy: proxy.name,
            faults: proxy.faults,
        })
        .collect();
    let run = Run {
        schema_version: config.schema_version,
        name: "test run".into(),
        proxies,
        phases: vec![Phase {
            name: "active".into(),
            duration: None,
            proxies: phase_proxies,
        }],
    };
    FaultEngine::from_run(&run)
}

fn test_routes(remote: impl Into<String>) -> TestRoutes {
    TestRoutes {
        schema_version: SCHEMA_VERSION,
        proxies: vec![TestProxy {
            name: "primary".into(),
            protocol: TransportProtocol::Tcp,
            listen: "127.0.0.1:0".into(),
            upstream: remote.into(),
            faults: Vec::new(),
        }],
    }
}

fn fixed_latency(milliseconds: f64) -> FaultSpec {
    latency(milliseconds, TrafficFlow::ToUpstream)
}

fn latency(milliseconds: f64, flow: TrafficFlow) -> FaultSpec {
    FaultSpec::Latency {
        flow,
        distribution: DelayDistribution::Uniform {
            min_ms: milliseconds,
            max_ms: milliseconds,
        },
    }
}

fn bandwidth(bytes_per_second: u64) -> FaultSpec {
    FaultSpec::Bandwidth { flow: TrafficFlow::ToUpstream, bytes_per_second }
}

fn jitter(min_delay_ms: f64, max_delay_ms: f64, probability: f64) -> FaultSpec {
    FaultSpec::Jitter {
        flow: TrafficFlow::ToUpstream,
        min_delay_ms,
        max_delay_ms,
        probability,
    }
}

fn blackhole(flow: TrafficFlow) -> FaultSpec {
    FaultSpec::Blackhole { flow }
}

fn connection_reset(flow: TrafficFlow, probability: f64) -> FaultSpec {
    FaultSpec::ConnectionReset { flow, probability }
}

fn phase(name: &str, duration: &str, faults: Vec<FaultSpec>) -> Phase {
    Phase {
        name: name.into(),
        duration: Some(duration.parse::<HumanDuration>().unwrap()),
        proxies: vec![fault_model::ProxyFaults {
            proxy: "primary".into(),
            faults,
        }],
    }
}

fn local_run(phases: Vec<Phase>) -> Run {
    Run {
        schema_version: SCHEMA_VERSION,
        name: "engine lifecycle".into(),
        proxies: vec![Proxy {
            name: "primary".into(),
            protocol: TransportProtocol::Tcp,
            listen: "127.0.0.1:0".into(),
            upstream: "127.0.0.1:1".into(),
        }],
        phases,
    }
}

async fn expect_connection_reset(client: &mut TcpStream) {
    let mut byte = [0; 1];
    let error = timeout(Duration::from_secs(1), client.read(&mut byte))
        .await
        .expect("connection was not closed")
        .expect_err("connection closed cleanly instead of being reset");
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
}

async fn start_echo_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });

    (address, task)
}

#[tokio::test]
async fn returns_an_injected_dns_error_from_a_udp_proxy() {
    let config = TestRoutes {
        schema_version: SCHEMA_VERSION,
        proxies: vec![TestProxy {
            name: "dns".into(),
            protocol: TransportProtocol::Udp,
            listen: "127.0.0.1:0".into(),
            upstream: "127.0.0.1:9".into(),
            faults: vec![FaultSpec::Dns {
                case: DnsCase::NxDomain,
                delay_ms: None,
            }],
        }],
    };
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let query = [
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x07, b'e', b'x', b'a', b'm', b'p', b'l', b'e', 0x03, b'c', b'o', b'm',
        0x00, 0x00, 0x01, 0x00, 0x01,
    ];
    client.send_to(&query, running.endpoints().udp[0]).await.unwrap();
    let mut response = [0; 512];
    let length = timeout(Duration::from_secs(1), client.recv(&mut response))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&response[..2], &query[..2]);
    assert_eq!(response[3] & 0x0f, 3);

    let summary = running.shutdown().await.unwrap();
    assert_eq!(summary.udp_exchanges.len(), 1);
    assert_eq!(summary.udp_exchanges[0].faults.dns_interventions, 1);
    assert!(length >= 12);
}

#[tokio::test]
async fn applies_chained_transport_faults_to_a_udp_exchange() {
    let config = TestRoutes {
        schema_version: SCHEMA_VERSION,
        proxies: vec![TestProxy {
            name: "udp".into(),
            protocol: TransportProtocol::Udp,
            listen: "127.0.0.1:0".into(),
            upstream: "127.0.0.1:9".into(),
            faults: vec![
                latency(5.0, TrafficFlow::ToUpstream),
                jitter(1.0, 2.0, 1.0),
                blackhole(TrafficFlow::ToUpstream),
            ],
        }],
    };
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"fault", running.endpoints().udp[0]).await.unwrap();

    timeout(Duration::from_secs(1), async {
        while running.transport_status().udp.completed == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let summary = running.shutdown().await.unwrap();
    let exchange = &summary.udp_exchanges[0];
    assert_eq!(exchange.faults.latency.applications, 1);
    assert_eq!(exchange.faults.jitter.applications, 1);
    assert_eq!(exchange.faults.blackhole_activations, 1);
    assert!(matches!(
        &exchange.outcome,
        fault_model::UdpExchangeOutcome::FaultDropped
    ));
}

#[tokio::test]
async fn forwards_bytes_to_and_from_the_upstream() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let endpoint = running.endpoints().tcp[0];
    let mut client = TcpStream::connect(endpoint).await.unwrap();

    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");

    let transport = running.shutdown().await.unwrap();
    assert_eq!(transport.tcp_streams.len(), 1);
    let connection = &transport.tcp_streams[0];
    assert_eq!(connection.bytes_to_upstream, 5);
    assert_eq!(connection.bytes_to_client, 5);
    assert!(connection.closed_at.is_some());
    echo_task.abort();
}

#[tokio::test]
async fn streams_completed_connections_without_retaining_them() {
    let (upstream, echo_task) = start_echo_server().await;
    let (running, mut events) = test_engine(test_routes(upstream.to_string()))
        .start_with_transport_events(4)
        .await
        .unwrap();
    let endpoint = running.endpoints().tcp[0];
    let mut client = TcpStream::connect(endpoint).await.unwrap();

    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    client.read_exact(&mut echoed).await.unwrap();
    drop(client);

    let record =
        timeout(Duration::from_secs(1), events.recv()).await.unwrap().unwrap();
    let TransportRecord::TcpStream { stream } = record else {
        panic!("expected a TCP stream record");
    };
    assert_eq!(stream.bytes_to_upstream, 5);
    assert_eq!(stream.bytes_to_client, 5);

    let transport = running.shutdown().await.unwrap();
    assert!(transport.tcp_streams.is_empty());
    assert_eq!(transport.status.tcp.opened, 1);
    assert_eq!(transport.status.tcp.completed, 1);
    echo_task.abort();
}

#[tokio::test]
async fn best_effort_transport_events_never_block_the_engine() {
    let (upstream, echo_task) = start_echo_server().await;
    let (running, _events) = test_engine(test_routes(upstream.to_string()))
        .start_with_transport_events(1)
        .await
        .unwrap();
    let endpoint = running.endpoints().tcp[0];

    for byte in *b"abc" {
        let mut client = TcpStream::connect(endpoint).await.unwrap();
        client.write_all(&[byte]).await.unwrap();
        let mut echoed = [0; 1];
        client.read_exact(&mut echoed).await.unwrap();
        drop(client);
    }

    timeout(Duration::from_secs(1), async {
        while running.transport_status().tcp.completed < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("best-effort observation stalled connection completion");

    assert_eq!(running.transport_status().dropped_records, 2);
    timeout(Duration::from_secs(1), running.shutdown())
        .await
        .expect("best-effort observation stalled shutdown")
        .unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn resolves_an_upstream_hostname() {
    let (upstream, echo_task) = start_echo_server().await;
    let config = test_routes(format!("localhost:{}", upstream.port()));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn activates_latency_on_an_existing_connection() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    client.read_exact(&mut echoed).await.unwrap();

    running.set_faults("primary", vec![fixed_latency(150.0)]).await.unwrap();
    let started = Instant::now();
    client.write_all(b"b").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"b");
    assert!(started.elapsed() >= Duration::from_millis(130));

    let transport = running.shutdown().await.unwrap();
    let latency = &transport.tcp_streams[0].faults.latency;
    assert_eq!(latency.applications, 1);
    assert_eq!(latency.total_delay_ms, 150.0);
    echo_task.abort();
}

#[tokio::test]
async fn deactivates_blackhole_on_an_existing_connection() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    running
        .set_faults("primary", vec![blackhole(TrafficFlow::ToUpstream)])
        .await
        .unwrap();
    client.write_all(b"a").await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), client.read_exact(&mut echoed))
            .await
            .is_err()
    );

    running.set_faults("primary", Vec::new()).await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .expect("existing connection did not observe blackhole deactivation")
        .unwrap();
    assert_eq!(&echoed, b"a");

    let transport = running.shutdown().await.unwrap();
    assert_eq!(transport.tcp_streams[0].faults.blackhole_activations, 1);
    echo_task.abort();
}

#[tokio::test]
async fn resets_an_existing_connection_when_activated() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    client.read_exact(&mut echoed).await.unwrap();
    running
        .set_faults("primary", vec![connection_reset(TrafficFlow::Both, 1.0)])
        .await
        .unwrap();

    expect_connection_reset(&mut client).await;

    let transport = running.shutdown().await.unwrap();
    assert_eq!(transport.tcp_streams[0].faults.connection_resets, 1);
    echo_task.abort();
}

#[tokio::test]
async fn rejects_an_invalid_update_without_changing_active_faults() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    let result = running.set_faults("primary", vec![bandwidth(0)]).await;
    assert!(matches!(result, Err(EngineError::InvalidConfiguration(_))));

    client.write_all(b"a").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"a");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn chains_latency_faults() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults = vec![fixed_latency(40.0), fixed_latency(40.0)];
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    let started = Instant::now();
    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");
    assert!(started.elapsed() >= Duration::from_millis(75));

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn applies_faults_to_client_bound_traffic() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(latency(60.0, TrafficFlow::ToClient));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    let started = Instant::now();
    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");
    assert!(started.elapsed() >= Duration::from_millis(50));

    let transport = running.shutdown().await.unwrap();
    let latency = &transport.tcp_streams[0].faults.latency;
    assert_eq!(latency.applications, 1);
    assert_eq!(latency.total_delay_ms, 60.0);
    echo_task.abort();
}

#[tokio::test]
async fn latency_delays_only_the_first_bytes_of_a_tcp_flow() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(fixed_latency(250.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    let first_started = Instant::now();
    client.write_all(b"a").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert!(first_started.elapsed() >= Duration::from_millis(225));

    client.write_all(b"b").await.unwrap();
    timeout(Duration::from_millis(100), client.read_exact(&mut echoed))
        .await
        .expect("latency was applied more than once to the TCP flow")
        .unwrap();
    assert_eq!(&echoed, b"b");

    let transport = running.shutdown().await.unwrap();
    let latency = &transport.tcp_streams[0].faults.latency;
    assert_eq!(latency.applications, 1);
    assert_eq!(latency.total_delay_ms, 250.0);
    echo_task.abort();
}

#[tokio::test]
async fn blackholes_upstream_bound_traffic_until_shutdown() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(blackhole(TrafficFlow::ToUpstream));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), client.read_exact(&mut echoed))
            .await
            .is_err()
    );

    timeout(Duration::from_secs(1), running.shutdown())
        .await
        .expect("shutdown did not interrupt the blackhole")
        .unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn blackholes_client_bound_traffic_until_shutdown() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(blackhole(TrafficFlow::ToClient));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), client.read_exact(&mut echoed))
            .await
            .is_err()
    );

    timeout(Duration::from_secs(1), running.shutdown())
        .await
        .expect("shutdown did not interrupt the blackhole")
        .unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn resets_connections_on_upstream_bound_traffic() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0]
        .faults
        .push(connection_reset(TrafficFlow::ToUpstream, 1.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    client.write_all(b"a").await.unwrap();
    expect_connection_reset(&mut client).await;

    let transport = running.shutdown().await.unwrap();
    assert!(matches!(
        transport.tcp_streams[0].outcome,
        TcpStreamOutcome::FaultReset
    ));
    assert_eq!(transport.status.tcp.failed, 0);
    echo_task.abort();
}

#[tokio::test]
async fn resets_connections_on_client_bound_traffic() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(connection_reset(TrafficFlow::ToClient, 1.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    client.write_all(b"a").await.unwrap();
    expect_connection_reset(&mut client).await;

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn bypasses_connection_reset_when_probability_does_not_match() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(connection_reset(TrafficFlow::Both, 0.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"a");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn rejects_invalid_connection_reset_probability() {
    let mut config = test_routes("127.0.0.1:8080");
    config.proxies[0].faults.push(connection_reset(TrafficFlow::Both, 1.1));

    let result = test_engine(config).retain_transport_history().start().await;

    assert!(matches!(result, Err(EngineError::InvalidConfiguration(_))));
}

#[tokio::test]
async fn applies_jitter_when_probability_matches() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(jitter(60.0, 60.0, 1.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    let started = Instant::now();
    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");
    assert!(started.elapsed() >= Duration::from_millis(50));

    let transport = running.shutdown().await.unwrap();
    let jitter = &transport.tcp_streams[0].faults.jitter;
    assert!(jitter.applications >= 1);
    assert!(jitter.total_delay_ms >= 60.0);
    echo_task.abort();
}

#[tokio::test]
async fn bypasses_jitter_when_probability_does_not_match() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(jitter(10_000.0, 10_000.0, 0.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();

    client.write_all(b"fault").await.unwrap();
    let mut echoed = [0; 5];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"fault");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn rejects_invalid_jitter_configuration() {
    let mut config = test_routes("127.0.0.1:8080");
    config.proxies[0].faults.push(jitter(20.0, 10.0, 1.5));

    let result = test_engine(config).retain_transport_history().start().await;

    assert!(matches!(result, Err(EngineError::InvalidConfiguration(_))));
}

#[tokio::test]
async fn chains_different_fault_types() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults = vec![fixed_latency(40.0), bandwidth(4_096)];
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let payload = [42; 2_048];
    let mut echoed = [0; 2_048];

    let started = Instant::now();
    client.write_all(&payload).await.unwrap();
    timeout(Duration::from_secs(2), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(echoed, payload);
    // The bandwidth pacing clock runs while outer latency wrappers wait, so
    // independently introduced delays can overlap rather than simply add.
    assert!(started.elapsed() >= Duration::from_millis(240));

    let transport = running.shutdown().await.unwrap();
    let connection = &transport.tcp_streams[0];
    assert_eq!(connection.faults.latency.applications, 1);
    assert_eq!(connection.faults.bandwidth_bytes_limited, payload.len() as u64);
    echo_task.abort();
}

#[tokio::test]
async fn bandwidth_accounts_for_transferred_bytes_not_buffer_capacity() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(bandwidth(10));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut echoed = [0; 1];

    client.write_all(b"a").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    let started = Instant::now();
    client.write_all(b"b").await.unwrap();
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&echoed, b"b");
    assert!(started.elapsed() >= Duration::from_millis(75));

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn rejects_zero_bandwidth() {
    let mut config = test_routes("127.0.0.1:8080");
    config.proxies[0].faults.push(bandwidth(0));

    let result = test_engine(config).retain_transport_history().start().await;

    assert!(matches!(result, Err(EngineError::InvalidConfiguration(_))));
}

#[tokio::test]
async fn shutdown_interrupts_delayed_connections() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(fixed_latency(10_000.0));
    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    client.write_all(b"fault").await.unwrap();
    tokio::task::yield_now().await;

    timeout(Duration::from_secs(1), running.shutdown())
        .await
        .expect("shutdown waited for the latency timer")
        .unwrap();

    echo_task.abort();
}

#[tokio::test]
async fn starts_every_tcp_mapping() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies.push(TestProxy {
        name: "secondary".into(),
        protocol: TransportProtocol::Tcp,
        listen: "127.0.0.1:0".into(),
        upstream: upstream.to_string(),
        faults: Vec::new(),
    });

    let running =
        test_engine(config).retain_transport_history().start().await.unwrap();

    assert_eq!(running.endpoints().tcp.len(), 2);

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn faults_are_isolated_to_the_named_proxy() {
    let (upstream, echo_task) = start_echo_server().await;
    let mut config = test_routes(upstream.to_string());
    config.proxies[0].faults.push(blackhole(TrafficFlow::ToUpstream));
    config.proxies.push(TestProxy {
        name: "secondary".into(),
        protocol: TransportProtocol::Tcp,
        listen: "127.0.0.1:0".into(),
        upstream: upstream.to_string(),
        faults: Vec::new(),
    });
    let running = test_engine(config).start().await.unwrap();

    let mut impacted =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    impacted.write_all(b"primary").await.unwrap();
    let mut impacted_reply = [0; 7];
    assert!(
        timeout(
            Duration::from_millis(100),
            impacted.read_exact(&mut impacted_reply)
        )
        .await
        .is_err()
    );

    let mut healthy =
        TcpStream::connect(running.endpoints().tcp[1]).await.unwrap();
    healthy.write_all(b"secondary").await.unwrap();
    let mut healthy_reply = [0; 9];
    timeout(Duration::from_secs(1), healthy.read_exact(&mut healthy_reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&healthy_reply, b"secondary");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn rejects_an_invalid_listen_address() {
    let mut config = test_routes("127.0.0.1:8080");
    config.proxies[0].listen = "not-an-address".into();

    let result = test_engine(config).retain_transport_history().start().await;

    assert!(matches!(result, Err(EngineError::InvalidListenAddress { .. })));
}

#[tokio::test]
async fn rejects_an_invalid_remote_address() {
    let result = test_engine(test_routes("not-an-address")).start().await;

    assert!(matches!(result, Err(EngineError::InvalidUpstreamAddress { .. })));
}

#[tokio::test]
async fn closes_a_connection_when_the_upstream_is_unreachable() {
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable_address = unavailable.local_addr().unwrap();
    drop(unavailable);

    let running = test_engine(test_routes(unavailable_address.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let endpoint = running.endpoints().tcp[0];
    let mut client = TcpStream::connect(endpoint).await.unwrap();
    let mut byte = [0; 1];

    let read = timeout(Duration::from_secs(1), client.read(&mut byte))
        .await
        .expect("proxy did not close the failed connection");
    assert_eq!(read.unwrap(), 0);

    let transport = running.shutdown().await.unwrap();
    assert_eq!(transport.status.tcp.failed, 1);
    assert!(matches!(
        transport.tcp_streams[0].outcome,
        TcpStreamOutcome::UpstreamConnectFailed
    ));
    assert_eq!(
        transport.tcp_streams[0].failure.as_ref().unwrap().category,
        TransportFailureCategory::ConnectionRefused
    );
}

#[tokio::test]
async fn run_phases_reconfigure_an_existing_connection() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let mut progress = running.subscribe_run_progress();
    let run = local_run(vec![
        phase(
            "traffic disappears",
            "100ms",
            vec![blackhole(TrafficFlow::ToUpstream)],
        ),
        phase("traffic returns", "50ms", Vec::new()),
    ]);

    let run_execution = running.run_phases(run);
    let traffic = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        client.write_all(b"a").await.unwrap();
        let mut echoed = [0; 1];
        assert!(
            timeout(Duration::from_millis(40), client.read_exact(&mut echoed))
                .await
                .is_err()
        );
        let status = running.transport_status();
        assert_eq!(status.tcp.active, 1);
        assert_eq!(status.tcp.active_impacted, 1);
        timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
            .await
            .expect("connection did not observe the next run phase")
            .unwrap();
        assert_eq!(&echoed, b"a");
    };
    let transitions = async {
        let first = progress.recv().await.unwrap();
        let second = progress.recv().await.unwrap();
        vec![first, second]
    };

    let (result, (), transitions) =
        tokio::join!(run_execution, traffic, transitions);
    let result = result.unwrap();
    assert_eq!(transitions[0].phase_name, "traffic disappears");
    assert_eq!(transitions[0].phase_index, 1);
    assert_eq!(transitions[1].phase_name, "traffic returns");
    assert_eq!(transitions[1].phase_index, 2);
    assert!(matches!(result.outcome, RunOutcome::Success));
    assert_eq!(result.transport.tcp_streams.len(), 1);
    let connection = &result.transport.tcp_streams[0];
    assert_eq!(connection.bytes_to_upstream, 1);
    assert_eq!(connection.bytes_to_client, 1);
    assert_eq!(connection.faults.blackhole_activations, 1);
    let status = running.transport_status();
    assert_eq!(status.tcp.opened, 1);
    assert_eq!(status.tcp.impacted, 1);

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn cancelling_a_run_restores_the_previous_faults() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let mut client =
        TcpStream::connect(running.endpoints().tcp[0]).await.unwrap();
    let run = local_run(vec![phase(
        "traffic disappears",
        "10s",
        vec![blackhole(TrafficFlow::ToUpstream)],
    )]);

    assert!(
        timeout(Duration::from_millis(30), running.run_phases(run))
            .await
            .is_err()
    );

    client.write_all(b"a").await.unwrap();
    let mut echoed = [0; 1];
    timeout(Duration::from_secs(1), client.read_exact(&mut echoed))
        .await
        .expect("cancelled run did not restore the previous faults")
        .unwrap();
    assert_eq!(&echoed, b"a");

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn control_session_updates_atomically_and_restores_on_drop() {
    let (upstream, echo_task) = start_echo_server().await;
    let running =
        test_engine(test_routes(upstream.to_string())).start().await.unwrap();
    let control = running.begin_control().unwrap();

    let invalid = control.replace_faults(&[
        fault_model::ProxyFaults {
            proxy: "primary".into(),
            faults: vec![blackhole(TrafficFlow::ToUpstream)],
        },
        fault_model::ProxyFaults {
            proxy: "missing".into(),
            faults: Vec::new(),
        },
    ]);
    assert!(matches!(invalid, Err(EngineError::UnknownProxy(_))));
    assert!(control.active_faults()[0].faults.is_empty());

    control
        .set_faults("primary", vec![blackhole(TrafficFlow::ToUpstream)])
        .unwrap();
    assert_eq!(control.active_faults()[0].faults.len(), 1);
    assert!(matches!(
        running.set_faults("primary", Vec::new()).await,
        Err(EngineError::ControlAlreadyActive)
    ));

    drop(control);
    assert!(running.active_faults()[0].faults.is_empty());

    let phases = running.begin_schedule().unwrap();
    let latency = phases
        .add_phase(
            "latency".into(),
            Some("10s".parse().unwrap()),
            vec![fault_model::ProxyFaults {
                proxy: "primary".into(),
                faults: vec![fixed_latency(10.0)],
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Pending
    );
    phases.start_phase(latency.id).await.unwrap();
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Running
    );
    assert!(matches!(
        phases
            .modify_phase(
                latency.id,
                "changed".into(),
                Some("10s".parse().unwrap()),
                Vec::new(),
            )
            .await,
        Err(EngineError::PhaseImmutable { .. })
    ));
    let bandwidth = phases
        .add_phase("bandwidth".into(), Some("10s".parse().unwrap()), Vec::new())
        .await
        .unwrap();
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Pending
    );
    let transitions = phases.start_phase(bandwidth.id).await.unwrap();
    assert_eq!(transitions.len(), 2);
    assert_eq!(transitions[0].state, fault_engine::PhaseState::Stopped);
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Stopped
    );
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Running
    );
    phases.stop_phase(bandwidth.id).await.unwrap();
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.state,
        fault_engine::PhaseState::Stopped
    );
    assert!(
        timeout(Duration::from_millis(10), phases.next_transition())
            .await
            .is_err()
    );
    assert!(matches!(
        phases.delete_phase(bandwidth.id).await,
        Err(EngineError::PhaseImmutable { .. })
    ));

    let first = phases
        .add_phase("first".into(), Some("100ms".parse().unwrap()), Vec::new())
        .await
        .unwrap();
    let second = phases
        .add_phase("second".into(), Some("100ms".parse().unwrap()), Vec::new())
        .await
        .unwrap();
    let third = phases
        .add_phase("third".into(), Some("100ms".parse().unwrap()), Vec::new())
        .await
        .unwrap();
    for _ in 0..3 {
        assert_eq!(
            phases.next_transition().await.unwrap().unwrap().phase.state,
            fault_engine::PhaseState::Pending
        );
    }
    phases.move_phase(third.id, 0).await.unwrap();
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().kind,
        fault_engine::PhaseTransitionKind::Modified
    );
    let transitions = phases.start_phase(first.id).await.unwrap();
    assert_eq!(transitions[0].state, fault_engine::PhaseState::Running);
    assert!(
        phases.next_transition().await.unwrap().unwrap().phase.state
            == fault_engine::PhaseState::Running
    );
    assert!(
        phases.move_phase(third.id, 0).await.unwrap().planned_start_at
            < phases.move_phase(second.id, 1).await.unwrap().planned_start_at
    );
    for _ in 0..2 {
        assert_eq!(
            phases.next_transition().await.unwrap().unwrap().kind,
            fault_engine::PhaseTransitionKind::Modified
        );
    }

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        phases.next_transition().await.unwrap().unwrap().phase.name,
        "first"
    );
    let automatically_started =
        phases.next_transition().await.unwrap().unwrap();
    assert_eq!(automatically_started.phase.name, "third");
    assert_eq!(
        automatically_started.phase.state,
        fault_engine::PhaseState::Running
    );
    assert_eq!(
        automatically_started.reason,
        Some(fault_engine::PhaseTransitionReason::Automatic)
    );
    let transitions = phases.transitions();
    drop(phases);
    assert!(transitions.next().await.unwrap().is_none());

    running.shutdown().await.unwrap();
    echo_task.abort();
}

#[tokio::test]
async fn manual_fault_changes_are_rejected_while_a_run_executes() {
    let (upstream, echo_task) = start_echo_server().await;
    let running = test_engine(test_routes(upstream.to_string()))
        .retain_transport_history()
        .start()
        .await
        .unwrap();
    let run = local_run(vec![phase("steady", "100ms", Vec::new())]);

    let run_execution = running.run_phases(run);
    let manual_change = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        running.set_faults("primary", vec![fixed_latency(10.0)]).await
    };
    let (run_result, manual_result) =
        tokio::join!(run_execution, manual_change);

    assert!(run_result.is_ok());
    assert!(matches!(manual_result, Err(EngineError::ControlAlreadyActive)));

    running.shutdown().await.unwrap();
    echo_task.abort();
}
