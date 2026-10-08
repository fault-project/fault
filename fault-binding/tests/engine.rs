use std::time::Duration;

use fault_binding::Engine;
use fault_binding::ErrorKind;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::timeout;

async fn echo_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            tokio::spawn(async move {
                let mut buffer = [0; 1024];
                while let Ok(read) = stream.read(&mut buffer).await {
                    if read == 0
                        || stream.write_all(&buffer[..read]).await.is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    address.to_string()
}

fn run_json(upstream: &str) -> String {
    json!({
        "schema_version": 1,
        "name": "binding test",
        "proxies": [{
            "name": "api",
            "protocol": "tcp",
            "listen": "127.0.0.1:0",
            "upstream": upstream,
        }],
        "phases": [{ "name": "steady", "proxies": [] }],
    })
    .to_string()
}

fn parse(value: &str) -> Value {
    serde_json::from_str(value).unwrap()
}

async fn started() -> (Engine, String) {
    let engine = Engine::new(&run_json(&echo_upstream().await), None).unwrap();
    let endpoints = parse(&engine.start().await.unwrap());
    let proxy = endpoints["tcp"][0].as_str().unwrap().to_owned();
    (engine, proxy)
}

async fn round_trip(proxy: &str) {
    let mut stream = TcpStream::connect(proxy).await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut buffer = [0; 4];
    stream.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"ping");
}

#[test]
fn rejects_invalid_construction() {
    let error = Engine::new("{", None).err().unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidInput);

    let error = Engine::new(&run_json("127.0.0.1:1"), Some(0)).err().unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidInput);

    let mut run = parse(&run_json("127.0.0.1:1"));
    run["schema_version"] = json!(99);
    let error = Engine::new(&run.to_string(), None).err().unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
}

#[tokio::test]
async fn owns_the_engine_lifecycle() {
    let engine = Engine::new(&run_json(&echo_upstream().await), None).unwrap();
    assert!(!engine.alive());
    assert!(engine.endpoints().is_err());
    assert!(engine.status().await.is_err());

    let endpoints = engine.start().await.unwrap();
    assert!(engine.alive());
    assert_eq!(engine.endpoints().unwrap(), endpoints);
    assert_eq!(
        engine.start().await.unwrap_err().message,
        "the engine is already running"
    );

    let summary = engine.shutdown().await.unwrap();
    assert!(!engine.alive());
    assert_eq!(engine.summary(), Some(summary));
    assert_eq!(engine.next_event(Some(0.05)).await.unwrap(), None);
    assert_eq!(
        engine.shutdown().await.unwrap_err().message,
        "the engine is already stopped"
    );
    assert_eq!(engine.close().await.unwrap(), None);
}

#[tokio::test]
async fn close_is_safe_in_every_state() {
    let engine = Engine::new(&run_json(&echo_upstream().await), None).unwrap();
    assert_eq!(engine.close().await.unwrap(), None);
    engine.start().await.unwrap();
    let summary = engine.close().await.unwrap();
    assert!(summary.is_some());
    assert_eq!(engine.summary(), summary);
    assert_eq!(engine.close().await.unwrap(), None);
}

#[tokio::test]
async fn reports_records_and_periodic_status() {
    let (engine, proxy) = started().await;

    let idle = parse(&engine.next_event(Some(0.05)).await.unwrap().unwrap());
    assert_eq!(idle["type"], "status");

    round_trip(&proxy).await;
    let event = timeout(Duration::from_secs(5), async {
        loop {
            let event =
                parse(&engine.next_event(Some(0.05)).await.unwrap().unwrap());
            if event["type"] != "status" {
                return event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event["type"], "tcp-stream");
    assert_eq!(event["stream"]["proxy"], "api");

    for interval in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let error = engine.next_event(Some(interval)).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidInput);
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn validates_and_drives_a_schedule() {
    let (engine, _) = started().await;
    let latency = json!([{
        "proxy": "api",
        "faults": [{
            "type": "latency",
            "flow": "both",
            "distribution": { "type": "uniform", "min_ms": 1, "max_ms": 2 },
        }],
    }])
    .to_string();

    assert_eq!(
        engine
            .add_phase("early".into(), None, "[]".into())
            .await
            .unwrap_err()
            .message,
        "no phase schedule is active"
    );
    engine.begin_schedule().await.unwrap();
    assert!(engine.schedule_active());
    assert!(engine.begin_schedule().await.is_err());

    let error = engine
        .add_phase("bad".into(), Some("soon".into()), "[]".into())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);

    let first = parse(
        &engine.add_phase("first".into(), None, latency.clone()).await.unwrap(),
    );
    assert_eq!(first["state"], "pending");
    let id = first["id"].as_str().unwrap().to_owned();
    let second = parse(
        &engine
            .add_phase("second".into(), Some("50ms".into()), "[]".into())
            .await
            .unwrap(),
    );
    let second_id = second["id"].as_str().unwrap().to_owned();

    let error = engine.move_phase(id.clone(), -1).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    let error = engine.move_phase("nope".into(), 0).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    let error = engine.stop_phase(id.clone()).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::PhaseState);

    let moved = parse(&engine.move_phase(second_id, 0).await.unwrap());
    assert_eq!(moved["name"], "second");

    let added = parse(&engine.next_transition().await.unwrap().unwrap());
    assert_eq!(added["kind"], "added");
    assert_eq!(added["reason"], Value::Null);

    let started = parse(&engine.start_phase(id.clone()).await.unwrap());
    assert_eq!(started[0]["state"], "running");
    assert_eq!(
        parse(&engine.active_faults().await.unwrap())[0]["faults"][0]["type"],
        "latency"
    );

    let stopped = parse(&engine.stop_phase(id).await.unwrap());
    assert_eq!(stopped[0]["state"], "stopped");
    assert_eq!(stopped[1]["name"], "second");

    let reasons = timeout(Duration::from_secs(5), async {
        let mut reasons = Vec::new();
        while let Some(transition) = engine.next_transition().await.unwrap() {
            let transition = parse(&transition);
            reasons.push(transition["reason"].clone());
            if transition["reason"] == "duration-elapsed" {
                return reasons;
            }
        }
        reasons
    })
    .await
    .unwrap();
    assert!(reasons.contains(&json!("automatic")));
    assert_eq!(reasons.last(), Some(&json!("duration-elapsed")));

    assert!(engine.end_schedule().await);
    assert!(!engine.schedule_active());
    assert!(!engine.end_schedule().await);
    assert!(engine.next_transition().await.is_err());
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_closes_an_active_schedule() {
    let (engine, _) = started().await;
    engine.begin_schedule().await.unwrap();
    engine.shutdown().await.unwrap();
    assert!(!engine.schedule_active());
    assert!(!engine.end_schedule().await);
}
