use std::net::SocketAddr;
use std::time::Duration;

use fault_model::TcpStreamOutcome;
use fault_model::TransportFailure;
use fault_model::TransportFailureCategory;
use fault_model::TransportFailureStage;
use fault_model::TransportProtocol;
use tokio::io;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::EngineError;
use crate::RuntimeFailure;
use crate::faults::DynamicFaultStream;
use crate::faults::FaultRuntime;
use crate::faults::InjectionContext;
use crate::observation::ObservationRecorder;
use crate::observation::ObservedStream;

const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct RunningTcpProxy {
    address: SocketAddr,
    task: JoinHandle<Result<(), EngineError>>,
}

impl RunningTcpProxy {
    pub(crate) async fn bind(
        listen_address: SocketAddr,
        remote_address: String,
        name: String,
        cancellation: CancellationToken,
        fault_runtime: FaultRuntime,
        observations: ObservationRecorder,
        failures: tokio::sync::broadcast::Sender<RuntimeFailure>,
    ) -> Result<Self, EngineError> {
        let listener = TcpListener::bind(listen_address).await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let result = run(
                listener,
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
    listener: TcpListener,
    remote_address: String,
    name: String,
    cancellation: CancellationToken,
    fault_runtime: FaultRuntime,
    observations: ObservationRecorder,
) -> Result<(), EngineError> {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            Some(completed) = connections.join_next(), if !connections.is_empty() => {
                propagate_connection_task(completed)?;
            }
            accepted = listener.accept() => {
                let (client, peer) = accepted?;
                connections.spawn(forward(
                    client,
                    peer,
                    remote_address.clone(),
                    name.clone(),
                    cancellation.clone(),
                    fault_runtime.clone(),
                    observations.clone(),
                ));
            }
        }
    }

    while let Some(completed) = connections.join_next().await {
        propagate_connection_task(completed)?;
    }

    Ok(())
}

fn propagate_connection_task(
    completed: Result<Result<(), EngineError>, tokio::task::JoinError>,
) -> Result<(), EngineError> {
    completed.map_err(|error| EngineError::ProxyTask(error.to_string()))?
}

async fn forward(
    client: TcpStream,
    peer: SocketAddr,
    remote_address: String,
    proxy: String,
    cancellation: CancellationToken,
    fault_runtime: FaultRuntime,
    observations: ObservationRecorder,
) -> Result<(), EngineError> {
    let connection_id = uuid::Uuid::new_v4();
    let metrics = observations.metrics(TransportProtocol::Tcp);
    let guard = observations
        .open(
            connection_id,
            TransportProtocol::Tcp,
            proxy,
            peer.to_string(),
            remote_address.clone(),
            metrics.clone(),
        )
        .await?;
    let upstream = tokio::select! {
        _ = cancellation.cancelled() => {
            guard.finish_tcp(TcpStreamOutcome::Cancelled, None).await;
            return Ok(());
        }
        connected = tokio::time::timeout(
            UPSTREAM_CONNECT_TIMEOUT,
            TcpStream::connect(&remote_address),
        ) => match connected {
            Err(_) => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "connection attempt exceeded {}s",
                        UPSTREAM_CONNECT_TIMEOUT.as_secs()
                    ),
                );
                let failure = connection_failure(
                    TransportFailureStage::Connect,
                    &remote_address,
                    &error,
                );
                guard
                    .finish_tcp(
                        TcpStreamOutcome::UpstreamConnectFailed,
                        Some(failure),
                    )
                    .await;
                return Ok(());
            }
            Ok(Ok(upstream)) => upstream,
            Ok(Err(error)) => {
                let failure = connection_failure(
                    TransportFailureStage::Connect,
                    &remote_address,
                    &error,
                );
                guard
                    .finish_tcp(
                        TcpStreamOutcome::UpstreamConnectFailed,
                        Some(failure),
                    )
                    .await;
                return Ok(());
            }
        },
    };
    let context =
        InjectionContext { connection_id, peer, metrics: metrics.clone() };
    let observed = ObservedStream::new(Box::new(client), metrics.clone());
    let mut client =
        DynamicFaultStream::new(Box::new(observed), fault_runtime, context)?;
    let mut upstream = upstream;

    let (outcome, failure) = tokio::select! {
        _ = cancellation.cancelled() => (TcpStreamOutcome::Cancelled, None),
        copied = io::copy_bidirectional(&mut client, &mut upstream) => {
            match copied {
                Ok(_) => (TcpStreamOutcome::Completed, None),
                Err(_) if metrics.was_reset_injected() => {
                    (TcpStreamOutcome::FaultReset, None)
                }
                Err(error) => {
                    let failure = connection_failure(
                        TransportFailureStage::Transfer,
                        &remote_address,
                        &error,
                    );
                    (TcpStreamOutcome::TransferFailed, Some(failure))
                }
            }
        },
    };
    guard.finish_tcp(outcome, failure).await;
    Ok(())
}

fn connection_failure(
    stage: TransportFailureStage,
    upstream: &str,
    error: &std::io::Error,
) -> TransportFailure {
    let category = match error.kind() {
        std::io::ErrorKind::NotFound => TransportFailureCategory::DnsFailed,
        std::io::ErrorKind::ConnectionRefused => {
            TransportFailureCategory::ConnectionRefused
        }
        std::io::ErrorKind::TimedOut => TransportFailureCategory::TimedOut,
        std::io::ErrorKind::NetworkUnreachable
        | std::io::ErrorKind::HostUnreachable => {
            TransportFailureCategory::NetworkUnreachable
        }
        std::io::ErrorKind::ConnectionReset => {
            TransportFailureCategory::ConnectionReset
        }
        std::io::ErrorKind::BrokenPipe => TransportFailureCategory::BrokenPipe,
        _ => TransportFailureCategory::Other,
    };
    TransportFailure {
        stage,
        category,
        message: format!("{} {upstream}: {error}", stage_name(stage)),
    }
}

fn stage_name(stage: TransportFailureStage) -> &'static str {
    match stage {
        TransportFailureStage::Connect => "connect",
        TransportFailureStage::Transfer => "transfer",
        TransportFailureStage::Exchange => "exchange",
    }
}
