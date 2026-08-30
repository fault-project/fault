use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use fault_model::FaultSpec;
use fault_model::Phase;
use fault_model::Proxy;
use fault_model::ProxyFaults;
use fault_model::Run;
use fault_model::RunProgress;
use fault_model::RunResult;
use fault_model::SCHEMA_VERSION;
use fault_model::TransportProtocol;
use fault_model::TransportRecord;
use fault_model::TransportStatus;
use fault_model::TransportSummary;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::BoundEndpoints;
use crate::ControlSession;
use crate::EngineError;
use crate::PhaseSchedule;
use crate::RuntimeFailure;
use crate::faults::FaultRuntime;
use crate::observation::ObservationRecorder;
use crate::proxy::tcp::RunningTcpProxy;
use crate::proxy::udp::RunningUdpProxy;

pub struct FaultEngine {
    proxies: Vec<EngineProxy>,
    retain_transport_history: bool,
    transport_event_capacity: Option<usize>,
}

#[derive(Clone)]
struct EngineProxy {
    name: String,
    protocol: TransportProtocol,
    listen: String,
    upstream: String,
    faults: Vec<FaultSpec>,
}

impl FaultEngine {
    pub fn from_run(run: &Run) -> Self {
        let initial_faults: BTreeMap<_, _> = run
            .phases
            .first()
            .into_iter()
            .flat_map(|phase| &phase.proxies)
            .map(|entry| (entry.proxy.as_str(), entry.faults.clone()))
            .collect();
        Self {
            proxies: run
                .proxies
                .iter()
                .map(|proxy| EngineProxy {
                    name: proxy.name.clone(),
                    protocol: proxy.protocol,
                    listen: proxy.listen.clone(),
                    upstream: proxy.upstream.clone(),
                    faults: initial_faults
                        .get(proxy.name.as_str())
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect(),
            retain_transport_history: false,
            transport_event_capacity: None,
        }
    }

    /// Retain completed TCP-stream and UDP-exchange records until shutdown.
    ///
    /// Prefer `start_with_transport_events` for long-running engines.
    pub fn retain_transport_history(mut self) -> Self {
        self.retain_transport_history = true;
        self
    }

    fn with_transport_events(mut self, capacity: usize) -> Self {
        self.transport_event_capacity = Some(capacity.max(1));
        self
    }

    /// Start the engine and return its bounded completed-transport stream.
    ///
    /// Publishing is best-effort: when the consumer falls behind, records are
    /// dropped and counted in `TransportStatus::dropped_records`.
    pub async fn start_with_transport_events(
        self,
        capacity: usize,
    ) -> Result<
        (RunningEngine, tokio::sync::mpsc::Receiver<TransportRecord>),
        EngineError,
    > {
        let mut running = self.with_transport_events(capacity).start().await?;
        let receiver = running
            .take_transport_events()
            .expect("transport events were configured before engine start");
        Ok((running, receiver))
    }

    pub async fn start(self) -> Result<RunningEngine, EngineError> {
        validate_engine_proxies(&self.proxies)?;
        let cancellation = CancellationToken::new();
        let mut fault_runtimes = BTreeMap::new();
        let mut endpoints = BoundEndpoints::default();
        let mut tcp_proxies: Vec<RunningTcpProxy> = Vec::new();
        let mut udp_proxies: Vec<RunningUdpProxy> = Vec::new();
        let mut mappings = Vec::with_capacity(self.proxies.len());
        for mapping in &self.proxies {
            let runtime = FaultRuntime::new(&mapping.faults, mapping.protocol)?;
            fault_runtimes.insert(mapping.name.clone(), runtime.clone());
            mappings.push((
                mapping.name.clone(),
                mapping.protocol,
                parse_listen_address(&mapping.listen)?,
                resolve_upstream_address(&mapping.upstream).await?,
                runtime,
            ));
        }
        let (transport_event_sender, transport_events) = self
            .transport_event_capacity
            .map(tokio::sync::mpsc::channel)
            .map_or((None, None), |(sender, receiver)| {
                (Some(sender), Some(receiver))
            });
        let (observation_recorder, observation_task) =
            ObservationRecorder::start(
                self.retain_transport_history,
                transport_event_sender,
            );
        let (run_progress, _) = tokio::sync::broadcast::channel(32);
        let (runtime_failures, _) = tokio::sync::broadcast::channel(32);

        for (name, protocol, listen_address, remote_address, fault_runtime) in
            mappings
        {
            let bound = match protocol {
                TransportProtocol::Tcp => RunningTcpProxy::bind(
                    listen_address,
                    remote_address,
                    name,
                    cancellation.clone(),
                    fault_runtime,
                    observation_recorder.clone(),
                    runtime_failures.clone(),
                )
                .await
                .map(BoundProxy::Tcp),
                TransportProtocol::Udp => RunningUdpProxy::bind(
                    listen_address,
                    remote_address,
                    name,
                    cancellation.clone(),
                    fault_runtime,
                    observation_recorder.clone(),
                    runtime_failures.clone(),
                )
                .await
                .map(BoundProxy::Udp),
            };
            let bound = match bound {
                Ok(bound) => bound,
                Err(error) => {
                    cancellation.cancel();
                    for proxy in tcp_proxies {
                        let _ = proxy.join().await;
                    }
                    for proxy in udp_proxies {
                        let _ = proxy.join().await;
                    }
                    let _ = observation_recorder.stop().await;
                    let _ = observation_task.await;
                    return Err(error);
                }
            };
            match bound {
                BoundProxy::Tcp(proxy) => {
                    endpoints.tcp.push(proxy.address());
                    tcp_proxies.push(proxy);
                }
                BoundProxy::Udp(proxy) => {
                    endpoints.udp.push(proxy.address());
                    udp_proxies.push(proxy);
                }
            }
        }

        Ok(RunningEngine {
            endpoints,
            cancellation,
            fault_runtimes,
            observation_recorder,
            observation_task: Some(observation_task),
            transport_events,
            control_lock: Arc::new(Mutex::new(())),
            run_progress,
            runtime_failures,
            tcp_proxies,
            udp_proxies,
        })
    }
}

pub struct RunningEngine {
    endpoints: BoundEndpoints,
    cancellation: CancellationToken,
    fault_runtimes: BTreeMap<String, FaultRuntime>,
    observation_recorder: ObservationRecorder,
    observation_task: Option<tokio::task::JoinHandle<()>>,
    transport_events: Option<tokio::sync::mpsc::Receiver<TransportRecord>>,
    control_lock: Arc<Mutex<()>>,
    run_progress: tokio::sync::broadcast::Sender<RunProgress>,
    runtime_failures: tokio::sync::broadcast::Sender<RuntimeFailure>,
    tcp_proxies: Vec<RunningTcpProxy>,
    udp_proxies: Vec<RunningUdpProxy>,
}

impl RunningEngine {
    pub fn endpoints(&self) -> BoundEndpoints {
        self.endpoints.clone()
    }

    fn take_transport_events(
        &mut self,
    ) -> Option<tokio::sync::mpsc::Receiver<TransportRecord>> {
        self.transport_events.take()
    }

    pub async fn set_faults(
        &self,
        proxy: &str,
        faults: Vec<FaultSpec>,
    ) -> Result<(), EngineError> {
        let control = self
            .control_lock
            .try_lock()
            .map_err(|_| EngineError::ControlAlreadyActive)?;
        drop(control);
        let runtime = self
            .fault_runtimes
            .get(proxy)
            .ok_or_else(|| EngineError::UnknownProxy(proxy.to_owned()))?;
        runtime.set_faults(&faults)
    }

    /// Begin exclusive transactional control of the engine's fault chains.
    pub fn begin_control(&self) -> Result<ControlSession, EngineError> {
        let guard = self
            .control_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| EngineError::ControlAlreadyActive)?;
        Ok(ControlSession::new(self.fault_runtimes.clone(), guard))
    }

    pub fn begin_schedule(&self) -> Result<PhaseSchedule, EngineError> {
        self.begin_control().map(PhaseSchedule::new)
    }

    pub async fn run_phases(&self, run: Run) -> Result<RunResult, EngineError> {
        let control = self.begin_control()?;
        crate::run::execute(
            control,
            self.observation_recorder.clone(),
            self.run_progress.clone(),
            run,
        )
        .await
    }

    /// Subscribe to lightweight run phase transitions.
    pub fn subscribe_run_progress(
        &self,
    ) -> tokio::sync::broadcast::Receiver<RunProgress> {
        self.run_progress.subscribe()
    }

    /// Subscribe to unexpected proxy-task failures.
    pub fn subscribe_runtime_failures(
        &self,
    ) -> tokio::sync::broadcast::Receiver<RuntimeFailure> {
        self.runtime_failures.subscribe()
    }

    pub async fn transport_snapshot(
        &self,
    ) -> Result<TransportSummary, EngineError> {
        self.observation_recorder.snapshot().await
    }

    pub fn transport_status(&self) -> TransportStatus {
        self.observation_recorder.status()
    }

    pub fn active_faults(&self) -> Vec<ProxyFaults> {
        self.fault_runtimes
            .iter()
            .map(|(proxy, runtime)| ProxyFaults {
                proxy: proxy.clone(),
                faults: runtime.active_specs(),
            })
            .collect()
    }

    pub async fn shutdown(mut self) -> Result<TransportSummary, EngineError> {
        self.cancellation.cancel();

        let mut errors = Vec::new();
        for proxy in std::mem::take(&mut self.tcp_proxies) {
            if let Err(error) = proxy.join().await {
                errors.push(error);
            }
        }
        for proxy in std::mem::take(&mut self.udp_proxies) {
            if let Err(error) = proxy.join().await {
                errors.push(error);
            }
        }

        if !errors.is_empty() {
            return Err(EngineError::ShutdownErrors(errors));
        }
        let summary = self.observation_recorder.stop().await?;
        if let Some(task) = self.observation_task.take() {
            task.await
                .map_err(|error| EngineError::ProxyTask(error.to_string()))?;
        }
        Ok(summary)
    }
}

enum BoundProxy {
    Tcp(RunningTcpProxy),
    Udp(RunningUdpProxy),
}

impl Drop for RunningEngine {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn parse_listen_address(input: &str) -> Result<SocketAddr, EngineError> {
    input.parse().map_err(|source| EngineError::InvalidListenAddress {
        input: input.to_owned(),
        source,
    })
}

fn validate_engine_proxies(proxies: &[EngineProxy]) -> Result<(), EngineError> {
    Run {
        schema_version: SCHEMA_VERSION,
        name: "engine".into(),
        proxies: proxies
            .iter()
            .map(|proxy| Proxy {
                name: proxy.name.clone(),
                protocol: proxy.protocol,
                listen: proxy.listen.clone(),
                upstream: proxy.upstream.clone(),
            })
            .collect(),
        phases: vec![Phase {
            name: "initial".into(),
            duration: None,
            proxies: proxies
                .iter()
                .map(|proxy| ProxyFaults {
                    proxy: proxy.name.clone(),
                    faults: proxy.faults.clone(),
                })
                .collect(),
        }],
    }
    .validate()
    .map_err(Into::into)
}

async fn resolve_upstream_address(input: &str) -> Result<String, EngineError> {
    let mut addresses =
        tokio::net::lookup_host(input).await.map_err(|source| {
            EngineError::InvalidUpstreamAddress {
                input: input.to_owned(),
                source,
            }
        })?;
    if addresses.next().is_none() {
        return Err(EngineError::InvalidUpstreamAddress {
            input: input.to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "the host resolved to no addresses",
            ),
        });
    }
    Ok(input.to_owned())
}
