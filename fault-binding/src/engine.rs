use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::time::Duration;

use fault_engine::FaultEngine;
use fault_engine::PhaseSchedule;
use fault_engine::PhaseTransitions;
use fault_engine::RunningEngine;
use fault_model::EngineEvent;
use fault_model::FaultSpec;
use fault_model::HumanDuration;
use fault_model::ProxyFaults;
use fault_model::Run;
use fault_model::RunProgress;
use fault_model::TransportRecord;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::Error;
use crate::Result;

/// Default bound of the completed transport record channel.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// Default wait before [`Engine::next_event`] reports a status event.
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(2);

/// A language-neutral handle over one fault engine.
///
/// Every value crossing the handle is JSON matching the published schemas,
/// so a binding only converts strings and maps [`crate::ErrorKind`] onto
/// its own error types. Clones share the same engine.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    run: Run,
    event_capacity: usize,
    state: SyncMutex<State>,
    progress: Mutex<Option<broadcast::Receiver<RunProgress>>>,
    records: Mutex<Option<mpsc::Receiver<TransportRecord>>>,
    schedule: Mutex<Option<PhaseSchedule>>,
    transitions: SyncMutex<Option<PhaseTransitions>>,
}

enum State {
    Configured(FaultEngine),
    Starting,
    Running { engine: Arc<RunningEngine>, endpoints: String },
    Stopped { summary: Option<String> },
}

impl Engine {
    /// Validate a run and prepare an engine for it without binding sockets.
    ///
    /// `event_capacity` bounds the completed transport record channel and
    /// defaults to [`DEFAULT_EVENT_CAPACITY`].
    pub fn new(config_json: &str, event_capacity: Option<i64>) -> Result<Self> {
        let event_capacity = match event_capacity {
            None => DEFAULT_EVENT_CAPACITY,
            Some(value) => usize::try_from(value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    Error::invalid_input("event_capacity must be at least 1")
                })?,
        };
        let run: Run = from_json(config_json)?;
        fault_model::validate_schema_version(&run)
            .map_err(|error| Error::invalid_input(error.to_string()))?;
        run.validate()
            .map_err(|error| Error::invalid_input(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                state: SyncMutex::new(State::Configured(
                    FaultEngine::from_run(&run),
                )),
                run,
                event_capacity,
                progress: Mutex::new(None),
                records: Mutex::new(None),
                schedule: Mutex::new(None),
                transitions: SyncMutex::new(None),
            }),
        })
    }

    /// Whether the engine is started and not yet shut down.
    pub fn alive(&self) -> bool {
        matches!(*self.inner.state(), State::Running { .. })
    }

    /// The bound endpoints as `BoundEndpoints` JSON, once started.
    pub fn endpoints(&self) -> Result<String> {
        match &*self.inner.state() {
            State::Running { endpoints, .. } => Ok(endpoints.clone()),
            State::Configured(_) | State::Starting => {
                Err(Error::runtime("the engine has not been started"))
            }
            State::Stopped { .. } => {
                Err(Error::runtime("the engine has been stopped"))
            }
        }
    }

    /// The final `TransportSummary` JSON, once shut down.
    pub fn summary(&self) -> Option<String> {
        match &*self.inner.state() {
            State::Stopped { summary } => summary.clone(),
            _ => None,
        }
    }

    /// Bind every configured proxy and return `BoundEndpoints` JSON.
    pub async fn start(&self) -> Result<String> {
        let configured = {
            let mut state = self.inner.state();
            match std::mem::replace(&mut *state, State::Starting) {
                State::Configured(engine) => engine,
                previous => {
                    let message = match previous {
                        State::Starting => "the engine is already starting",
                        State::Running { .. } => {
                            "the engine is already running"
                        }
                        _ => "the engine has been stopped",
                    };
                    *state = previous;
                    return Err(Error::runtime(message));
                }
            }
        };

        let started = configured
            .start_with_transport_events(self.inner.event_capacity)
            .await;
        let (running, records) = match started {
            Ok(started) => started,
            Err(error) => {
                *self.inner.state() = State::Stopped { summary: None };
                return Err(error.into());
            }
        };
        let endpoints = to_json(&running.endpoints())?;
        *self.inner.records.lock().await = Some(records);
        *self.inner.progress.lock().await =
            Some(running.subscribe_run_progress());
        *self.inner.state() = State::Running {
            engine: Arc::new(running),
            endpoints: endpoints.clone(),
        };
        Ok(endpoints)
    }

    /// Replace the active fault chain (`FaultSpec[]` JSON) of one proxy.
    pub async fn set_faults(
        &self,
        proxy: String,
        faults_json: String,
    ) -> Result<()> {
        let faults: Vec<FaultSpec> = from_json(&faults_json)?;
        let engine = self.inner.running()?;
        Ok(engine.set_faults(&proxy, faults).await?)
    }

    /// Execute the configured phases and return `RunResult` JSON.
    pub async fn run(&self) -> Result<String> {
        let engine = self.inner.running()?;
        to_json(&engine.run_phases(self.inner.run.clone()).await?)
    }

    /// Return the current `TransportStatus` JSON.
    pub async fn status(&self) -> Result<String> {
        to_json(&self.inner.running()?.transport_status())
    }

    /// Return the current `TransportSummary` JSON.
    pub async fn snapshot(&self) -> Result<String> {
        let engine = self.inner.running()?;
        to_json(&engine.transport_snapshot().await?)
    }

    /// Return the active `ProxyFaults[]` JSON.
    pub async fn active_faults(&self) -> Result<String> {
        to_json(&self.inner.running()?.active_faults())
    }

    /// Wait for the next `RunProgress` JSON; `None` once the engine stops.
    ///
    /// Progress is best effort: events missed by a slow reader are skipped.
    pub async fn next_progress(&self) -> Result<Option<String>> {
        let mut progress = self.inner.progress.lock().await;
        let receiver = progress.as_mut().ok_or_else(|| {
            Error::runtime("start the engine before reading progress")
        })?;
        loop {
            match receiver.recv().await {
                Ok(event) => return to_json(&event).map(Some),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return Ok(None),
            }
        }
    }

    /// Wait for the next `TransportRecord` JSON; `None` once the engine stops.
    pub async fn next_record(&self) -> Result<Option<String>> {
        self.inner
            .next_record()
            .await?
            .map(|record| to_json(&record))
            .transpose()
    }

    /// Wait for the next `EngineEvent` JSON; `None` once the engine stops.
    ///
    /// When no record completes within `status_interval_secs` (default
    /// [`DEFAULT_STATUS_INTERVAL`]), a status event is returned instead.
    pub async fn next_event(
        &self,
        status_interval_secs: Option<f64>,
    ) -> Result<Option<String>> {
        let interval = match status_interval_secs {
            None => DEFAULT_STATUS_INTERVAL,
            Some(seconds) => Duration::try_from_secs_f64(seconds)
                .ok()
                .filter(|interval| !interval.is_zero())
                .ok_or_else(|| {
                    Error::invalid_input(
                        "status_interval must be a finite number of seconds greater than zero",
                    )
                })?,
        };
        let event = match tokio::time::timeout(
            interval,
            self.inner.next_record(),
        )
        .await
        {
            Ok(record) => record?.map(EngineEvent::from),
            Err(_) => match &*self.inner.state() {
                State::Running { engine, .. } => Some(EngineEvent::Status {
                    status: engine.transport_status(),
                }),
                _ => None,
            },
        };
        event.map(|event| to_json(&event)).transpose()
    }

    /// Stop every proxy and return the final `TransportSummary` JSON.
    ///
    /// Any active phase schedule is closed first.
    pub async fn shutdown(&self) -> Result<String> {
        self.inner.close_schedule().await;
        let engine = {
            let mut state = self.inner.state();
            match &*state {
                State::Running { engine, .. }
                    if Arc::strong_count(engine) > 1 =>
                {
                    return Err(Error::runtime(
                        "wait for active engine operations before shutdown",
                    ));
                }
                State::Running { .. } => {}
                State::Configured(_) | State::Starting => {
                    return Err(Error::runtime(
                        "the engine has not been started",
                    ));
                }
                State::Stopped { .. } => {
                    return Err(Error::runtime(
                        "the engine is already stopped",
                    ));
                }
            }
            let State::Running { engine, .. } = std::mem::replace(
                &mut *state,
                State::Stopped { summary: None },
            ) else {
                unreachable!("state was checked");
            };
            Arc::into_inner(engine).expect("no other engine references")
        };
        let summary = to_json(&engine.shutdown().await?)?;
        *self.inner.state() = State::Stopped { summary: Some(summary.clone()) };
        Ok(summary)
    }

    /// Shut the engine down if it is running and return the final
    /// `TransportSummary` JSON; otherwise do nothing and return `None`.
    ///
    /// Suited to scope-exit cleanup, which must not fail because the engine
    /// never started or was already shut down explicitly.
    pub async fn close(&self) -> Result<Option<String>> {
        if self.alive() {
            self.shutdown().await.map(Some)
        } else {
            self.inner.close_schedule().await;
            Ok(None)
        }
    }

    /// Take transactional control of the engine's faults through a schedule.
    pub async fn begin_schedule(&self) -> Result<()> {
        let engine = self.inner.running()?;
        let mut slot = self.inner.schedule.lock().await;
        if slot.is_some() {
            return Err(Error::runtime("a phase schedule is already active"));
        }
        let schedule = engine.begin_schedule()?;
        *self.inner.transitions() = Some(schedule.transitions());
        *slot = Some(schedule);
        Ok(())
    }

    /// Whether a phase schedule is active.
    pub fn schedule_active(&self) -> bool {
        self.inner.transitions().is_some()
    }

    /// Close the active schedule, restoring the faults that were active when
    /// it began. Returns whether a schedule was active.
    pub async fn end_schedule(&self) -> bool {
        self.inner.close_schedule().await
    }

    /// Append a pending phase and return its `ControlledPhase` JSON.
    ///
    /// `duration` is a human-readable duration such as `"30s"`; omit it to
    /// run the phase until stopped. `faults_json` is `ProxyFaults[]` JSON.
    pub async fn add_phase(
        &self,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> Result<String> {
        let duration = parse_duration(duration)?;
        let faults: Vec<ProxyFaults> = from_json(&faults_json)?;
        let schedule = self.inner.schedule.lock().await;
        let phase =
            active(&schedule)?.add_phase(name, duration, faults).await?;
        to_json(&phase)
    }

    /// Replace a pending phase and return its `ControlledPhase` JSON.
    pub async fn modify_phase(
        &self,
        id: String,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> Result<String> {
        let id = parse_phase_id(&id)?;
        let duration = parse_duration(duration)?;
        let faults: Vec<ProxyFaults> = from_json(&faults_json)?;
        let schedule = self.inner.schedule.lock().await;
        let phase =
            active(&schedule)?.modify_phase(id, name, duration, faults).await?;
        to_json(&phase)
    }

    /// Delete a pending phase and return its `ControlledPhase` JSON.
    pub async fn delete_phase(&self, id: String) -> Result<String> {
        let id = parse_phase_id(&id)?;
        let schedule = self.inner.schedule.lock().await;
        to_json(&active(&schedule)?.delete_phase(id).await?)
    }

    /// Start a pending phase, superseding any running phase. Returns every
    /// changed phase as `ControlledPhase[]` JSON.
    pub async fn start_phase(&self, id: String) -> Result<String> {
        let id = parse_phase_id(&id)?;
        let schedule = self.inner.schedule.lock().await;
        to_json(&active(&schedule)?.start_phase(id).await?)
    }

    /// Stop a running phase and start the next pending one, if any. Returns
    /// every changed phase as `ControlledPhase[]` JSON.
    pub async fn stop_phase(&self, id: String) -> Result<String> {
        let id = parse_phase_id(&id)?;
        let schedule = self.inner.schedule.lock().await;
        to_json(&active(&schedule)?.stop_phase(id).await?)
    }

    /// Move a pending phase to a zero-based position among pending phases
    /// and return its `ControlledPhase` JSON. Positions past the end append.
    pub async fn move_phase(
        &self,
        id: String,
        position: i64,
    ) -> Result<String> {
        let id = parse_phase_id(&id)?;
        let position = usize::try_from(position).map_err(|_| {
            Error::invalid_input("phase position cannot be negative")
        })?;
        let schedule = self.inner.schedule.lock().await;
        to_json(&active(&schedule)?.move_phase(id, position).await?)
    }

    /// Wait for the next `PhaseTransition` JSON; `None` once the schedule
    /// closes.
    pub async fn next_transition(&self) -> Result<Option<String>> {
        let transitions =
            self.inner.transitions().clone().ok_or_else(no_active_schedule)?;
        transitions
            .next()
            .await?
            .map(|transition| to_json(&transition))
            .transpose()
    }
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn transitions(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<PhaseTransitions>> {
        self.transitions.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn running(&self) -> Result<Arc<RunningEngine>> {
        match &*self.state() {
            State::Running { engine, .. } => Ok(Arc::clone(engine)),
            State::Configured(_) | State::Starting => {
                Err(Error::runtime("the engine has not been started"))
            }
            State::Stopped { .. } => {
                Err(Error::runtime("the engine has been stopped"))
            }
        }
    }

    async fn next_record(&self) -> Result<Option<TransportRecord>> {
        let mut records = self.records.lock().await;
        let receiver = records.as_mut().ok_or_else(|| {
            Error::runtime("start the engine before reading transport records")
        })?;
        Ok(receiver.recv().await)
    }

    async fn close_schedule(&self) -> bool {
        let closed = self.schedule.lock().await.take().is_some();
        self.transitions().take();
        closed
    }
}

fn active(schedule: &Option<PhaseSchedule>) -> Result<&PhaseSchedule> {
    schedule.as_ref().ok_or_else(no_active_schedule)
}

fn no_active_schedule() -> Error {
    Error::runtime("no phase schedule is active")
}

fn parse_phase_id(id: &str) -> Result<uuid::Uuid> {
    id.parse()
        .map_err(|_| Error::invalid_input(format!("invalid phase id {id:?}")))
}

fn parse_duration(duration: Option<String>) -> Result<Option<HumanDuration>> {
    duration
        .map(|value| {
            value.parse().map_err(|error| {
                Error::invalid_input(format!(
                    "invalid phase duration {value:?}: {error}"
                ))
            })
        })
        .transpose()
}

fn from_json<T: DeserializeOwned>(input: &str) -> Result<T> {
    serde_json::from_str(input)
        .map_err(|error| Error::invalid_input(error.to_string()))
}

fn to_json(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| Error::runtime(error.to_string()))
}
