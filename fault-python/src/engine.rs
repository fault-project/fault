use std::sync::Arc;

use fault_engine::FaultEngine;
use fault_engine::PhaseSchedule;
use fault_engine::PhaseTransitions;
use fault_engine::RunningEngine;
use fault_model::FaultSpec;
use fault_model::ProxyFaults;
use fault_model::Run;
use fault_model::RunProgress;
use fault_model::TransportRecord;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;

use crate::error::engine_error;
use crate::error::runtime_error;
use crate::json::from_json;
use crate::json::to_json;

#[pyclass]
pub(crate) struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    run: Run,
    progress: Mutex<Option<tokio::sync::broadcast::Receiver<RunProgress>>>,
    records: Mutex<Option<tokio::sync::mpsc::Receiver<TransportRecord>>>,
    schedule: Mutex<Option<PhaseSchedule>>,
    schedule_transitions: Mutex<Option<PhaseTransitions>>,
    event_capacity: usize,
}

enum State {
    Configured(Option<FaultEngine>),
    Running(Arc<RunningEngine>),
    Stopped,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (config_json, event_capacity=1024))]
    fn new(config_json: &str, event_capacity: usize) -> PyResult<Self> {
        let run: Run = from_json(config_json)?;
        fault_model::validate_schema_version(&run)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        run.validate()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Configured(Some(
                    FaultEngine::from_run(&run),
                ))),
                run,
                progress: Mutex::new(None),
                records: Mutex::new(None),
                schedule: Mutex::new(None),
                schedule_transitions: Mutex::new(None),
                event_capacity: event_capacity.max(1),
            }),
        })
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let configured = {
                let mut state = inner.state.lock().await;
                match &mut *state {
                    State::Configured(engine) => {
                        engine.take().ok_or_else(|| {
                            runtime_error("the engine is already starting")
                        })?
                    }
                    State::Running(_) => {
                        return Err(runtime_error(
                            "the engine is already running",
                        ));
                    }
                    State::Stopped => {
                        return Err(runtime_error(
                            "the engine has been stopped",
                        ));
                    }
                }
            };

            let (running, records) = configured
                .start_with_transport_events(inner.event_capacity)
                .await
                .map_err(engine_error)?;
            let progress = running.subscribe_run_progress();
            let endpoints = running.endpoints();
            let tcp = endpoints
                .tcp
                .into_iter()
                .map(|endpoint| endpoint.to_string())
                .collect::<Vec<_>>();
            let udp = endpoints
                .udp
                .into_iter()
                .map(|endpoint| endpoint.to_string())
                .collect::<Vec<_>>();

            *inner.records.lock().await = Some(records);
            *inner.progress.lock().await = Some(progress);
            *inner.state.lock().await = State::Running(Arc::new(running));

            to_json(&serde_json::json!({ "tcp": tcp, "udp": udp }))
        })
    }

    fn set_faults<'py>(
        &self,
        py: Python<'py>,
        proxy: String,
        faults_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let faults: Vec<FaultSpec> = from_json(faults_json)?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            engine.set_faults(&proxy, faults).await.map_err(engine_error)
        })
    }

    fn begin_schedule<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            let schedule = engine.begin_schedule().map_err(engine_error)?;
            *inner.schedule_transitions.lock().await =
                Some(schedule.transitions());
            *inner.schedule.lock().await = Some(schedule);
            Ok(())
        })
    }

    fn schedule_add_phase<'py>(
        &self,
        py: Python<'py>,
        name: String,
        duration: Option<String>,
        faults_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let faults: Vec<ProxyFaults> = from_json(faults_json)?;
        let duration = duration
            .map(|value| {
                value.parse().map_err(|error| {
                    runtime_error(format!("invalid phase duration: {error}"))
                })
            })
            .transpose()?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut schedule = inner.schedule.lock().await;
            let schedule = schedule
                .as_mut()
                .ok_or_else(|| runtime_error("no phase schedule is active"))?;
            let phase = schedule
                .add_phase(name, duration, faults)
                .await
                .map_err(engine_error)?;
            Ok(phase_json(&phase))
        })
    }

    fn schedule_modify_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
        name: String,
        duration: Option<String>,
        faults_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let faults: Vec<ProxyFaults> = from_json(faults_json)?;
        let duration = duration
            .map(|value| {
                value.parse().map_err(|error| {
                    runtime_error(format!("invalid phase duration: {error}"))
                })
            })
            .transpose()?;
        let id = parse_phase_id(&id)?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut schedule = inner.schedule.lock().await;
            let schedule = schedule
                .as_mut()
                .ok_or_else(|| runtime_error("no phase schedule is active"))?;
            let phase = schedule
                .modify_phase(id, name, duration, faults)
                .await
                .map_err(engine_error)?;
            Ok(phase_json(&phase))
        })
    }

    fn schedule_delete_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        schedule_phase_operation(py, Arc::clone(&self.inner), id, "delete")
    }

    fn schedule_start_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        schedule_phase_operation(py, Arc::clone(&self.inner), id, "start")
    }

    fn schedule_stop_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        schedule_phase_operation(py, Arc::clone(&self.inner), id, "stop")
    }

    fn schedule_move_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
        position: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let id = parse_phase_id(&id)?;
        future_into_py(py, async move {
            let mut guard = inner.schedule.lock().await;
            let schedule = guard
                .as_mut()
                .ok_or_else(|| runtime_error("no phase schedule is active"))?;
            let phase = schedule
                .move_phase(id, position)
                .await
                .map_err(engine_error)?;
            phases_json(&[phase])
        })
    }

    fn schedule_next_transition<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let transitions = inner
                .schedule_transitions
                .lock()
                .await
                .as_ref()
                .cloned()
                .ok_or_else(|| runtime_error("no phase schedule is active"))?;
            let transition = transitions.next().await.map_err(engine_error)?;
            Ok(transition.map(|transition| transition_json(&transition)))
        })
    }

    fn end_schedule<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .schedule
                .lock()
                .await
                .take()
                .ok_or_else(|| runtime_error("no phase schedule is active"))?;
            inner.schedule_transitions.lock().await.take();
            Ok(())
        })
    }

    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            let result = engine
                .run_phases(inner.run.clone())
                .await
                .map_err(engine_error)?;
            to_json(&result)
        })
    }

    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            to_json(&engine.transport_status())
        })
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            let summary =
                engine.transport_snapshot().await.map_err(engine_error)?;
            to_json(&summary)
        })
    }

    fn active_faults<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let engine = running(&inner).await?;
            to_json(&engine.active_faults())
        })
    }

    fn next_progress<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut progress = inner.progress.lock().await;
            let receiver = progress.as_mut().ok_or_else(|| {
                runtime_error("start the engine before reading progress")
            })?;
            loop {
                match receiver.recv().await {
                    Ok(event) => return to_json(&event).map(Some),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(
                        _,
                    )) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Ok(None);
                    }
                }
            }
        })
    }

    fn next_record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut records = inner.records.lock().await;
            let receiver = records.as_mut().ok_or_else(|| {
                runtime_error(
                    "start the engine before reading transport records",
                )
            })?;
            receiver.recv().await.map(|record| to_json(&record)).transpose()
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner.schedule.lock().await.take();
            inner.schedule_transitions.lock().await.take();
            let engine = {
                let mut state = inner.state.lock().await;
                match std::mem::replace(&mut *state, State::Stopped) {
                    State::Running(engine) => match Arc::try_unwrap(engine) {
                        Ok(engine) => engine,
                        Err(engine) => {
                            *state = State::Running(engine);
                            return Err(runtime_error(
                                "wait for active engine operations before shutdown",
                            ));
                        }
                    },
                    State::Configured(engine) => {
                        *state = State::Configured(engine);
                        return Err(runtime_error(
                            "the engine has not been started",
                        ));
                    }
                    State::Stopped => {
                        return Err(runtime_error(
                            "the engine is already stopped",
                        ));
                    }
                }
            };
            let summary = engine.shutdown().await.map_err(engine_error)?;
            to_json(&summary)
        })
    }
}

fn schedule_phase_operation<'py>(
    py: Python<'py>,
    inner: Arc<Inner>,
    id: String,
    operation: &'static str,
) -> PyResult<Bound<'py, PyAny>> {
    let id = parse_phase_id(&id)?;
    future_into_py(py, async move {
        let mut schedule = inner.schedule.lock().await;
        let schedule = schedule
            .as_mut()
            .ok_or_else(|| runtime_error("no phase schedule is active"))?;
        let phases = match operation {
            "delete" => {
                vec![schedule.delete_phase(id).await.map_err(engine_error)?]
            }
            "start" => schedule.start_phase(id).await.map_err(engine_error)?,
            "stop" => schedule.stop_phase(id).await.map_err(engine_error)?,
            _ => unreachable!("known phase operation"),
        };
        phases_json(&phases)
    })
}

fn phases_json(phases: &[fault_engine::ControlledPhase]) -> PyResult<String> {
    serde_json::to_string(&phases.iter().map(phase_value).collect::<Vec<_>>())
        .map_err(|error| runtime_error(error.to_string()))
}

fn parse_phase_id(id: &str) -> PyResult<uuid::Uuid> {
    id.parse().map_err(|_| runtime_error(format!("invalid phase id {id:?}")))
}

fn phase_json(phase: &fault_engine::ControlledPhase) -> String {
    phase_value(phase).to_string()
}

fn transition_json(transition: &fault_engine::PhaseTransition) -> String {
    serde_json::json!({
        "phase": phase_value(&transition.phase),
        "kind": transition.kind.as_str(),
        "reason": transition.reason.map(|reason| reason.as_str()),
    })
    .to_string()
}

fn phase_value(phase: &fault_engine::ControlledPhase) -> serde_json::Value {
    serde_json::json!({
        "id": phase.id.to_string(),
        "name": phase.name,
        "state": phase.state.as_str(),
        "faults": phase.faults,
        "duration": phase.duration.as_ref().map(ToString::to_string),
        "planned_start_at": phase.planned_start_at,
        "started_at": phase.started_at,
    })
}

async fn running(inner: &Inner) -> PyResult<Arc<RunningEngine>> {
    match &*inner.state.lock().await {
        State::Running(engine) => Ok(Arc::clone(engine)),
        State::Configured(_) => {
            Err(runtime_error("the engine has not been started"))
        }
        State::Stopped => Err(runtime_error("the engine has been stopped")),
    }
}
