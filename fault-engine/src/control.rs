use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fault_model::FaultSpec;
use fault_model::HumanDuration;
use fault_model::ProxyFaults;
use tokio::sync::Mutex;
use tokio::sync::OwnedMutexGuard;

use crate::EngineError;
use crate::faults::ActiveFaults;
use crate::faults::FaultRuntime;

/// Exclusive, transactional control over an engine's active fault chains.
///
/// Dropping the session restores every chain that was active when the session
/// began. Updates are prepared in full before any proxy is changed.
pub struct ControlSession {
    runtimes: BTreeMap<String, FaultRuntime>,
    previous: Option<BTreeMap<String, Vec<FaultSpec>>>,
    _guard: OwnedMutexGuard<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhaseState {
    Pending,
    Running,
    Stopped,
    Deleted,
}

impl PhaseState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ControlledPhase {
    pub id: uuid::Uuid,
    pub name: String,
    pub faults: Vec<ProxyFaults>,
    pub duration: Option<HumanDuration>,
    pub state: PhaseState,
    pub planned_start_at: Option<chrono::DateTime<chrono::Utc>>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhaseTransitionKind {
    Added,
    Modified,
    Deleted,
    Started,
    Stopped,
}

impl PhaseTransitionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
            Self::Started => "started",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhaseTransitionReason {
    Explicit,
    Automatic,
    DurationElapsed,
    Superseded,
}

impl PhaseTransitionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Automatic => "automatic",
            Self::DurationElapsed => "duration-elapsed",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PhaseTransition {
    pub phase: ControlledPhase,
    pub kind: PhaseTransitionKind,
    pub reason: Option<PhaseTransitionReason>,
}

pub struct PhaseSchedule {
    state: Arc<Mutex<PhaseControlState>>,
    events: Arc<Mutex<tokio::sync::mpsc::Receiver<PhaseTransition>>>,
    dropped_events: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct PhaseTransitions {
    events: Arc<Mutex<tokio::sync::mpsc::Receiver<PhaseTransition>>>,
    dropped_events: Arc<AtomicU64>,
}

struct PhaseControlState {
    control: ControlSession,
    phases: BTreeMap<uuid::Uuid, ControlledPhase>,
    pending: VecDeque<uuid::Uuid>,
    active: Option<uuid::Uuid>,
    generation: u64,
    events: tokio::sync::mpsc::Sender<PhaseTransition>,
    dropped_events: Arc<AtomicU64>,
}

impl PhaseSchedule {
    pub(crate) fn new(control: ControlSession) -> Self {
        let (events, receiver) = tokio::sync::mpsc::channel(1_024);
        let dropped_events = Arc::new(AtomicU64::new(0));
        Self {
            state: Arc::new(Mutex::new(PhaseControlState {
                control,
                phases: BTreeMap::new(),
                pending: VecDeque::new(),
                active: None,
                generation: 0,
                events,
                dropped_events: Arc::clone(&dropped_events),
            })),
            events: Arc::new(Mutex::new(receiver)),
            dropped_events,
        }
    }

    pub async fn add_phase(
        &self,
        name: String,
        duration: Option<HumanDuration>,
        faults: Vec<ProxyFaults>,
    ) -> Result<ControlledPhase, EngineError> {
        let mut state = self.state.lock().await;
        // Preparing through a temporary replacement would mutate state, so
        // validate every chain independently before storing the plan.
        for entry in &faults {
            let runtime =
                state.control.runtimes.get(&entry.proxy).ok_or_else(|| {
                    EngineError::UnknownProxy(entry.proxy.clone())
                })?;
            runtime.prepare_specs(&entry.faults)?;
        }
        let phase = ControlledPhase {
            id: uuid::Uuid::new_v4(),
            name,
            faults,
            duration,
            state: PhaseState::Pending,
            planned_start_at: None,
            started_at: None,
        };
        state.phases.insert(phase.id, phase.clone());
        state.pending.push_back(phase.id);
        state.recompute_schedule();
        let phase = state.phases[&phase.id].clone();
        state.emit(PhaseTransition {
            phase: phase.clone(),
            kind: PhaseTransitionKind::Added,
            reason: None,
        });
        Ok(phase)
    }

    pub async fn modify_phase(
        &self,
        id: uuid::Uuid,
        name: String,
        duration: Option<HumanDuration>,
        faults: Vec<ProxyFaults>,
    ) -> Result<ControlledPhase, EngineError> {
        let mut state = self.state.lock().await;
        state.ensure_pending(id)?;
        for entry in &faults {
            let runtime =
                state.control.runtimes.get(&entry.proxy).ok_or_else(|| {
                    EngineError::UnknownProxy(entry.proxy.clone())
                })?;
            runtime.prepare_specs(&entry.faults)?;
        }
        let phase = state.phases.get_mut(&id).expect("phase was checked");
        phase.name = name;
        phase.duration = duration;
        phase.faults = faults;
        state.recompute_schedule();
        let phase = state.phases[&id].clone();
        state.emit(PhaseTransition {
            phase: phase.clone(),
            kind: PhaseTransitionKind::Modified,
            reason: None,
        });
        Ok(phase)
    }

    pub async fn delete_phase(
        &self,
        id: uuid::Uuid,
    ) -> Result<ControlledPhase, EngineError> {
        let mut state = self.state.lock().await;
        state.ensure_pending(id)?;
        {
            let phase = state.phases.get_mut(&id).expect("phase was checked");
            phase.state = PhaseState::Deleted;
            phase.planned_start_at = None;
        }
        state.pending.retain(|pending| *pending != id);
        state.recompute_schedule();
        let phase = state.phases[&id].clone();
        state.emit(PhaseTransition {
            phase: phase.clone(),
            kind: PhaseTransitionKind::Deleted,
            reason: None,
        });
        Ok(phase)
    }

    pub async fn start_phase(
        &self,
        id: uuid::Uuid,
    ) -> Result<Vec<ControlledPhase>, EngineError> {
        let (transitions, timer) = {
            let mut state = self.state.lock().await;
            state.ensure_pending(id)?;
            let transitions =
                state.start(id, PhaseTransitionReason::Explicit)?;
            let timer = state.phases[&id]
                .duration
                .as_ref()
                .map(|duration| (state.generation, duration.as_std()));
            (transitions, timer)
        };
        if let Some((generation, duration)) = timer {
            schedule(Arc::downgrade(&self.state), id, generation, duration);
        }
        Ok(transitions)
    }

    pub async fn stop_phase(
        &self,
        id: uuid::Uuid,
    ) -> Result<Vec<ControlledPhase>, EngineError> {
        let (transitions, next) = {
            let mut state = self.state.lock().await;
            let transitions =
                state.stop_and_advance(id, PhaseTransitionReason::Explicit)?;
            let next = state.active.and_then(|next| {
                state.phases[&next]
                    .duration
                    .as_ref()
                    .map(|duration| (next, state.generation, duration.as_std()))
            });
            (transitions, next)
        };
        if let Some((next, generation, duration)) = next {
            schedule(Arc::downgrade(&self.state), next, generation, duration);
        }
        Ok(transitions)
    }

    pub async fn move_phase(
        &self,
        id: uuid::Uuid,
        position: usize,
    ) -> Result<ControlledPhase, EngineError> {
        let mut state = self.state.lock().await;
        state.ensure_pending(id)?;
        state.pending.retain(|pending| *pending != id);
        let position = position.min(state.pending.len());
        state.pending.insert(position, id);
        state.recompute_schedule();
        let phase = state.phases.get(&id).expect("phase was checked").clone();
        state.emit(PhaseTransition {
            phase: phase.clone(),
            kind: PhaseTransitionKind::Modified,
            reason: None,
        });
        Ok(phase)
    }

    /// Wait for the next phase lifecycle transition.
    pub async fn next_transition(
        &self,
    ) -> Result<Option<PhaseTransition>, EngineError> {
        next_transition(&self.events, &self.dropped_events).await
    }

    /// Create a transition reader that does not retain schedule ownership.
    pub fn transitions(&self) -> PhaseTransitions {
        PhaseTransitions {
            events: Arc::clone(&self.events),
            dropped_events: Arc::clone(&self.dropped_events),
        }
    }
}

impl PhaseTransitions {
    pub async fn next(&self) -> Result<Option<PhaseTransition>, EngineError> {
        next_transition(&self.events, &self.dropped_events).await
    }
}

impl PhaseControlState {
    fn ensure_pending(&self, id: uuid::Uuid) -> Result<(), EngineError> {
        let phase =
            self.phases.get(&id).ok_or(EngineError::UnknownPhase(id))?;
        if phase.state != PhaseState::Pending {
            return Err(EngineError::PhaseImmutable {
                id,
                state: phase.state.as_str(),
            });
        }
        Ok(())
    }

    fn start(
        &mut self,
        id: uuid::Uuid,
        reason: PhaseTransitionReason,
    ) -> Result<Vec<ControlledPhase>, EngineError> {
        self.pending.retain(|pending| *pending != id);
        let mut transitions = Vec::new();
        if let Some(active) = self.active.take() {
            let phase = {
                let phase =
                    self.phases.get_mut(&active).expect("active phase exists");
                phase.state = PhaseState::Stopped;
                phase.clone()
            };
            transitions.push(phase.clone());
            self.emit(PhaseTransition {
                phase,
                kind: PhaseTransitionKind::Stopped,
                reason: Some(PhaseTransitionReason::Superseded),
            });
        }
        let faults = self.phases[&id].faults.clone();
        self.control.replace_faults(&faults)?;
        let phase = {
            let phase = self.phases.get_mut(&id).expect("phase was checked");
            phase.state = PhaseState::Running;
            phase.planned_start_at = None;
            phase.started_at = Some(chrono::Utc::now());
            phase.clone()
        };
        self.active = Some(id);
        self.generation = self.generation.wrapping_add(1);
        transitions.push(phase.clone());
        self.emit(PhaseTransition {
            phase,
            kind: PhaseTransitionKind::Started,
            reason: Some(reason),
        });
        self.recompute_schedule();
        Ok(transitions)
    }

    fn stop_and_advance(
        &mut self,
        id: uuid::Uuid,
        reason: PhaseTransitionReason,
    ) -> Result<Vec<ControlledPhase>, EngineError> {
        let phase =
            self.phases.get(&id).ok_or(EngineError::UnknownPhase(id))?;
        if phase.state != PhaseState::Running {
            return Err(EngineError::PhaseNotRunning(id));
        }
        let phase = self.phases.get_mut(&id).expect("phase was checked");
        phase.state = PhaseState::Stopped;
        self.active = None;
        self.generation = self.generation.wrapping_add(1);
        let phase = phase.clone();
        self.emit(PhaseTransition {
            phase: phase.clone(),
            kind: PhaseTransitionKind::Stopped,
            reason: Some(reason),
        });
        let mut transitions = vec![phase];
        if let Some(next) = self.pending.pop_front() {
            transitions
                .extend(self.start(next, PhaseTransitionReason::Automatic)?);
        } else {
            self.control.replace_faults(&[])?;
            self.recompute_schedule();
        }
        Ok(transitions)
    }

    fn recompute_schedule(&mut self) {
        let mut next = self.active.and_then(|id| {
            let phase = &self.phases[&id];
            phase.started_at.and_then(|started| {
                chrono::Duration::from_std(phase.duration.as_ref()?.as_std())
                    .ok()
                    .and_then(|duration| started.checked_add_signed(duration))
            })
        });
        for id in &self.pending {
            let phase = self.phases.get_mut(id).expect("pending phase exists");
            phase.planned_start_at = next;
            next = next.and_then(|start| {
                chrono::Duration::from_std(phase.duration.as_ref()?.as_std())
                    .ok()
                    .and_then(|duration| start.checked_add_signed(duration))
            });
        }
    }

    fn emit(&self, transition: PhaseTransition) {
        match self.events.try_send(transition) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

fn schedule(
    state: std::sync::Weak<Mutex<PhaseControlState>>,
    id: uuid::Uuid,
    generation: u64,
    duration: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let Some(state) = state.upgrade() else { return };
        let next = {
            let mut state = state.lock().await;
            if state.generation != generation || state.active != Some(id) {
                return;
            }
            if state
                .stop_and_advance(id, PhaseTransitionReason::DurationElapsed)
                .is_err()
            {
                return;
            }
            state.active.and_then(|next| {
                state.phases[&next]
                    .duration
                    .as_ref()
                    .map(|duration| (next, state.generation, duration.as_std()))
            })
        };
        if let Some((next, generation, duration)) = next {
            schedule(Arc::downgrade(&state), next, generation, duration);
        }
    });
}

async fn next_transition(
    events: &Mutex<tokio::sync::mpsc::Receiver<PhaseTransition>>,
    dropped_events: &AtomicU64,
) -> Result<Option<PhaseTransition>, EngineError> {
    let dropped = dropped_events.swap(0, Ordering::Relaxed);
    if dropped > 0 {
        return Err(EngineError::MissedPhaseTransitions(dropped));
    }
    Ok(events.lock().await.recv().await)
}

impl ControlSession {
    pub(crate) fn new(
        runtimes: BTreeMap<String, FaultRuntime>,
        guard: OwnedMutexGuard<()>,
    ) -> Self {
        let previous = Some(
            runtimes
                .iter()
                .map(|(name, runtime)| (name.clone(), runtime.active_specs()))
                .collect(),
        );
        Self { runtimes, previous, _guard: guard }
    }

    /// Replace one proxy's complete active fault chain.
    pub fn set_faults(
        &self,
        proxy: &str,
        faults: Vec<FaultSpec>,
    ) -> Result<(), EngineError> {
        let runtime = self
            .runtimes
            .get(proxy)
            .ok_or_else(|| EngineError::UnknownProxy(proxy.to_owned()))?;
        let prepared = runtime.prepare_specs(&faults)?;
        runtime.apply(prepared);
        Ok(())
    }

    /// Atomically replace all proxy chains; omitted proxies are cleared.
    pub fn replace_faults(
        &self,
        configured: &[ProxyFaults],
    ) -> Result<(), EngineError> {
        let mut requested = BTreeMap::new();
        for entry in configured {
            if !self.runtimes.contains_key(&entry.proxy) {
                return Err(EngineError::UnknownProxy(entry.proxy.clone()));
            }
            if requested.insert(entry.proxy.as_str(), &entry.faults).is_some() {
                return Err(EngineError::InvalidFaultConfig(format!(
                    "proxy {:?} is configured more than once",
                    entry.proxy
                )));
            }
        }

        let prepared = self
            .runtimes
            .iter()
            .map(|(name, runtime)| {
                let faults = requested
                    .get(name.as_str())
                    .map_or(&[][..], |faults| faults.as_slice());
                runtime
                    .prepare_specs(faults)
                    .map(|active| (runtime.clone(), active))
            })
            .collect::<Result<Vec<(FaultRuntime, Arc<ActiveFaults>)>, _>>()?;

        for (runtime, active) in prepared {
            runtime.apply(active);
        }
        Ok(())
    }

    pub fn active_faults(&self) -> Vec<ProxyFaults> {
        self.runtimes
            .iter()
            .map(|(proxy, runtime)| ProxyFaults {
                proxy: proxy.clone(),
                faults: runtime.active_specs(),
            })
            .collect()
    }

    /// Restore the pre-session chains now rather than on drop.
    pub fn restore(mut self) {
        self.restore_previous();
    }

    fn restore_previous(&mut self) {
        let Some(previous) = self.previous.take() else { return };
        for (name, faults) in previous {
            if let Some(runtime) = self.runtimes.get(&name) {
                if let Ok(prepared) = runtime.prepare_specs(&faults) {
                    runtime.apply(prepared);
                }
            }
        }
    }
}

impl Drop for ControlSession {
    fn drop(&mut self) {
        self.restore_previous();
    }
}
