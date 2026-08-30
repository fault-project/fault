use chrono::Utc;
use fault_model::Run;
use fault_model::RunOutcome;
use fault_model::RunProgress;
use fault_model::RunResult;
use fault_model::SCHEMA_VERSION;

use crate::ControlSession;
use crate::EngineError;
use crate::PhaseSchedule;
use crate::PhaseTransitionKind;
use crate::PhaseTransitionReason;
use crate::observation::ObservationRecorder;

pub(crate) async fn execute(
    control: ControlSession,
    observations: ObservationRecorder,
    progress: tokio::sync::broadcast::Sender<RunProgress>,
    run: Run,
) -> Result<RunResult, EngineError> {
    run.validate()?;
    let started_at = Utc::now();
    let run_name = run.name.clone();
    let phase_count = run.phases.len() as u32;
    let schedule = PhaseSchedule::new(control);
    let mut positions = std::collections::BTreeMap::new();
    let mut first_phase = None;

    for (phase_offset, phase) in run.phases.into_iter().enumerate() {
        let controlled = schedule
            .add_phase(phase.name, phase.duration, phase.proxies)
            .await?;
        positions.insert(controlled.id, phase_offset as u32 + 1);
        first_phase.get_or_insert(controlled.id);

        // Declarative runs do not expose planning edits. Consume the internal
        // added transition immediately so the bounded lifecycle stream cannot
        // accumulate with the number of phases.
        let _ = schedule.next_transition().await?;
    }

    schedule
        .start_phase(first_phase.expect("validated run has phases"))
        .await?;

    while let Some(transition) = schedule.next_transition().await? {
        if transition.kind == PhaseTransitionKind::Started {
            let phase = &transition.phase;
            let _ = progress.send(RunProgress {
                run_name: run_name.clone(),
                phase_name: phase.name.clone(),
                phase_index: positions[&phase.id],
                phase_count,
                phase_duration_ms: phase.duration.as_ref().map(|duration| {
                    duration.as_std().as_millis().min(u128::from(u64::MAX))
                        as u64
                }),
                phase_started_at: phase.started_at.unwrap_or_else(Utc::now),
                proxies: phase.faults.clone(),
            });
        }

        if transition.kind == PhaseTransitionKind::Stopped
            && transition.reason == Some(PhaseTransitionReason::DurationElapsed)
            && positions[&transition.phase.id] == phase_count
        {
            break;
        }
    }

    drop(schedule);
    let transport = observations.snapshot().await?;
    Ok(RunResult {
        schema_version: SCHEMA_VERSION,
        run_name: run.name,
        started_at,
        completed_at: Utc::now(),
        outcome: RunOutcome::Success,
        transport,
    })
}
