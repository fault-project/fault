use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use fault_engine::FaultEngine;
use fault_model::Run;
use fault_model::TransportProtocol;

use crate::commands::CommandStatus;
use crate::output::Output;
use crate::output::OutputEvent;
use crate::output::ProxyEndpoint;

pub(crate) async fn run_phases(
    run: Run,
    journal_path: Option<&std::path::Path>,
    output: &Output,
) -> anyhow::Result<CommandStatus> {
    let run_name = run.name.clone();
    let configured_proxies = run.proxies.clone();
    let engine_builder = FaultEngine::from_run(&run);
    let (engine, transport_events) = if journal_path.is_some() {
        let (engine, receiver) = engine_builder
            .start_with_transport_events(1_024)
            .await
            .context("failed to start fault engine")?;
        (engine, Some(receiver))
    } else {
        (
            engine_builder
                .start()
                .await
                .context("failed to start fault engine")?,
            None,
        )
    };
    let mut progress_receiver = engine.subscribe_run_progress();
    let mut runtime_failures = engine.subscribe_runtime_failures();
    let run_started_at = Instant::now();

    let endpoints = engine.endpoints();
    let mut tcp = endpoints.tcp.into_iter();
    let mut udp = endpoints.udp.into_iter();
    let proxies: Vec<_> = configured_proxies
        .into_iter()
        .map(|proxy| {
            let endpoint = match proxy.protocol {
                TransportProtocol::Tcp => tcp.next(),
                TransportProtocol::Udp => udp.next(),
            }
            .expect("every configured proxy has a bound endpoint");
            ProxyEndpoint {
                name: proxy.name,
                protocol: proxy.protocol,
                listen: format!("{}://{endpoint}", proxy.protocol.as_str()),
                upstream: proxy.upstream,
            }
        })
        .collect();
    let endpoints = proxies.iter().map(|proxy| proxy.listen.clone()).collect();
    let mut journal = if let Some(path) = journal_path {
        let receiver = transport_events
            .context("transport journal stream was not configured")?;
        Some(
            crate::journal::Journal::start(
                path,
                Some(run_name.clone()),
                receiver,
                proxies,
                engine.active_faults(),
            )
            .await?,
        )
    } else {
        None
    };
    output.emit(&OutputEvent::RunStarted { run_name, endpoints })?;

    let mut execution = Box::pin(engine.run_phases(run));
    let mut refresh = tokio::time::interval(Duration::from_secs(1));
    refresh.tick().await;
    let mut active_phase = None;
    let journal_enabled = journal.is_some();
    let execution_outcome = loop {
        tokio::select! {
            result = &mut execution => {
                break result.context("run execution failed").map(Some);
            }
            signal = tokio::signal::ctrl_c() => {
                break signal.context("failed to listen for Ctrl-C").map(|()| None);
            }
            progress = progress_receiver.recv() => {
                match progress {
                    Ok(progress) => {
                        active_phase = Some((progress, Instant::now()));
                        emit_progress(
                            output,
                            &engine,
                            active_phase.as_ref().unwrap(),
                            run_started_at,
                        )?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                }
            }
            _ = refresh.tick(), if output.wants_live_updates() && active_phase.is_some() => {
                emit_progress(
                    output,
                    &engine,
                    active_phase.as_ref().unwrap(),
                    run_started_at,
                )?;
            }
            failure = wait_for_journal_failure(&mut journal), if journal_enabled => {
                journal.take();
                break failure
                    .context("transport journal failed")
                    .map(|()| None);
            }
            failure = runtime_failures.recv() => {
                break runtime_failure(failure).map(|()| None);
            }
        }
    };
    drop(execution);

    let shutdown =
        engine.shutdown().await.context("failed to stop fault engine");
    let result = execution_outcome?;
    let transport = shutdown?;
    if let Some(journal) = journal {
        journal.finish(&transport.status).await?;
    }

    let Some(result) = result else {
        output.emit(&OutputEvent::Interrupted { operation: "run".into() })?;
        return Ok(CommandStatus::Interrupted);
    };
    output.emit(&OutputEvent::RunCompleted { result })?;
    Ok(CommandStatus::Completed)
}

fn runtime_failure(
    failure: Result<
        fault_engine::RuntimeFailure,
        tokio::sync::broadcast::error::RecvError,
    >,
) -> anyhow::Result<()> {
    match failure {
        Ok(failure) => anyhow::bail!(
            "proxy {} stopped unexpectedly: {}",
            failure.proxy,
            failure.message
        ),
        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
            anyhow::bail!("missed {count} unexpected proxy failures")
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
            anyhow::bail!("proxy failure monitor stopped unexpectedly")
        }
    }
}

async fn wait_for_journal_failure(
    journal: &mut Option<crate::journal::Journal>,
) -> anyhow::Result<()> {
    journal
        .as_mut()
        .context("transport journal was not configured")?
        .wait_for_failure()
        .await
}

fn emit_progress(
    output: &Output,
    engine: &fault_engine::RunningEngine,
    active_phase: &(fault_model::RunProgress, Instant),
    run_started_at: Instant,
) -> anyhow::Result<()> {
    let (progress, phase_received_at) = active_phase;
    let elapsed_ms = phase_received_at.elapsed().as_millis();
    let remaining_ms = progress.phase_duration_ms.map(|duration| {
        u128::from(duration)
            .saturating_sub(elapsed_ms)
            .min(u128::from(u64::MAX)) as u64
    });
    let event = OutputEvent::RunProgress {
        progress: progress.clone(),
        transport: engine.transport_status(),
        phase_remaining_ms: remaining_ms,
        running_for_seconds: run_started_at.elapsed().as_secs(),
    };
    output.update(&event)
}
