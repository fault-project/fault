use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use fault_engine::FaultEngine;
use fault_engine::RunningEngine;
use fault_model::Proxy;
use fault_model::Run;
use fault_model::RunResult;
use fault_model::TransportProtocol;
use fault_model::TransportRecord;

use crate::commands::CommandStatus;
use crate::config::ConfigWatcher;
use crate::config::LoadedRun;
use crate::output::Output;
use crate::output::OutputEvent;
use crate::output::ProxyEndpoint;

/// How one execution of the phase timeline ended.
#[expect(
    clippy::large_enum_variant,
    reason = "a single short-lived value per run execution"
)]
enum Exit {
    /// The run completed (`Some`), was interrupted (`None`), or failed.
    Finished(anyhow::Result<Option<RunResult>>),
    /// The configuration changed and must replace the current run.
    Reload(LoadedRun),
}

pub(crate) async fn run_phases(
    config_path: &Path,
    loaded: LoadedRun,
    journal_path: Option<&Path>,
    watch: bool,
    output: &Output,
) -> anyhow::Result<CommandStatus> {
    let mut watcher =
        if watch { Some(ConfigWatcher::new(config_path)?) } else { None };
    let mut current = loaded;
    let (mut engine, transport_events) =
        start_engine(&current.run, journal_path.is_some()).await?;
    let proxies = proxy_endpoints(&engine, &current.run.proxies);
    let endpoints = proxies.iter().map(|proxy| proxy.listen.clone()).collect();
    let mut journal = if let Some(path) = journal_path {
        let receiver = transport_events
            .context("transport journal stream was not configured")?;
        Some(
            crate::journal::Journal::start(
                path,
                Some(current.run.name.clone()),
                receiver,
                proxies,
                engine.active_faults(),
            )
            .await?,
        )
    } else {
        None
    };
    output.emit(&OutputEvent::RunStarted {
        run_name: current.run.name.clone(),
        endpoints,
        config_sha256: current.sha256.clone(),
    })?;

    // Last configuration revision acted upon, accepted or rejected.
    let mut last_seen = current.sha256.clone();
    let execution_outcome = loop {
        // The engine is seeded with the first phase's faults. Clear them so
        // that the faults restored when a run stops, completes, or is
        // replaced by a reload are always "no faults".
        clear_faults(&engine).await?;

        let mut progress_receiver = engine.subscribe_run_progress();
        let mut runtime_failures = engine.subscribe_runtime_failures();
        let run_started_at = Instant::now();
        let mut execution = Box::pin(engine.run_phases(current.run.clone()));
        let mut refresh = tokio::time::interval(Duration::from_secs(1));
        refresh.tick().await;
        let mut active_phase = None;
        let journal_enabled = journal.is_some();
        let exit = loop {
            tokio::select! {
                result = &mut execution => {
                    break Exit::Finished(
                        result.context("run execution failed").map(Some),
                    );
                }
                signal = tokio::signal::ctrl_c() => {
                    break Exit::Finished(
                        signal
                            .context("failed to listen for Ctrl-C")
                            .map(|()| None),
                    );
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
                    break Exit::Finished(
                        failure
                            .context("transport journal failed")
                            .map(|()| None),
                    );
                }
                failure = runtime_failures.recv() => {
                    break Exit::Finished(runtime_failure(failure).map(|()| None));
                }
                change = wait_for_change(&mut watcher) => {
                    if let Err(error) = change {
                        break Exit::Finished(Err(error));
                    }
                    let loaded = crate::config::load_run(config_path).await;
                    let revision = revision_of(&loaded);
                    let previous =
                        std::mem::replace(&mut last_seen, revision.clone());
                    // The watcher wakes up for any activity in the file's
                    // directory. Act only on a revision not seen yet, and
                    // never reload the configuration that is already running.
                    if revision == previous || revision == current.sha256 {
                        continue;
                    }
                    match loaded {
                        Ok(loaded) => break Exit::Reload(loaded),
                        Err(failure) => {
                            output.emit(&OutputEvent::ConfigReloadFailed {
                                path: config_path.display().to_string(),
                                sha256: failure.sha256,
                                message: format!("{:#}", failure.error),
                            })?;
                        }
                    }
                }
            }
        };
        // Stopping the run atomically restores the faults that were active
        // before it started, which are none (see `clear_faults` above).
        drop(execution);

        let loaded = match exit {
            Exit::Finished(outcome) => break outcome,
            Exit::Reload(loaded) => loaded,
        };
        let proxies_restarted = loaded.run.proxies != current.run.proxies;
        if proxies_restarted {
            engine
                .shutdown()
                .await
                .context("failed to stop fault engine for reload")?;
            let (replacement, transport_events) =
                match start_engine(&loaded.run, journal.is_some()).await {
                    Ok(started) => started,
                    Err(error) => {
                        // The new proxies could not be bound. Bring the
                        // previous configuration back so the run continues.
                        output.emit(&OutputEvent::ConfigReloadFailed {
                        path: config_path.display().to_string(),
                        sha256: Some(loaded.sha256),
                        message: format!(
                            "{error:#}; restarted the previous configuration"
                        ),
                    })?;
                        let restored = start_engine(
                            &current.run,
                            journal.is_some(),
                        )
                        .await
                        .context(
                            "failed to restart the previous configuration",
                        )?;
                        engine = restored.0;
                        if let (Some(journal), Some(receiver)) =
                            (&journal, restored.1)
                        {
                            journal.follow(receiver)?;
                        }
                        continue;
                    }
                };
            engine = replacement;
            if let (Some(journal), Some(receiver)) =
                (&journal, transport_events)
            {
                journal.follow(receiver)?;
            }
        }
        current = loaded;
        output.emit(&OutputEvent::ConfigReloaded {
            path: config_path.display().to_string(),
            sha256: current.sha256.clone(),
            run_name: current.run.name.clone(),
            proxies_restarted,
            endpoints: proxy_endpoints(&engine, &current.run.proxies)
                .into_iter()
                .map(|proxy| proxy.listen)
                .collect(),
        })?;
    };

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

async fn start_engine(
    run: &Run,
    transport_events: bool,
) -> anyhow::Result<(
    RunningEngine,
    Option<tokio::sync::mpsc::Receiver<TransportRecord>>,
)> {
    let builder = FaultEngine::from_run(run);
    if transport_events {
        let (engine, receiver) = builder
            .start_with_transport_events(1_024)
            .await
            .context("failed to start fault engine")?;
        Ok((engine, Some(receiver)))
    } else {
        let engine =
            builder.start().await.context("failed to start fault engine")?;
        Ok((engine, None))
    }
}

async fn clear_faults(engine: &RunningEngine) -> anyhow::Result<()> {
    for proxy in engine.active_faults() {
        engine.set_faults(&proxy.proxy, Vec::new()).await.with_context(
            || format!("failed to clear faults on {}", proxy.proxy),
        )?;
    }
    Ok(())
}

fn proxy_endpoints(
    engine: &RunningEngine,
    configured: &[Proxy],
) -> Vec<ProxyEndpoint> {
    let endpoints = engine.endpoints();
    let mut tcp = endpoints.tcp.into_iter();
    let mut udp = endpoints.udp.into_iter();
    configured
        .iter()
        .map(|proxy| {
            let endpoint = match proxy.protocol {
                TransportProtocol::Tcp => tcp.next(),
                TransportProtocol::Udp => udp.next(),
            }
            .expect("every configured proxy has a bound endpoint");
            ProxyEndpoint {
                name: proxy.name.clone(),
                protocol: proxy.protocol,
                listen: format!("{}://{endpoint}", proxy.protocol.as_str()),
                upstream: proxy.upstream.clone(),
            }
        })
        .collect()
}

/// Identify a configuration revision: its digest when the file could be read,
/// otherwise the read error, so a persistently unreadable file is reported
/// once.
fn revision_of(loaded: &Result<LoadedRun, crate::config::LoadError>) -> String {
    match loaded {
        Ok(loaded) => loaded.sha256.clone(),
        Err(failure) => failure
            .sha256
            .clone()
            .unwrap_or_else(|| format!("unreadable: {:#}", failure.error)),
    }
}

async fn wait_for_change(
    watcher: &mut Option<ConfigWatcher>,
) -> anyhow::Result<()> {
    match watcher {
        Some(watcher) => watcher.changed().await,
        None => std::future::pending().await,
    }
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
