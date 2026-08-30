use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use fault_model::JournalEvent;
use fault_model::Proxy;
use fault_model::ProxyFaults;
use fault_model::SCHEMA_VERSION;
use fault_model::TransportRecord;
use fault_model::TransportStatus;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::output::ProxyEndpoint;

pub(crate) struct Journal {
    task: JoinHandle<anyhow::Result<BufWriter<tokio::fs::File>>>,
}

impl Journal {
    pub(crate) async fn start(
        path: &Path,
        name: Option<String>,
        receiver: mpsc::Receiver<TransportRecord>,
        proxies: Vec<ProxyEndpoint>,
        faults: Vec<ProxyFaults>,
    ) -> anyhow::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .with_context(|| {
                format!(
                    "failed to create journal {}; choose a new path if it already exists",
                    path.display()
                )
            })?;
        let writer = BufWriter::new(file);
        let task = tokio::spawn(write_records(
            writer, name, receiver, proxies, faults,
        ));
        Ok(Self { task })
    }

    pub(crate) async fn finish(
        self,
        status: &TransportStatus,
    ) -> anyhow::Result<()> {
        let mut writer =
            self.task.await.context("journal writer task failed")??;
        write_event(
            &mut writer,
            &JournalEvent::RunCompleted {
                completed_at: chrono::Utc::now(),
                status: status.clone(),
            },
        )
        .await?;
        writer.flush().await.context("failed to flush journal")
    }

    /// Resolves only if the writer stops unexpectedly while a run is active.
    pub(crate) async fn wait_for_failure(&mut self) -> anyhow::Result<()> {
        match (&mut self.task).await {
            Ok(Ok(_)) => anyhow::bail!("journal writer stopped unexpectedly"),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(error).context("journal writer task failed"),
        }
    }
}

async fn write_records(
    mut writer: BufWriter<tokio::fs::File>,
    name: Option<String>,
    mut receiver: mpsc::Receiver<TransportRecord>,
    proxies: Vec<ProxyEndpoint>,
    faults: Vec<ProxyFaults>,
) -> anyhow::Result<BufWriter<tokio::fs::File>> {
    let proxies = proxies
        .into_iter()
        .map(|proxy| Proxy {
            name: proxy.name,
            protocol: proxy.protocol,
            listen: proxy.listen,
            upstream: proxy.upstream,
        })
        .collect();
    write_event(
        &mut writer,
        &JournalEvent::RunStarted {
            schema_version: SCHEMA_VERSION,
            started_at: chrono::Utc::now(),
            name,
            proxies,
            faults,
        },
    )
    .await?;
    let mut flush = tokio::time::interval(Duration::from_secs(1));
    flush.tick().await;
    loop {
        tokio::select! {
            record = receiver.recv() => {
                let Some(record) = record else { break };
                let event = match record {
                    TransportRecord::TcpStream { stream } => {
                        JournalEvent::TcpStreamCompleted { stream }
                    }
                    TransportRecord::UdpExchange { exchange } => {
                        JournalEvent::UdpExchangeCompleted { exchange }
                    }
                };
                write_event(&mut writer, &event).await?;
            }
            _ = flush.tick() => {
                writer.flush().await.context("failed to flush journal")?;
            }
        }
    }
    Ok(writer)
}

async fn write_event(
    writer: &mut (impl AsyncWrite + Unpin),
    event: &JournalEvent,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(event)
        .context("failed to serialize journal event")?;
    line.push(b'\n');
    writer.write_all(&line).await.context("failed to write journal event")
}
