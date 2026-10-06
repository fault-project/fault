use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use fault_model::Run;
use notify::EventKind;
use notify::RecursiveMode;
use notify::Watcher as _;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Quiet period that coalesces the burst of events editors emit on save.
const DEBOUNCE: Duration = Duration::from_millis(250);

/// A validated run together with the digest of the exact bytes it came from.
pub(crate) struct LoadedRun {
    pub(crate) run: Run,
    pub(crate) sha256: String,
}

/// A configuration that could not be loaded. The digest is present when the
/// file could be read, so a rejected revision can still be identified.
pub(crate) struct LoadError {
    pub(crate) sha256: Option<String>,
    pub(crate) error: anyhow::Error,
}

pub(crate) async fn load_run(path: &Path) -> Result<LoadedRun, LoadError> {
    let contents = tokio::fs::read(path).await.map_err(|error| LoadError {
        sha256: None,
        error: anyhow::Error::new(error)
            .context(format!("failed to read {}", path.display())),
    })?;
    let sha256 = sha256_hex(&contents);
    match parse_run(path, &contents) {
        Ok(run) => Ok(LoadedRun { run, sha256 }),
        Err(error) => Err(LoadError { sha256: Some(sha256), error }),
    }
}

fn parse_run(path: &Path, contents: &[u8]) -> anyhow::Result<Run> {
    let contents = std::str::from_utf8(contents)
        .with_context(|| format!("{} is not valid UTF-8", path.display()))?;
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let value: serde_json::Value = match extension.as_deref() {
        Some("json") => serde_json::from_str(contents).with_context(|| {
            format!("failed to parse JSON in {}", path.display())
        })?,
        Some("yaml" | "yml") => {
            yaml_serde::from_str(contents).with_context(|| {
                format!("failed to parse YAML in {}", path.display())
            })?
        }
        _ => anyhow::bail!(
            "unsupported configuration format for {}; use a .json, .yaml, or .yml file",
            path.display()
        ),
    };

    let run: Run = serde_json::from_value(value).with_context(|| {
        format!("invalid run configuration in {}", path.display())
    })?;
    fault_model::validate_schema_version(&run)?;
    run.validate()?;
    Ok(run)
}

fn sha256_hex(contents: &[u8]) -> String {
    Sha256::digest(contents).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Watches one configuration file for changes.
///
/// The parent directory is watched rather than the file itself, and any
/// change in it counts as a possible change of the file. This covers:
///
/// - editors that save by renaming a new file over the old one;
/// - Kubernetes ConfigMap and Secret volumes, where `FILE` is a symlink
///   through `..data` and an update atomically swaps the `..data` symlink
///   without touching `FILE` itself.
///
/// Spurious wake-ups are cheap: callers compare content digests and ignore
/// revisions that did not change. Access events are ignored so reading the
/// file does not wake the watcher.
pub(crate) struct ConfigWatcher {
    _watcher: notify::RecommendedWatcher,
    events: mpsc::UnboundedReceiver<notify::Result<()>>,
    deadline: Option<Instant>,
}

impl ConfigWatcher {
    pub(crate) fn new(path: &Path) -> anyhow::Result<Self> {
        let path = std::path::absolute(path)
            .with_context(|| format!("failed to resolve {}", path.display()))?;
        let directory = path
            .parent()
            .with_context(|| format!("{} has no parent", path.display()))?
            .to_path_buf();
        let (sender, events) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(
            move |event: notify::Result<notify::Event>| {
                let notification = match event {
                    Ok(event) => (!matches!(event.kind, EventKind::Access(_)))
                        .then_some(Ok(())),
                    Err(error) => Some(Err(error)),
                };
                if let Some(notification) = notification {
                    let _ = sender.send(notification);
                }
            },
        )
        .context("failed to create configuration watcher")?;
        watcher.watch(&directory, RecursiveMode::NonRecursive).with_context(
            || format!("failed to watch {}", directory.display()),
        )?;
        Ok(Self { _watcher: watcher, events, deadline: None })
    }

    /// Resolve once the file has changed and then stayed quiet for the
    /// debounce period.
    ///
    /// Cancel-safe: progress is kept in `self`, so it may be raced in
    /// `tokio::select!` without losing changes.
    pub(crate) async fn changed(&mut self) -> anyhow::Result<()> {
        loop {
            let event = match self.deadline {
                None => self.events.recv().await,
                Some(deadline) => tokio::select! {
                    () = tokio::time::sleep_until(deadline) => {
                        self.deadline = None;
                        return Ok(());
                    }
                    event = self.events.recv() => event,
                },
            };
            event
                .context("configuration watcher stopped unexpectedly")?
                .context("configuration watcher failed")?;
            self.deadline = Some(Instant::now() + DEBOUNCE);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ConfigWatcher;
    use super::sha256_hex;

    #[tokio::test]
    async fn watcher_follows_atomic_saves_but_not_reads() {
        let directory = scratch_directory("save");
        let config = directory.join("run.yaml");
        std::fs::write(&config, "a").unwrap();
        let mut watcher = ConfigWatcher::new(&config).unwrap();

        std::fs::read(&config).unwrap();
        let read =
            tokio::time::timeout(Duration::from_millis(600), watcher.changed())
                .await;
        assert!(read.is_err(), "reading the file must not wake the watcher");

        let staged = directory.join("run.yaml.tmp");
        std::fs::write(&staged, "b").unwrap();
        std::fs::rename(&staged, &config).unwrap();
        tokio::time::timeout(Duration::from_secs(5), watcher.changed())
            .await
            .expect("an atomic save must trigger a reload")
            .unwrap();

        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Reproduces the kubelet's atomic writer for ConfigMap volumes:
    /// `run.yaml -> ..data/run.yaml` and `..data -> ..<timestamp>`, updated
    /// by renaming a fresh `..data_tmp` symlink over `..data`.
    #[cfg(unix)]
    #[tokio::test]
    async fn watcher_follows_kubernetes_configmap_updates() {
        use std::os::unix::fs::symlink;

        let volume = scratch_directory("configmap");
        std::fs::create_dir(volume.join("..2026_01_01_00_00_00.1")).unwrap();
        std::fs::write(volume.join("..2026_01_01_00_00_00.1/run.yaml"), "a")
            .unwrap();
        symlink("..2026_01_01_00_00_00.1", volume.join("..data")).unwrap();
        symlink("..data/run.yaml", volume.join("run.yaml")).unwrap();
        let config = volume.join("run.yaml");
        let mut watcher = ConfigWatcher::new(&config).unwrap();

        std::fs::create_dir(volume.join("..2026_01_01_00_01_00.2")).unwrap();
        std::fs::write(volume.join("..2026_01_01_00_01_00.2/run.yaml"), "b")
            .unwrap();
        symlink("..2026_01_01_00_01_00.2", volume.join("..data_tmp")).unwrap();
        std::fs::rename(volume.join("..data_tmp"), volume.join("..data"))
            .unwrap();
        std::fs::remove_dir_all(volume.join("..2026_01_01_00_00_00.1"))
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), watcher.changed())
            .await
            .expect("a ConfigMap update must trigger a reload")
            .unwrap();
        assert_eq!(std::fs::read(&config).unwrap(), b"b");

        std::fs::remove_dir_all(&volume).unwrap();
    }

    fn scratch_directory(label: &str) -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "fault-watch-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn digests_are_lowercase_hex_sha256() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
