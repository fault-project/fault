use std::io::IsTerminal;
use std::io::Write as _;

use anyhow::Context;
use fault_model::DelayDistribution;
use fault_model::FaultSpec;
use fault_model::RunOutcome;
use fault_model::RunProgress;
use fault_model::RunResult;
use fault_model::TransportProtocol;
use fault_model::TransportStatus;

use crate::cli::ColorChoice;
use crate::cli::OutputFormat;

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ProxyEndpoint {
    pub(crate) name: String,
    pub(crate) protocol: TransportProtocol,
    pub(crate) listen: String,
    pub(crate) upstream: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub(crate) enum OutputEvent {
    RunStarted {
        run_name: String,
        endpoints: Vec<String>,
    },
    RunProgress {
        progress: RunProgress,
        transport: TransportStatus,
        phase_remaining_ms: Option<u64>,
        running_for_seconds: u64,
    },
    RunCompleted {
        result: RunResult,
    },
    Interrupted {
        operation: String,
    },
    SkillInstalled {
        target: String,
        scope: String,
        destination: String,
        changed: bool,
    },
    Error {
        message: String,
    },
}

pub(crate) struct Output {
    format: OutputFormat,
    interactive: bool,
    color: bool,
}

impl Output {
    pub(crate) fn new(format: OutputFormat, choice: ColorChoice) -> Self {
        let interactive = std::io::stdout().is_terminal();
        let color = matches!(format, OutputFormat::Text)
            && match choice {
                ColorChoice::Always => true,
                ColorChoice::Never => false,
                ColorChoice::Auto => {
                    interactive && std::env::var_os("NO_COLOR").is_none()
                }
            };
        Self { format, interactive, color }
    }

    pub(crate) fn wants_live_updates(&self) -> bool {
        matches!(self.format, OutputFormat::Json) || self.interactive
    }

    pub(crate) fn emit(&self, event: &OutputEvent) -> anyhow::Result<()> {
        self.write(event, false)
    }

    pub(crate) fn update(&self, event: &OutputEvent) -> anyhow::Result<()> {
        self.write(
            event,
            self.interactive && matches!(self.format, OutputFormat::Text),
        )
    }

    fn write(&self, event: &OutputEvent, redraw: bool) -> anyhow::Result<()> {
        let rendered = self.render(event)?;
        let mut stdout = std::io::stdout().lock();
        if redraw {
            write!(stdout, "\x1b[2J\x1b[H")
                .context("failed to redraw command output")?;
        }
        writeln!(stdout, "{rendered}")
            .context("failed to write command output")?;
        stdout.flush().context("failed to flush command output")
    }

    fn render(&self, event: &OutputEvent) -> anyhow::Result<String> {
        match self.format {
            OutputFormat::Text => Ok(self.render_text(event)),
            OutputFormat::Json => serde_json::to_string(event)
                .context("failed to serialize command output"),
        }
    }

    fn render_text(&self, event: &OutputEvent) -> String {
        match event {
            OutputEvent::RunStarted { run_name, endpoints } => {
                format!(
                    "Run \u{201c}{run_name}\u{201d} started with proxy at {}",
                    endpoints.join(", ")
                )
            }
            OutputEvent::RunProgress {
                progress,
                transport,
                phase_remaining_ms,
                running_for_seconds,
            } => self.render_run_progress(
                progress,
                transport,
                *phase_remaining_ms,
                *running_for_seconds,
            ),
            OutputEvent::RunCompleted { result } => match &result.outcome {
                RunOutcome::Success => format!(
                    "Run \u{201c}{}\u{201d} completed successfully.",
                    result.run_name
                ),
                RunOutcome::Failed { message } => format!(
                    "Run \u{201c}{}\u{201d} failed: {message}",
                    result.run_name
                ),
            },
            OutputEvent::Interrupted { operation } => {
                format!("{operation} interrupted.")
            }
            OutputEvent::SkillInstalled {
                target,
                scope,
                destination,
                changed,
            } => {
                let action =
                    if *changed { "Installed" } else { "Already installed" };
                format!("{action} {target} skill for {scope} at {destination}")
            }
            OutputEvent::Error { message } => {
                self.paint("31", &format!("Error: {message}"))
            }
        }
    }

    fn render_run_progress(
        &self,
        progress: &RunProgress,
        transport: &TransportStatus,
        phase_remaining_ms: Option<u64>,
        running_for_seconds: u64,
    ) -> String {
        let mut lines = vec![format!(
            "{}   {}   {}",
            self.paint("1;36", "fault run"),
            self.paint("1;32", "● RUNNING"),
            self.paint("2", &format_duration(running_for_seconds)),
        )];
        lines.extend([
            self.paint("1", &progress.run_name),
            String::new(),
            self.section("CURRENT PHASE"),
            format!(
                "  {} {}   {} of {}",
                self.paint("32", "●"),
                self.paint("1", &progress.phase_name),
                progress.phase_index,
                progress.phase_count,
            ),
            match (phase_remaining_ms, progress.phase_duration_ms) {
                (Some(remaining), Some(duration)) => format!(
                    "    {} remaining of {}",
                    self.paint("1;36", &format_milliseconds(remaining)),
                    format_milliseconds(duration),
                ),
                _ => format!("    {}", self.paint("1;36", "until stopped")),
            },
            String::new(),
            self.section("ACTIVE FAULTS"),
        ]);
        if progress.proxies.iter().all(|proxy| proxy.faults.is_empty()) {
            lines.push(format!(
                "  {} Healthy network conditions",
                self.paint("32", "○")
            ));
        } else {
            for proxy in &progress.proxies {
                for fault in &proxy.faults {
                    lines.push(format!(
                        "  {} {}  {}",
                        self.paint("33", "●"),
                        self.paint("1", &proxy.proxy),
                        self.paint("1;33", &format_fault(fault)),
                    ));
                }
            }
        }
        lines.extend([
            String::new(),
            self.section("TRANSPORT"),
            format!(
                "  TCP  {} active   {} impacted   {} opened",
                self.paint("1;36", &format_count(transport.tcp.active)),
                self.paint(
                    "1;33",
                    &format_count(transport.tcp.active_impacted)
                ),
                format_count(transport.tcp.opened),
            ),
            format!(
                "  UDP  {} active   {} impacted   {} started",
                self.paint("1;36", &format_count(transport.udp.active)),
                self.paint(
                    "1;33",
                    &format_count(transport.udp.active_impacted)
                ),
                format_count(transport.udp.started),
            ),
        ]);
        lines.join("\n")
    }

    fn section(&self, title: &str) -> String {
        self.paint("2", &format!("── {title} ──"))
    }

    fn paint(&self, code: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.into()
        }
    }
}

fn format_fault(fault: &FaultSpec) -> String {
    match fault {
        FaultSpec::Latency { flow, distribution } => {
            format!(
                "latency       {flow}   {}",
                format_distribution(distribution)
            )
        }
        FaultSpec::Jitter { flow, min_delay_ms, max_delay_ms, probability } => {
            format!(
                "jitter        {flow}   {min_delay_ms}\u{2013}{max_delay_ms}ms at {:.0}%",
                probability * 100.0
            )
        }
        FaultSpec::Bandwidth { flow, bytes_per_second } => format!(
            "bandwidth     {flow}   {}/s",
            format_bytes(*bytes_per_second)
        ),
        FaultSpec::Blackhole { flow } => format!("blackhole     {flow}"),
        FaultSpec::ConnectionReset { flow, probability } => {
            format!("connection-reset   {flow}   {:.0}%", probability * 100.0)
        }
        FaultSpec::Dns { case, delay_ms } => match delay_ms {
            Some(delay_ms) => format!("dns           {case:?}   {delay_ms}ms"),
            None => format!("dns           {case:?}"),
        },
    }
}

fn format_distribution(distribution: &DelayDistribution) -> String {
    match distribution {
        DelayDistribution::Uniform { min_ms, max_ms } => {
            format!("{min_ms}\u{2013}{max_ms}ms")
        }
        DelayDistribution::Normal { mean_ms, stddev_ms } => {
            format!("normal {mean_ms}ms \u{00b1} {stddev_ms}ms")
        }
        DelayDistribution::Pareto { shape, scale_ms } => {
            format!("pareto scale {scale_ms}ms, shape {shape}")
        }
        DelayDistribution::ParetoNormal { .. } => "pareto-normal".into(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{} B", bytes as u64)
    }
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = seconds % 3_600 / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn format_milliseconds(milliseconds: u64) -> String {
    format_duration(milliseconds.saturating_add(999) / 1_000)
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(character);
    }
    formatted
}

#[cfg(test)]
mod tests {
    use fault_model::RunProgress;
    use fault_model::TcpStreamStatus;
    use fault_model::TransportStatus;

    use super::Output;
    use super::OutputEvent;
    use crate::cli::ColorChoice;
    use crate::cli::OutputFormat;

    #[test]
    fn run_dashboard_shows_phase_progress() {
        let output = Output::new(OutputFormat::Text, ColorChoice::Never);
        let rendered = output
            .render(&OutputEvent::RunProgress {
                progress: RunProgress {
                    run_name: "database instability".into(),
                    phase_name: "slow connection".into(),
                    phase_index: 2,
                    phase_count: 3,
                    phase_duration_ms: Some(10_000),
                    phase_started_at: chrono::Utc::now(),
                    proxies: Vec::new(),
                },
                transport: TransportStatus {
                    tcp: TcpStreamStatus {
                        active: 4,
                        active_impacted: 3,
                        ..TcpStreamStatus::default()
                    },
                    ..TransportStatus::default()
                },
                phase_remaining_ms: Some(7_000),
                running_for_seconds: 18,
            })
            .unwrap();

        assert!(rendered.contains("fault run   ● RUNNING   00:18"));
        assert!(rendered.contains("slow connection   2 of 3"));
        assert!(rendered.contains("00:07 remaining of 00:10"));
        assert!(rendered.contains("TCP  4 active"));
    }
}
