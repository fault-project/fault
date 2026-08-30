mod cli;
mod commands;
mod journal;
mod output;

use std::path::Path;
use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use fault_model::Run;

use crate::cli::Cli;
use crate::cli::Command;
use crate::commands::CommandStatus;
use crate::output::Output;
use crate::output::OutputEvent;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let output = Output::new(cli.output, cli.color);
    match dispatch(cli.command, &output).await {
        Ok(CommandStatus::Completed) => ExitCode::SUCCESS,
        Ok(CommandStatus::Interrupted) => ExitCode::from(130),
        Err(error) => {
            let event = OutputEvent::Error { message: format!("{error:#}") };
            if let Err(output_error) = output.emit(&event) {
                eprintln!("error: {error:#}");
                eprintln!(
                    "error: failed to render command output: {output_error}"
                );
            }
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(
    command: Command,
    output: &Output,
) -> anyhow::Result<CommandStatus> {
    match command {
        Command::Run(options) => {
            let run = load_run(&options.config).await?;
            commands::run_phases(run, options.journal.as_deref(), output).await
        }
        Command::Skill(options) => {
            match options.command {
                crate::cli::SkillCommand::Show => commands::show_skill()?,
                crate::cli::SkillCommand::Install { target, scope, force } => {
                    let (destination, scope, changed) =
                        commands::install_skill(target, scope, force)?;
                    output.emit(&OutputEvent::SkillInstalled {
                        target: target.as_str().into(),
                        scope: scope.as_str().into(),
                        destination: destination.display().to_string(),
                        changed,
                    })?;
                }
            }
            Ok(CommandStatus::Completed)
        }
    }
}

async fn load_run(path: &Path) -> anyhow::Result<Run> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    let value: serde_json::Value = match extension.as_deref() {
        Some("json") => serde_json::from_str(&contents).with_context(|| {
            format!("failed to parse JSON in {}", path.display())
        })?,
        Some("yaml" | "yml") => {
            yaml_serde::from_str(&contents).with_context(|| {
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
