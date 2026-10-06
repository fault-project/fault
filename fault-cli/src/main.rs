mod cli;
mod commands;
mod config;
mod journal;
mod output;

use std::process::ExitCode;

use clap::Parser;

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
            let loaded = config::load_run(&options.config)
                .await
                .map_err(|failure| failure.error)?;
            commands::run_phases(
                &options.config,
                loaded,
                options.journal.as_deref(),
                options.watch,
                output,
            )
            .await
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
