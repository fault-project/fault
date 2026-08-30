use std::path::PathBuf;

use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;

#[derive(Debug, Parser)]
#[command(name = "fault", version, about)]
pub(crate) struct Cli {
    /// Choose human-readable text or newline-delimited JSON events.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,

    /// Control ANSI colours in text output.
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto)]
    pub(crate) color: ColorChoice,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum ColorChoice {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Run a network fault configuration.
    Run(RunOptions),
    /// Show or install the fault skill for an AI coding agent.
    Skill(SkillOptions),
}

#[derive(Debug, Args)]
pub(crate) struct SkillOptions {
    #[command(subcommand)]
    pub(crate) command: SkillCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SkillCommand {
    /// Print the bundled skill to standard output.
    Show,
    /// Install the bundled skill for one agent.
    Install {
        /// Agent whose skill directory should be used.
        #[arg(long, value_enum)]
        target: SkillTarget,

        /// Install for this workspace or for the current user.
        #[arg(long, value_enum)]
        scope: Option<SkillScope>,

        /// Replace an installed skill whose contents differ.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum SkillTarget {
    Codex,
    Claude,
    Opencode,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum SkillScope {
    Workspace,
    Home,
}

impl SkillScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Home => "home",
        }
    }
}

impl SkillTarget {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Opencode => "opencode",
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct RunOptions {
    /// Path to a JSON or YAML Run document.
    #[arg(value_name = "FILE")]
    pub(crate) config: PathBuf,

    /// Stream transport evidence as newline-delimited JSON.
    #[arg(long, value_name = "FILE")]
    pub(crate) journal: Option<PathBuf>,
}
