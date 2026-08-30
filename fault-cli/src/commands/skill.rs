use std::ffi::OsString;
use std::io::IsTerminal as _;
use std::io::Write as _;
use std::path::PathBuf;

use anyhow::Context;

use crate::cli::SkillScope;
use crate::cli::SkillTarget;

const SKILL_NAME: &str = "fault-network-injection";
const SKILL: &str =
    include_str!("../../skill/fault-network-injection/SKILL.md");

pub(crate) fn show_skill() -> anyhow::Result<()> {
    std::io::stdout()
        .lock()
        .write_all(SKILL.as_bytes())
        .context("failed to write the bundled skill")
}

pub(crate) fn install_skill(
    target: SkillTarget,
    scope: Option<SkillScope>,
    force: bool,
) -> anyhow::Result<(PathBuf, SkillScope, bool)> {
    let scope = scope.map_or_else(choose_scope, Ok)?;
    let destination = destination(target, scope)?;
    if destination.exists() {
        let installed =
            std::fs::read_to_string(&destination).with_context(|| {
                format!("failed to read {}", destination.display())
            })?;
        if installed == SKILL {
            return Ok((destination, scope, false));
        }
        if !force {
            anyhow::bail!(
                "{} already exists and differs from the bundled skill; use --force to replace it",
                destination.display()
            );
        }
    }

    let parent = destination
        .parent()
        .expect("a skill file always has a parent directory");
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    std::fs::write(&destination, SKILL).with_context(|| {
        format!("failed to write {}", destination.display())
    })?;
    Ok((destination, scope, true))
}

fn destination(
    target: SkillTarget,
    scope: SkillScope,
) -> anyhow::Result<PathBuf> {
    if matches!(scope, SkillScope::Workspace) {
        let workspace = std::env::current_dir()
            .context("could not determine the current workspace")?;
        let root = match target {
            SkillTarget::Codex => workspace.join(".agents").join("skills"),
            SkillTarget::Claude => workspace.join(".claude").join("skills"),
            SkillTarget::Opencode => workspace.join(".opencode").join("skills"),
        };
        return Ok(root.join(SKILL_NAME).join("SKILL.md"));
    }

    let home = home_dir()?;
    let root = match target {
        SkillTarget::Codex => std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"))
            .join("skills"),
        SkillTarget::Claude => home.join(".claude").join("skills"),
        SkillTarget::Opencode => {
            config_dir(&home).join("opencode").join("skills")
        }
    };
    Ok(root.join(SKILL_NAME).join("SKILL.md"))
}

fn choose_scope() -> anyhow::Result<SkillScope> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        anyhow::bail!(
            "installation scope is required without an interactive terminal; pass --scope workspace or --scope home"
        );
    }

    eprint!(
        "Install the fault skill for this workspace or your home directory? [workspace/home] "
    );
    std::io::stderr()
        .flush()
        .context("failed to display the installation prompt")?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .context("failed to read the installation scope")?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "w" | "workspace" => Ok(SkillScope::Workspace),
        "h" | "home" => Ok(SkillScope::Home),
        _ => anyhow::bail!(
            "choose workspace or home; alternatively pass --scope workspace or --scope home"
        ),
    }
}

fn home_dir() -> anyhow::Result<PathBuf> {
    env_path("HOME")
        .or_else(|| env_path("USERPROFILE"))
        .context("could not determine the user home directory")
}

fn config_dir(home: &std::path::Path) -> PathBuf {
    env_path("XDG_CONFIG_HOME")
        .or_else(|| env_path("APPDATA"))
        .unwrap_or_else(|| home.join(".config"))
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value: &OsString| !value.is_empty())
        .map(PathBuf::from)
}
