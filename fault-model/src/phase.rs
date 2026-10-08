use std::net::SocketAddr;

use crate::HumanDuration;
use crate::ProxyFaults;
use crate::TcpStreamRecord;
use crate::TransportRecord;
use crate::TransportStatus;
use crate::UdpExchangeRecord;

/// Lifecycle state of an adaptively scheduled phase.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseState {
    /// Waiting in the schedule; the only state in which a phase can change.
    Pending,
    /// Applying its faults to the engine.
    Running,
    /// Finished, either explicitly or because its duration elapsed.
    Stopped,
    /// Removed from the schedule before it started.
    Deleted,
}

impl PhaseState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Deleted => "deleted",
        }
    }
}

/// One phase of an adaptive schedule, as observed at a point in time.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct ControlledPhase {
    /// Identifier assigned by the engine when the phase is added.
    pub id: uuid::Uuid,
    /// Human-readable phase name.
    pub name: String,
    /// Fault chains applied to named proxies while the phase runs.
    pub faults: Vec<ProxyFaults>,
    /// Omitted when the phase runs until explicitly stopped.
    pub duration: Option<HumanDuration>,
    /// Current lifecycle state.
    pub state: PhaseState,
    /// Projected UTC start, known only when every earlier phase is timed.
    pub planned_start_at: Option<chrono::DateTime<chrono::Utc>>,
    /// UTC instant at which the phase started running.
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The lifecycle change carried by a phase transition.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseTransitionKind {
    Added,
    Modified,
    Deleted,
    Started,
    Stopped,
}

impl PhaseTransitionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
            Self::Started => "started",
            Self::Stopped => "stopped",
        }
    }
}

/// Why a phase started or stopped.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseTransitionReason {
    /// Requested by the schedule owner.
    Explicit,
    /// Started because the previous phase stopped.
    Automatic,
    /// Stopped because its duration elapsed.
    DurationElapsed,
    /// Stopped because another phase was started explicitly.
    Superseded,
}

impl PhaseTransitionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Automatic => "automatic",
            Self::DurationElapsed => "duration-elapsed",
            Self::Superseded => "superseded",
        }
    }
}

/// An immutable record of one phase lifecycle change.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct PhaseTransition {
    /// The phase immediately after the change.
    pub phase: ControlledPhase,
    pub kind: PhaseTransitionKind,
    /// Set for `started` and `stopped` transitions.
    pub reason: Option<PhaseTransitionReason>,
}

/// Socket addresses actually bound by a started engine, in configuration
/// order.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct BoundEndpoints {
    /// Bound address of every TCP proxy.
    pub tcp: Vec<SocketAddr>,
    /// Bound address of every UDP proxy.
    pub udp: Vec<SocketAddr>,
}

/// One observation from a running engine: a completed transport record, or
/// a periodic status when no record completed within the status interval.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum EngineEvent {
    /// Current transport counters, emitted when the status interval elapses.
    Status { status: TransportStatus },
    /// A completed TCP stream.
    TcpStream { stream: TcpStreamRecord },
    /// A completed UDP request/response exchange.
    UdpExchange { exchange: UdpExchangeRecord },
}

impl From<TransportRecord> for EngineEvent {
    fn from(record: TransportRecord) -> Self {
        match record {
            TransportRecord::TcpStream { stream } => Self::TcpStream { stream },
            TransportRecord::UdpExchange { exchange } => {
                Self::UdpExchange { exchange }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_transitions_use_kebab_case() {
        let value =
            serde_json::to_value(PhaseTransitionReason::DurationElapsed)
                .unwrap();
        assert_eq!(value, "duration-elapsed");
        assert_eq!(
            PhaseTransitionReason::DurationElapsed.as_str(),
            "duration-elapsed"
        );
    }

    #[test]
    fn endpoints_serialize_as_strings() {
        let endpoints = BoundEndpoints {
            tcp: vec!["127.0.0.1:1".parse().unwrap()],
            udp: vec![],
        };
        assert_eq!(
            serde_json::to_value(endpoints).unwrap(),
            serde_json::json!({ "tcp": ["127.0.0.1:1"], "udp": [] })
        );
    }
}
