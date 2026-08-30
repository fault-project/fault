use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::Error as _;

use crate::FaultSpec;
use crate::Proxy;
use crate::TransportSummary;
use crate::Versioned;

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// Version of the serialized run format. The current value is `1`.
    #[schemars(extend("const" = crate::SCHEMA_VERSION))]
    pub schema_version: u32,
    /// Human-readable name used in progress, results, and journals.
    #[schemars(length(min = 1))]
    pub name: String,
    /// Network entry points available throughout the run.
    #[schemars(length(min = 1))]
    pub proxies: Vec<Proxy>,
    /// Ordered phases. Each begins when the previous phase ends.
    #[schemars(length(min = 1))]
    pub phases: Vec<Phase>,
}

impl Versioned for Run {
    const TYPE_NAME: &'static str = "Run";

    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

/// A period during which one ordered set of network conditions is active.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    /// Human-readable phase name exposed in progress events.
    #[schemars(length(min = 1))]
    pub name: String,
    /// Omit to keep this phase active until explicitly stopped.
    pub duration: Option<HumanDuration>,
    /// Fault chains applied to named proxies during this phase.
    pub proxies: Vec<ProxyFaults>,
}

/// The complete fault chain active on one named proxy during a phase.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ProxyFaults {
    /// Name of a proxy declared by the containing run.
    #[schemars(length(min = 1))]
    pub proxy: String,
    /// Ordered chain of faults applied to traffic on this proxy.
    pub faults: Vec<FaultSpec>,
}

/// A lightweight phase transition published while a run executes.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct RunProgress {
    /// Name of the executing run.
    pub run_name: String,
    /// Name of the phase that has just started.
    pub phase_name: String,
    /// One-based position of the active phase.
    #[schemars(range(min = 1))]
    pub phase_index: u32,
    #[schemars(range(min = 1))]
    pub phase_count: u32,
    /// Planned duration, or absent for an indefinite final phase.
    pub phase_duration_ms: Option<u64>,
    /// UTC instant at which the phase became active.
    pub phase_started_at: chrono::DateTime<chrono::Utc>,
    /// Fault chains active for this phase.
    pub proxies: Vec<ProxyFaults>,
}

/// A validated, human-readable duration such as `250ms`, `30s`, or `2m`.
#[derive(Clone, Debug, PartialEq, Eq, schemars::JsonSchema)]
#[schemars(transparent)]
pub struct HumanDuration(#[schemars(length(min = 1))] String);

impl HumanDuration {
    pub fn as_std(&self) -> Duration {
        humantime::parse_duration(&self.0)
            .expect("HumanDuration is validated during construction")
    }
}

impl FromStr for HumanDuration {
    type Err = humantime::DurationError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let duration = humantime::parse_duration(input)?;
        if duration.is_zero() {
            return Err(humantime::DurationError::NumberExpected(0));
        }
        Ok(Self(input.to_owned()))
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl serde::Serialize for HumanDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        input.parse().map_err(D::Error::custom)
    }
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct RunResult {
    /// Version of the serialized result format. The current value is `1`.
    #[schemars(extend("const" = crate::SCHEMA_VERSION))]
    pub schema_version: u32,
    /// Name of the completed run.
    pub run_name: String,
    /// UTC instant at which execution began.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// UTC instant at which execution ended.
    pub completed_at: chrono::DateTime<chrono::Utc>,
    /// Whether the run completed successfully or failed.
    pub outcome: RunOutcome,
    /// Aggregate TCP and UDP observations collected during the run.
    pub transport: TransportSummary,
}

impl Versioned for RunResult {
    const TYPE_NAME: &'static str = "RunResult";

    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum RunOutcome {
    Success,
    Failed { message: String },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::HumanDuration;
    use super::Run;

    #[test]
    fn reads_a_phase_oriented_run() {
        let value = serde_json::json!({
            "schema_version": crate::SCHEMA_VERSION,
            "name": "database degradation",
            "proxies": [{
                "name": "database",
                "protocol": "tcp",
                "listen": "127.0.0.1:5433",
                "upstream": "database:5432"
            }],
            "phases": [
                {
                    "name": "normal traffic",
                    "duration": "30s",
                    "proxies": []
                },
                {
                    "name": "reset connections",
                    "duration": "1s",
                    "proxies": [{
                        "proxy": "database",
                        "faults": [{
                            "type": "connection-reset",
                            "flow": "both",
                            "probability": 1.0
                        }]
                    }]
                }
            ]
        });

        let run: Run = serde_json::from_value(value).unwrap();

        assert_eq!(run.phases.len(), 2);
        assert_eq!(
            run.phases[0].duration.as_ref().unwrap().as_std(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn rejects_machine_oriented_duration_numbers() {
        let value = serde_json::json!(30_000);

        assert!(serde_json::from_value::<HumanDuration>(value).is_err());
    }

    #[test]
    fn rejects_zero_duration() {
        assert!("0s".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn accepts_an_indefinite_final_phase() {
        let run: Run = serde_json::from_value(serde_json::json!({
            "schema_version": crate::SCHEMA_VERSION,
            "name": "permanent latency",
            "proxies": [{
                "name": "database",
                "protocol": "tcp",
                "listen": "127.0.0.1:15432",
                "upstream": "database:5432"
            }],
            "phases": [{ "name": "degraded", "proxies": [] }]
        }))
        .unwrap();

        assert!(run.phases[0].duration.is_none());
        assert!(run.validate().is_ok());
    }

    #[test]
    fn rejects_an_indefinite_phase_before_another_phase() {
        let run: Run = serde_json::from_value(serde_json::json!({
            "schema_version": crate::SCHEMA_VERSION,
            "name": "unreachable recovery",
            "proxies": [{
                "name": "database",
                "protocol": "tcp",
                "listen": "127.0.0.1:15432",
                "upstream": "database:5432"
            }],
            "phases": [
                { "name": "degraded", "proxies": [] },
                { "name": "recovered", "duration": "5s", "proxies": [] }
            ]
        }))
        .unwrap();

        let error = run.validate().unwrap_err().to_string();
        assert!(error.contains("makes later phases unreachable"));
    }

    #[test]
    fn reads_the_example_run() {
        let run: Run =
            serde_json::from_str(include_str!("../../examples/timed-run.json"))
                .unwrap();

        assert_eq!(run.proxies.len(), 1);
        assert_eq!(run.phases.len(), 3);
    }
}
