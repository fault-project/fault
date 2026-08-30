#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FaultSpec {
    /// Adds one sampled delay to the selected direction. TCP applies it to the
    /// first non-empty traffic after activation; UDP applies it once to each
    /// selected datagram in an exchange.
    Latency { flow: TrafficFlow, distribution: DelayDistribution },
    /// Adds a probabilistic sampled delay. TCP evaluates it per data operation;
    /// UDP evaluates it once for each selected datagram in an exchange.
    Jitter {
        flow: TrafficFlow,
        #[schemars(range(min = 0.0))]
        min_delay_ms: f64,
        #[schemars(range(min = 0.0))]
        max_delay_ms: f64,
        #[schemars(range(min = 0.0, max = 1.0))]
        probability: f64,
    },
    /// Limits transferred bytes per second independently for each connection
    /// stream in the selected flow.
    Bandwidth {
        flow: TrafficFlow,
        #[schemars(range(min = 1))]
        bytes_per_second: u64,
    },
    /// Leaves TCP operations pending until cancellation, or drops UDP
    /// datagrams in the selected direction.
    Blackhole { flow: TrafficFlow },
    /// Resets a selected connection when traffic first uses the chosen flow.
    ConnectionReset {
        flow: TrafficFlow,
        #[schemars(range(min = 0.0, max = 1.0))]
        probability: f64,
    },
    /// Alters DNS queries carried by a UDP proxy.
    Dns {
        case: DnsCase,
        #[schemars(range(min = 0))]
        delay_ms: Option<u64>,
    },
}

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum DnsCase {
    Delay,
    Timeout,
    Truncated,
    Refused,
    ServFail,
    NxDomain,
    EmptyAnswer,
    RandomA,
}

#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DelayDistribution {
    Normal {
        #[schemars(range(min = 0.0))]
        mean_ms: f64,
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        stddev_ms: f64,
    },
    Pareto {
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        shape: f64,
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        scale_ms: f64,
    },
    ParetoNormal {
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        pareto_shape: f64,
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        pareto_scale_ms: f64,
        #[schemars(range(min = 0.0))]
        normal_mean_ms: f64,
        #[schemars(extend("exclusiveMinimum" = 0.0))]
        normal_stddev_ms: f64,
    },
    Uniform {
        #[schemars(range(min = 0.0))]
        min_ms: f64,
        #[schemars(range(min = 0.0))]
        max_ms: f64,
    },
}

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum TrafficFlow {
    /// Traffic sent by the client toward the upstream service.
    ToUpstream,
    /// Traffic returned by the upstream service toward the client.
    ToClient,
    /// Traffic in both directions.
    Both,
}

#[cfg(test)]
mod tests {
    use super::FaultSpec;
    use super::TrafficFlow;

    #[test]
    fn bandwidth_has_a_single_unambiguous_wire_shape() {
        let fault = FaultSpec::Bandwidth {
            flow: TrafficFlow::ToUpstream,
            bytes_per_second: 65_536,
        };

        let value = serde_json::to_value(fault).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "type": "bandwidth",
                "flow": "to-upstream",
                "bytes_per_second": 65_536
            })
        );
    }

    #[test]
    fn contradictory_legacy_shape_is_rejected() {
        let value = serde_json::json!({
            "kind": "latency",
            "enabled": true,
            "direction": "ingress",
            "side": "client",
            "params": {
                "type": "bandwidth",
                "bytes_per_second": 65_536
            }
        });

        assert!(serde_json::from_value::<FaultSpec>(value).is_err());
    }
}
