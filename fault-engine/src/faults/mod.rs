mod bandwidth;
mod blackhole;
mod connection_reset;
pub(crate) mod delay;
mod jitter;
mod latency;
mod runtime;
pub(crate) mod stream;

use std::net::SocketAddr;
use std::sync::Arc;

use fault_model::FaultSpec;

use self::bandwidth::BandwidthStream;
use self::blackhole::BlackholeStream;
use self::connection_reset::ConnectionResetStream;
use self::delay::DelaySampler;
use self::jitter::JitterStream;
use self::latency::LatencyStream;
pub(crate) use self::runtime::ActiveFaults;
pub(crate) use self::runtime::DynamicFaultStream;
pub(crate) use self::runtime::FaultRuntime;
use self::stream::Bidirectional;
use crate::EngineError;

pub(crate) struct InjectionContext {
    pub(crate) connection_id: uuid::Uuid,
    pub(crate) peer: SocketAddr,
    pub(crate) metrics: Arc<crate::observation::TransportMetrics>,
}

pub(crate) trait FaultInjector: Send + Sync {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError>;
}

#[derive(Clone, Default)]
pub(crate) struct FaultChain {
    injectors: Vec<Arc<dyn FaultInjector>>,
}

impl FaultChain {
    pub(crate) fn from_specs(specs: &[FaultSpec]) -> Result<Self, EngineError> {
        let mut injectors: Vec<Arc<dyn FaultInjector>> = Vec::new();

        for spec in specs {
            match spec {
                FaultSpec::Latency { flow, distribution } => {
                    injectors.push(Arc::new(LatencyInjector {
                        sampler: DelaySampler::new(distribution)?,
                        flow: *flow,
                    }));
                }
                FaultSpec::Bandwidth { flow, bytes_per_second } => {
                    if *bytes_per_second == 0 {
                        return Err(EngineError::InvalidFaultConfig(
                            "bandwidth must be greater than zero".into(),
                        ));
                    }
                    injectors.push(Arc::new(BandwidthInjector {
                        bytes_per_second: *bytes_per_second,
                        flow: *flow,
                    }));
                }
                FaultSpec::Jitter {
                    flow,
                    min_delay_ms,
                    max_delay_ms,
                    probability,
                } => {
                    validate_jitter(
                        *min_delay_ms,
                        *max_delay_ms,
                        *probability,
                    )?;
                    let distribution =
                        fault_model::DelayDistribution::Uniform {
                            min_ms: *min_delay_ms,
                            max_ms: *max_delay_ms,
                        };
                    injectors.push(Arc::new(JitterInjector {
                        sampler: DelaySampler::new(&distribution)?,
                        probability: *probability,
                        flow: *flow,
                    }));
                }
                FaultSpec::Blackhole { flow } => {
                    injectors.push(Arc::new(BlackholeInjector { flow: *flow }));
                }
                FaultSpec::ConnectionReset { flow, probability } => {
                    validate_probability("connection-reset", *probability)?;
                    injectors.push(Arc::new(ConnectionResetInjector {
                        flow: *flow,
                        probability: *probability,
                    }));
                }
                FaultSpec::Dns { .. } => {}
            }
        }

        Ok(Self { injectors })
    }

    pub(crate) fn wrap_stream(
        &self,
        context: &InjectionContext,
        mut stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        // Wrappers are nested, so reverse construction makes the first
        // configured fault the first one encountered by traffic.
        for injector in self.injectors.iter().rev() {
            stream = injector.on_stream(context, stream)?;
        }
        Ok(stream)
    }
}

struct BlackholeInjector {
    flow: fault_model::TrafficFlow,
}

impl FaultInjector for BlackholeInjector {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        Ok(Box::new(BlackholeStream::new(
            stream,
            self.flow,
            context.metrics.clone(),
        )))
    }
}

struct ConnectionResetInjector {
    flow: fault_model::TrafficFlow,
    probability: f64,
}

impl FaultInjector for ConnectionResetInjector {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        let should_reset = rand::random_bool(self.probability);
        Ok(Box::new(ConnectionResetStream::new(
            stream,
            self.flow,
            should_reset,
            context.metrics.clone(),
        )))
    }
}

fn validate_jitter(
    min_delay_ms: f64,
    max_delay_ms: f64,
    probability: f64,
) -> Result<(), EngineError> {
    if !min_delay_ms.is_finite()
        || !max_delay_ms.is_finite()
        || min_delay_ms < 0.0
        || max_delay_ms < min_delay_ms
    {
        return Err(EngineError::InvalidFaultConfig(
            "jitter delay range must be finite, non-negative, and ordered"
                .into(),
        ));
    }
    validate_probability("jitter", probability)
}

fn validate_probability(
    name: &str,
    probability: f64,
) -> Result<(), EngineError> {
    if probability.is_finite() && (0.0..=1.0).contains(&probability) {
        Ok(())
    } else {
        Err(EngineError::InvalidFaultConfig(format!(
            "{name} probability must be between zero and one"
        )))
    }
}

struct LatencyInjector {
    sampler: DelaySampler,
    flow: fault_model::TrafficFlow,
}

impl FaultInjector for LatencyInjector {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        let _ = (context.connection_id, context.peer);
        Ok(Box::new(LatencyStream::new(
            stream,
            self.sampler.clone(),
            self.flow,
            context.metrics.clone(),
        )))
    }
}

struct BandwidthInjector {
    bytes_per_second: u64,
    flow: fault_model::TrafficFlow,
}

impl FaultInjector for BandwidthInjector {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        Ok(Box::new(BandwidthStream::new(
            stream,
            self.bytes_per_second,
            self.flow,
            context.metrics.clone(),
        )))
    }
}

struct JitterInjector {
    sampler: DelaySampler,
    probability: f64,
    flow: fault_model::TrafficFlow,
}

impl FaultInjector for JitterInjector {
    fn on_stream(
        &self,
        context: &InjectionContext,
        stream: Box<dyn Bidirectional>,
    ) -> Result<Box<dyn Bidirectional>, EngineError> {
        Ok(Box::new(JitterStream::new(
            stream,
            self.sampler.clone(),
            self.probability,
            self.flow,
            context.metrics.clone(),
        )))
    }
}
