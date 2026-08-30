use std::time::Duration;

use fault_model::DelayDistribution;
use rand_distr::Distribution;
use rand_distr::Normal;
use rand_distr::Pareto;
use rand_distr::Uniform;

use crate::EngineError;

#[derive(Clone)]
pub(crate) enum DelaySampler {
    Normal(Normal<f64>),
    Pareto(Pareto<f64>),
    ParetoNormal { pareto: Pareto<f64>, normal: Normal<f64> },
    Uniform(Uniform<f64>),
}

impl DelaySampler {
    pub(crate) fn new(
        distribution: &DelayDistribution,
    ) -> Result<Self, EngineError> {
        match distribution {
            DelayDistribution::Normal { mean_ms, stddev_ms } => {
                Normal::new(*mean_ms, *stddev_ms)
                    .map(Self::Normal)
                    .map_err(invalid_distribution)
            }
            DelayDistribution::Pareto { shape, scale_ms } => {
                Pareto::new(*scale_ms, *shape)
                    .map(Self::Pareto)
                    .map_err(invalid_distribution)
            }
            DelayDistribution::ParetoNormal {
                pareto_shape,
                pareto_scale_ms,
                normal_mean_ms,
                normal_stddev_ms,
            } => {
                let pareto = Pareto::new(*pareto_scale_ms, *pareto_shape)
                    .map_err(invalid_distribution)?;
                let normal = Normal::new(*normal_mean_ms, *normal_stddev_ms)
                    .map_err(invalid_distribution)?;
                Ok(Self::ParetoNormal { pareto, normal })
            }
            DelayDistribution::Uniform { min_ms, max_ms } => {
                Uniform::new_inclusive(*min_ms, *max_ms)
                    .map(Self::Uniform)
                    .map_err(invalid_distribution)
            }
        }
    }

    pub(crate) fn sample(&self) -> Duration {
        let mut rng = rand::rng();
        let milliseconds = match self {
            Self::Normal(distribution) => {
                sample_non_negative(distribution, &mut rng)
            }
            Self::Pareto(distribution) => distribution.sample(&mut rng),
            Self::ParetoNormal { pareto, normal } => {
                pareto.sample(&mut rng) + sample_non_negative(normal, &mut rng)
            }
            Self::Uniform(distribution) => distribution.sample(&mut rng),
        };

        Duration::from_secs_f64(milliseconds / 1_000.0)
    }
}

fn sample_non_negative(
    distribution: &Normal<f64>,
    rng: &mut impl rand::Rng,
) -> f64 {
    loop {
        let sample = distribution.sample(rng);
        if sample >= 0.0 {
            return sample;
        }
    }
}

fn invalid_distribution(error: impl std::fmt::Display) -> EngineError {
    EngineError::InvalidFaultConfig(error.to_string())
}
