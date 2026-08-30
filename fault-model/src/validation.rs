use crate::DelayDistribution;
use crate::FaultSpec;
use crate::Proxy;
use crate::Run;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {message} {hint}")]
pub struct ValidationError {
    pub path: String,
    pub message: String,
    pub hint: String,
}

impl ValidationError {
    fn new(path: impl Into<String>, message: &str, hint: &str) -> Self {
        Self { path: path.into(), message: message.into(), hint: hint.into() }
    }
}

impl Run {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_proxies(&self.proxies)?;
        if self.phases.is_empty() {
            return Err(ValidationError::new(
                "phases",
                "a run needs at least one phase.",
                "Add a phase describing how long a network condition should last.",
            ));
        }
        let proxy_names: std::collections::HashSet<_> =
            self.proxies.iter().map(|proxy| proxy.name.as_str()).collect();
        for (phase_index, phase) in self.phases.iter().enumerate() {
            let mut targeted = std::collections::HashSet::new();
            for (proxy_index, proxy) in phase.proxies.iter().enumerate() {
                let path =
                    format!("phases[{phase_index}].proxies[{proxy_index}]");
                if !proxy_names.contains(proxy.proxy.as_str()) {
                    return Err(ValidationError::new(
                        format!("{path}.proxy"),
                        &format!("proxy {:?} is not defined.", proxy.proxy),
                        "Use the name of a proxy declared in the run.",
                    ));
                }
                if !targeted.insert(proxy.proxy.as_str()) {
                    return Err(ValidationError::new(
                        format!("{path}.proxy"),
                        &format!(
                            "proxy {:?} is configured more than once in this phase.",
                            proxy.proxy
                        ),
                        "Combine its faults into one proxy entry.",
                    ));
                }
                validate_faults_at(&proxy.faults, &format!("{path}.faults"))?;
            }
            if phase.duration.is_none() && phase_index + 1 != self.phases.len()
            {
                return Err(ValidationError::new(
                    format!("phases[{phase_index}].duration"),
                    "an indefinite phase makes later phases unreachable.",
                    "Give this phase a duration or make it the final phase.",
                ));
            }
        }
        Ok(())
    }
}

pub fn validate_faults(faults: &[FaultSpec]) -> Result<(), ValidationError> {
    validate_faults_at(faults, "faults")
}

fn validate_proxies(proxies: &[Proxy]) -> Result<(), ValidationError> {
    if proxies.is_empty() {
        return Err(no_proxies());
    }
    let mut names = std::collections::HashSet::new();
    for (index, proxy) in proxies.iter().enumerate() {
        validate_proxy(
            index,
            &proxy.name,
            &proxy.listen,
            &proxy.upstream,
            &mut names,
        )?;
    }
    Ok(())
}

fn no_proxies() -> ValidationError {
    ValidationError::new(
        "proxies",
        "at least one proxy is required.",
        "Add a proxy with a listen address and an upstream address.",
    )
}

fn validate_proxy<'a>(
    index: usize,
    name: &'a str,
    listen: &str,
    upstream: &str,
    names: &mut std::collections::HashSet<&'a str>,
) -> Result<(), ValidationError> {
    if name.trim().is_empty() {
        return Err(ValidationError::new(
            format!("proxies[{index}].name"),
            "the proxy name is empty.",
            "Use a short stable name such as database or redis.",
        ));
    }
    if !names.insert(name) {
        return Err(ValidationError::new(
            format!("proxies[{index}].name"),
            &format!("proxy name {name:?} is duplicated."),
            "Give every proxy a unique name.",
        ));
    }
    if listen.trim().is_empty() {
        return Err(ValidationError::new(
            format!("proxies[{index}].listen"),
            "the listen address is empty.",
            "Use an address such as 127.0.0.1:8080.",
        ));
    }
    if upstream.trim().is_empty() {
        return Err(ValidationError::new(
            format!("proxies[{index}].upstream"),
            "the upstream address is empty.",
            "Use a host and port such as database:5432.",
        ));
    }
    Ok(())
}

fn validate_faults_at(
    faults: &[FaultSpec],
    path: &str,
) -> Result<(), ValidationError> {
    for (index, fault) in faults.iter().enumerate() {
        let path = format!("{path}[{index}]");
        match fault {
            FaultSpec::Latency { distribution, .. } => {
                validate_distribution(
                    distribution,
                    &format!("{path}.distribution"),
                )?;
            }
            FaultSpec::Jitter {
                min_delay_ms,
                max_delay_ms,
                probability,
                ..
            } => {
                validate_range(*min_delay_ms, *max_delay_ms, &path)?;
                validate_probability(
                    *probability,
                    &format!("{path}.probability"),
                )?;
            }
            FaultSpec::Bandwidth { bytes_per_second, .. } => {
                if *bytes_per_second == 0 {
                    return Err(ValidationError::new(
                        format!("{path}.bytes_per_second"),
                        "bandwidth must be greater than zero.",
                        "Use the number of bytes allowed per second.",
                    ));
                }
            }
            FaultSpec::ConnectionReset { probability, .. } => {
                validate_probability(
                    *probability,
                    &format!("{path}.probability"),
                )?;
            }
            FaultSpec::Blackhole { .. } => {}
            FaultSpec::Dns { .. } => {}
        }
    }
    Ok(())
}

fn validate_distribution(
    distribution: &DelayDistribution,
    path: &str,
) -> Result<(), ValidationError> {
    match distribution {
        DelayDistribution::Normal { mean_ms, stddev_ms } => {
            validate_non_negative(*mean_ms, &format!("{path}.mean_ms"))?;
            validate_positive(*stddev_ms, &format!("{path}.stddev_ms"))
        }
        DelayDistribution::Pareto { shape, scale_ms } => {
            validate_positive(*shape, &format!("{path}.shape"))?;
            validate_positive(*scale_ms, &format!("{path}.scale_ms"))
        }
        DelayDistribution::ParetoNormal {
            pareto_shape,
            pareto_scale_ms,
            normal_mean_ms,
            normal_stddev_ms,
        } => {
            validate_positive(*pareto_shape, &format!("{path}.pareto_shape"))?;
            validate_positive(
                *pareto_scale_ms,
                &format!("{path}.pareto_scale_ms"),
            )?;
            validate_non_negative(
                *normal_mean_ms,
                &format!("{path}.normal_mean_ms"),
            )?;
            validate_positive(
                *normal_stddev_ms,
                &format!("{path}.normal_stddev_ms"),
            )
        }
        DelayDistribution::Uniform { min_ms, max_ms } => {
            validate_range(*min_ms, *max_ms, path)
        }
    }
}

fn validate_probability(value: f64, path: &str) -> Result<(), ValidationError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        return Ok(());
    }
    Err(ValidationError::new(
        path,
        &format!("probability must be between 0 and 1; received {value}."),
        "Use a decimal such as 0.75 for a 75% probability.",
    ))
}

fn validate_range(
    min: f64,
    max: f64,
    path: &str,
) -> Result<(), ValidationError> {
    validate_non_negative(min, &format!("{path}.min_delay_ms"))?;
    validate_non_negative(max, &format!("{path}.max_delay_ms"))?;
    if min <= max {
        return Ok(());
    }
    Err(ValidationError::new(
        path,
        &format!(
            "minimum delay {min}ms is greater than maximum delay {max}ms."
        ),
        "Make min_delay_ms less than or equal to max_delay_ms.",
    ))
}

fn validate_non_negative(
    value: f64,
    path: &str,
) -> Result<(), ValidationError> {
    if value.is_finite() && value >= 0.0 {
        return Ok(());
    }
    Err(ValidationError::new(
        path,
        &format!(
            "delay must be a finite non-negative number; received {value}."
        ),
        "Use milliseconds such as 100 or 250.5.",
    ))
}

fn validate_positive(value: f64, path: &str) -> Result<(), ValidationError> {
    if value.is_finite() && value > 0.0 {
        return Ok(());
    }
    Err(ValidationError::new(
        path,
        &format!(
            "value must be a finite number greater than zero; received {value}."
        ),
        "Choose a positive value appropriate for the distribution.",
    ))
}

#[cfg(test)]
mod tests {
    use crate::FaultSpec;
    use crate::Phase;
    use crate::Proxy;
    use crate::ProxyFaults;
    use crate::Run;
    use crate::SCHEMA_VERSION;
    use crate::TrafficFlow;
    use crate::TransportProtocol;

    #[test]
    fn probability_error_points_to_a_fix() {
        let run = Run {
            schema_version: SCHEMA_VERSION,
            name: "invalid reset".into(),
            proxies: vec![Proxy {
                name: "database".into(),
                protocol: TransportProtocol::Tcp,
                listen: "127.0.0.1:8080".into(),
                upstream: "database:5432".into(),
            }],
            phases: vec![Phase {
                name: "active".into(),
                duration: None,
                proxies: vec![ProxyFaults {
                    proxy: "database".into(),
                    faults: vec![FaultSpec::ConnectionReset {
                        flow: TrafficFlow::Both,
                        probability: 75.0,
                    }],
                }],
            }],
        };

        let error = run.validate().unwrap_err().to_string();

        assert!(error.contains("faults[0].probability"));
        assert!(error.contains("Use a decimal such as 0.75"));
    }

    #[test]
    fn empty_proxy_list_explains_what_to_add() {
        let run = Run {
            schema_version: SCHEMA_VERSION,
            name: "empty".into(),
            proxies: Vec::new(),
            phases: vec![Phase {
                name: "active".into(),
                duration: None,
                proxies: Vec::new(),
            }],
        };

        let error = run.validate().unwrap_err().to_string();

        assert!(error.contains("proxies"));
        assert!(error.contains("Add a proxy"));
    }
}
