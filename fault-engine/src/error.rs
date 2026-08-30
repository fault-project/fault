#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("proxy {0:?} is not configured")]
    UnknownProxy(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid listen address '{input}': {source}")]
    InvalidListenAddress {
        input: String,
        #[source]
        source: std::net::AddrParseError,
    },

    #[error(
        "could not resolve upstream '{input}': {source}. Use a host and port, such as database:5432"
    )]
    InvalidUpstreamAddress {
        input: String,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(#[from] fault_model::ValidationError),

    #[error("proxy task failed: {0}")]
    ProxyTask(String),

    #[error("invalid fault configuration: {0}")]
    InvalidFaultConfig(String),

    #[error("a run or adaptive phase is already controlling this engine")]
    ControlAlreadyActive,

    #[error("phase {0} does not exist")]
    UnknownPhase(uuid::Uuid),

    #[error("phase {id} is {state} and can no longer be changed")]
    PhaseImmutable { id: uuid::Uuid, state: &'static str },

    #[error("phase {0} is not running")]
    PhaseNotRunning(uuid::Uuid),

    #[error(
        "missed {0} phase transitions because the transition consumer fell behind"
    )]
    MissedPhaseTransitions(u64),

    #[error("transport observation recorder stopped unexpectedly")]
    ObservationRecorderStopped,

    #[error("one or more tasks failed during shutdown: {0:?}")]
    ShutdownErrors(Vec<EngineError>),
}

#[derive(Clone, Debug)]
pub struct RuntimeFailure {
    pub proxy: std::net::SocketAddr,
    pub message: String,
}
