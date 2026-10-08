use std::fmt;

/// How a language binding should surface an [`Error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The caller passed a malformed or out-of-range argument.
    InvalidInput,
    /// A phase operation targeted a phase in the wrong lifecycle state.
    PhaseState,
    /// Any other engine or lifecycle failure.
    Runtime,
}

impl ErrorKind {
    /// Stable identifier for bindings that expose error codes.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "INVALID_INPUT",
            Self::PhaseState => "PHASE_STATE",
            Self::Runtime => "RUNTIME",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::InvalidInput, message: message.into() }
    }

    pub(crate) fn runtime(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Runtime, message: message.into() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for Error {}

impl From<fault_engine::EngineError> for Error {
    fn from(error: fault_engine::EngineError) -> Self {
        let kind = match error {
            fault_engine::EngineError::UnknownPhase(_)
            | fault_engine::EngineError::PhaseImmutable { .. }
            | fault_engine::EngineError::PhaseNotRunning(_) => {
                ErrorKind::PhaseState
            }
            _ => ErrorKind::Runtime,
        };
        Self { kind, message: error.to_string() }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
