use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pyo3::create_exception!(_fault, PhaseStateError, PyRuntimeError);

pub(crate) fn engine_error(error: fault_engine::EngineError) -> PyErr {
    match error {
        fault_engine::EngineError::UnknownPhase(_)
        | fault_engine::EngineError::PhaseImmutable { .. }
        | fault_engine::EngineError::PhaseNotRunning(_) => {
            PhaseStateError::new_err(error.to_string())
        }
        _ => runtime_error(error.to_string()),
    }
}

pub(crate) fn runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}
