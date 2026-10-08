use fault_binding::ErrorKind;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pyo3::create_exception!(_fault, PhaseStateError, PyRuntimeError);

pub(crate) fn to_py(error: fault_binding::Error) -> PyErr {
    match error.kind {
        ErrorKind::InvalidInput => PyValueError::new_err(error.message),
        ErrorKind::PhaseState => PhaseStateError::new_err(error.message),
        ErrorKind::Runtime => PyRuntimeError::new_err(error.message),
    }
}
