mod engine;
mod error;
mod json;

use pyo3::prelude::*;

use crate::engine::Engine;
use crate::error::PhaseStateError;

#[pymodule]
fn _fault(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Engine>().and_then(|_| {
        module.add("PhaseStateError", module.py().get_type::<PhaseStateError>())
    })
}
