use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::runtime_error;

pub(crate) fn from_json<T: DeserializeOwned>(input: &str) -> PyResult<T> {
    serde_json::from_str(input)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

pub(crate) fn to_json(value: &impl Serialize) -> PyResult<String> {
    serde_json::to_string(value)
        .map_err(|error| runtime_error(error.to_string()))
}
