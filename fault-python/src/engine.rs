use std::future::Future;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::error::to_py;

/// Native engine; every method forwards to [`fault_binding::Engine`].
#[pyclass]
pub(crate) struct Engine {
    inner: fault_binding::Engine,
}

fn forward<'py, T, F>(py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = fault_binding::Result<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    future_into_py(py, async move { future.await.map_err(to_py) })
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (config_json, event_capacity=None))]
    fn new(config_json: &str, event_capacity: Option<i64>) -> PyResult<Self> {
        fault_binding::Engine::new(config_json, event_capacity)
            .map(|inner| Self { inner })
            .map_err(to_py)
    }

    fn alive(&self) -> bool {
        self.inner.alive()
    }

    fn endpoints(&self) -> PyResult<String> {
        self.inner.endpoints().map_err(to_py)
    }

    fn summary(&self) -> Option<String> {
        self.inner.summary()
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.start().await })
    }

    fn set_faults<'py>(
        &self,
        py: Python<'py>,
        proxy: String,
        faults_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move {
            engine.set_faults(proxy, faults_json).await.map(|()| None::<()>)
        })
    }

    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.run().await })
    }

    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.status().await })
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.snapshot().await })
    }

    fn active_faults<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.active_faults().await })
    }

    fn next_progress<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.next_progress().await })
    }

    fn next_record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.next_record().await })
    }

    #[pyo3(signature = (status_interval=None))]
    fn next_event<'py>(
        &self,
        py: Python<'py>,
        status_interval: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.next_event(status_interval).await })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.shutdown().await })
    }

    fn begin_schedule<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move {
            engine.begin_schedule().await.map(|()| None::<()>)
        })
    }

    fn schedule_active(&self) -> bool {
        self.inner.schedule_active()
    }

    fn end_schedule<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { Ok(engine.end_schedule().await) })
    }

    #[pyo3(signature = (name, duration, faults_json))]
    fn schedule_add_phase<'py>(
        &self,
        py: Python<'py>,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move {
            engine.add_phase(name, duration, faults_json).await
        })
    }

    #[pyo3(signature = (id, name, duration, faults_json))]
    fn schedule_modify_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move {
            engine.modify_phase(id, name, duration, faults_json).await
        })
    }

    fn schedule_delete_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.delete_phase(id).await })
    }

    fn schedule_start_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.start_phase(id).await })
    }

    fn schedule_stop_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.stop_phase(id).await })
    }

    fn schedule_move_phase<'py>(
        &self,
        py: Python<'py>,
        id: String,
        position: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.move_phase(id, position).await })
    }

    fn schedule_next_transition<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        forward(py, async move { engine.next_transition().await })
    }
}
