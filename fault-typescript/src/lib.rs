//! Node-API binding for fault. Every method forwards to
//! [`fault_binding::Engine`]; values cross as JSON strings.
//!
//! Errors are rejected as `Error`s whose message starts with the bracketed
//! binding error code, such as `[PHASE_STATE] phase ... is not running`.
//! The TypeScript wrapper turns that code into an error class.

use napi::Result;
use napi_derive::napi;

fn to_js(error: fault_binding::Error) -> napi::Error {
    napi::Error::from_reason(format!(
        "[{}] {}",
        error.kind.code(),
        error.message
    ))
}

/// Native engine handle. Use the `Engine` class exported by `faultlib`.
#[napi]
pub struct NativeEngine {
    inner: fault_binding::Engine,
}

#[napi]
impl NativeEngine {
    #[napi(constructor)]
    pub fn new(
        config_json: String,
        event_capacity: Option<i64>,
    ) -> Result<Self> {
        fault_binding::Engine::new(&config_json, event_capacity)
            .map(|inner| Self { inner })
            .map_err(to_js)
    }

    #[napi]
    pub fn alive(&self) -> bool {
        self.inner.alive()
    }

    #[napi]
    pub fn endpoints(&self) -> Result<String> {
        self.inner.endpoints().map_err(to_js)
    }

    #[napi]
    pub fn summary(&self) -> Option<String> {
        self.inner.summary()
    }

    #[napi]
    pub async fn start(&self) -> Result<String> {
        self.inner.start().await.map_err(to_js)
    }

    #[napi]
    pub async fn set_faults(
        &self,
        proxy: String,
        faults_json: String,
    ) -> Result<()> {
        self.inner.set_faults(proxy, faults_json).await.map_err(to_js)
    }

    #[napi]
    pub async fn run(&self) -> Result<String> {
        self.inner.run().await.map_err(to_js)
    }

    #[napi]
    pub async fn status(&self) -> Result<String> {
        self.inner.status().await.map_err(to_js)
    }

    #[napi]
    pub async fn snapshot(&self) -> Result<String> {
        self.inner.snapshot().await.map_err(to_js)
    }

    #[napi]
    pub async fn active_faults(&self) -> Result<String> {
        self.inner.active_faults().await.map_err(to_js)
    }

    #[napi]
    pub async fn next_progress(&self) -> Result<Option<String>> {
        self.inner.next_progress().await.map_err(to_js)
    }

    #[napi]
    pub async fn next_record(&self) -> Result<Option<String>> {
        self.inner.next_record().await.map_err(to_js)
    }

    #[napi]
    pub async fn next_event(
        &self,
        status_interval: Option<f64>,
    ) -> Result<Option<String>> {
        self.inner.next_event(status_interval).await.map_err(to_js)
    }

    #[napi]
    pub async fn shutdown(&self) -> Result<String> {
        self.inner.shutdown().await.map_err(to_js)
    }

    #[napi]
    pub async fn close(&self) -> Result<Option<String>> {
        self.inner.close().await.map_err(to_js)
    }

    #[napi]
    pub async fn begin_schedule(&self) -> Result<()> {
        self.inner.begin_schedule().await.map_err(to_js)
    }

    #[napi]
    pub fn schedule_active(&self) -> bool {
        self.inner.schedule_active()
    }

    #[napi]
    pub async fn end_schedule(&self) -> bool {
        self.inner.end_schedule().await
    }

    #[napi]
    pub async fn add_phase(
        &self,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> Result<String> {
        self.inner.add_phase(name, duration, faults_json).await.map_err(to_js)
    }

    #[napi]
    pub async fn modify_phase(
        &self,
        id: String,
        name: String,
        duration: Option<String>,
        faults_json: String,
    ) -> Result<String> {
        self.inner
            .modify_phase(id, name, duration, faults_json)
            .await
            .map_err(to_js)
    }

    #[napi]
    pub async fn delete_phase(&self, id: String) -> Result<String> {
        self.inner.delete_phase(id).await.map_err(to_js)
    }

    #[napi]
    pub async fn start_phase(&self, id: String) -> Result<String> {
        self.inner.start_phase(id).await.map_err(to_js)
    }

    #[napi]
    pub async fn stop_phase(&self, id: String) -> Result<String> {
        self.inner.stop_phase(id).await.map_err(to_js)
    }

    #[napi]
    pub async fn move_phase(
        &self,
        id: String,
        position: i64,
    ) -> Result<String> {
        self.inner.move_phase(id, position).await.map_err(to_js)
    }

    #[napi]
    pub async fn next_transition(&self) -> Result<Option<String>> {
        self.inner.next_transition().await.map_err(to_js)
    }
}
