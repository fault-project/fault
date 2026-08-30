#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct TransportSummary {
    pub status: TransportStatus,
    pub tcp_streams: Vec<TcpStreamRecord>,
    pub udp_exchanges: Vec<UdpExchangeRecord>,
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct TransportStatus {
    pub tcp: TcpStreamStatus,
    pub udp: UdpExchangeStatus,
    pub effects: FaultStatus,
    /// Completed records omitted from the best-effort event stream.
    pub dropped_records: u64,
    pub last_failure: Option<TransportFailure>,
}

impl TransportStatus {
    pub fn from_summary(summary: &TransportSummary) -> Self {
        summary.status.clone()
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct TcpStreamStatus {
    pub active: u64,
    pub opened: u64,
    pub active_impacted: u64,
    pub impacted: u64,
    pub completed: u64,
    pub failed: u64,
    pub average_bytes_to_upstream: u64,
    pub average_bytes_to_client: u64,
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct UdpExchangeStatus {
    pub active: u64,
    pub started: u64,
    pub active_impacted: u64,
    pub impacted: u64,
    pub completed: u64,
    pub failed: u64,
    pub average_request_bytes: u64,
    pub average_response_bytes: u64,
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct FaultStatus {
    pub latency_applications: u64,
    pub average_latency_ms: f64,
    pub jitter_applications: u64,
    pub average_jitter_ms: f64,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TransportRecord {
    TcpStream { stream: TcpStreamRecord },
    UdpExchange { exchange: UdpExchangeRecord },
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct TcpStreamRecord {
    pub stream_id: uuid::Uuid,
    pub proxy: String,
    pub peer: String,
    pub upstream: String,
    pub opened_at: chrono::DateTime<chrono::Utc>,
    pub closed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub bytes_to_upstream: u64,
    pub bytes_to_client: u64,
    pub faults: FaultRecord,
    pub outcome: TcpStreamOutcome,
    pub failure: Option<TransportFailure>,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct UdpExchangeRecord {
    pub exchange_id: uuid::Uuid,
    pub proxy: String,
    pub peer: String,
    pub upstream: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub faults: FaultRecord,
    pub outcome: UdpExchangeOutcome,
    pub failure: Option<TransportFailure>,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum TcpStreamOutcome {
    Active,
    Completed,
    UpstreamConnectFailed,
    TransferFailed,
    Cancelled,
    FaultReset,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum UdpExchangeOutcome {
    Active,
    Completed,
    TransferFailed,
    Cancelled,
    FaultDropped,
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct TransportFailure {
    pub stage: TransportFailureStage,
    pub category: TransportFailureCategory,
    pub message: String,
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum TransportFailureStage {
    Connect,
    Transfer,
    Exchange,
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum TransportFailureCategory {
    DnsFailed,
    ConnectionRefused,
    TimedOut,
    NetworkUnreachable,
    ConnectionReset,
    BrokenPipe,
    Other,
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct FaultRecord {
    pub latency: DelayRecord,
    pub jitter: DelayRecord,
    pub bandwidth_bytes_limited: u64,
    pub blackhole_activations: u64,
    pub connection_resets: u64,
    pub dns_interventions: u64,
}

impl FaultRecord {
    pub fn was_impacted(&self) -> bool {
        self.latency.applications > 0
            || self.jitter.applications > 0
            || self.bandwidth_bytes_limited > 0
            || self.blackhole_activations > 0
            || self.connection_resets > 0
            || self.dns_interventions > 0
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct DelayRecord {
    pub applications: u64,
    pub total_delay_ms: f64,
}
