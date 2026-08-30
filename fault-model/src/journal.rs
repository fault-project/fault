use crate::Proxy;
use crate::ProxyFaults;
use crate::TcpStreamRecord;
use crate::TransportStatus;
use crate::UdpExchangeRecord;

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum JournalEvent {
    RunStarted {
        #[schemars(extend("const" = crate::SCHEMA_VERSION))]
        schema_version: u32,
        started_at: chrono::DateTime<chrono::Utc>,
        name: Option<String>,
        proxies: Vec<Proxy>,
        faults: Vec<ProxyFaults>,
    },
    TcpStreamCompleted {
        stream: TcpStreamRecord,
    },
    UdpExchangeCompleted {
        exchange: UdpExchangeRecord,
    },
    RunCompleted {
        completed_at: chrono::DateTime<chrono::Utc>,
        status: TransportStatus,
    },
}
