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
pub enum TransportProtocol {
    /// A connection-oriented byte stream.
    Tcp,
    /// Connectionless datagrams, including DNS traffic.
    Udp,
}

impl TransportProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Proxy {
    /// Stable name used by phases to select this proxy.
    #[schemars(length(min = 1))]
    pub name: String,
    /// Transport accepted and forwarded by this proxy.
    pub protocol: TransportProtocol,
    /// Local socket address clients connect or send datagrams to.
    pub listen: String,
    /// Remote socket address to which traffic is forwarded.
    pub upstream: String,
}
