use std::net::SocketAddr;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BoundEndpoints {
    pub tcp: Vec<SocketAddr>,
    pub udp: Vec<SocketAddr>,
}
