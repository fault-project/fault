use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;

pub(crate) trait Bidirectional:
    AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    fn reset(&self) -> std::io::Result<()>;
    fn peel(self: Box<Self>) -> StreamLayer;
}

pub(crate) enum StreamLayer {
    Inner(Box<dyn Bidirectional>),
    Base(Box<dyn Bidirectional>),
}

impl Bidirectional for TcpStream {
    fn reset(&self) -> std::io::Result<()> {
        socket2::SockRef::from(self).set_linger(Some(Duration::ZERO))
    }

    fn peel(self: Box<Self>) -> StreamLayer {
        StreamLayer::Base(self)
    }
}
