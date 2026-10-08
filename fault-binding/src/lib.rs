//! The engine handle shared by every fault language binding.
//!
//! Bindings must stay thin: lifecycle, scheduling, validation, defaults, and
//! event semantics live here or in `fault-engine`, never in Python or
//! TypeScript. A binding converts JSON strings and maps [`ErrorKind`] onto
//! its native error types, nothing more.

mod engine;
mod error;

pub use engine::DEFAULT_EVENT_CAPACITY;
pub use engine::DEFAULT_STATUS_INTERVAL;
pub use engine::Engine;
pub use error::Error;
pub use error::ErrorKind;
pub use error::Result;
