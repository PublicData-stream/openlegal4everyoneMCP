//! Extensible, anonymous read-only MCP server infrastructure.
//!
//! Register trusted Rust tool modules and endpoint adapters before binding listeners.
//! Legal retrieval policy belongs in application services, not transport adapters.

pub mod config;
pub mod endpoint;
pub mod framing;
pub mod handler;
pub mod http;
pub mod registry;
mod timed_io;
pub mod webtransport;

/// Startup/transport failures are local diagnostics and must not be sent to clients.
pub type ServerError = Box<dyn std::error::Error + Send + Sync>;

pub use endpoint::ServerBuilder;
pub use registry::{ToolModule, ToolRegistry};
