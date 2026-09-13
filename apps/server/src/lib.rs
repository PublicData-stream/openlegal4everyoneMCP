//! Extensible anonymous MCP server; generic extensions remain read-only.
//! Built-in comparison deletion is explicitly bearer-authorized.
//!
//! Register trusted Rust tool modules and endpoint adapters before binding listeners.
//! Legal retrieval policy belongs in application services, not transport adapters.

pub mod config;
pub mod demo;
pub mod endpoint;
pub mod framing;
pub mod handler;
pub mod http;
pub mod progress;
pub mod registry;
pub mod resources;
pub mod text_diff;
mod timed_io;
pub mod webtransport;
mod widget;

/// Startup/transport failures are local diagnostics and must not be sent to clients.
pub type ServerError = Box<dyn std::error::Error + Send + Sync>;

pub use endpoint::ServerBuilder;
pub use registry::{ToolModule, ToolRegistry};
