//! Bounded, optional progress attached to one tool invocation.

use crate::registry::ToolError;
use rmcp::{Peer, RoleServer, model::*};
use std::sync::Arc;
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

/// Coarse stages only: progress must never include upstream payloads or diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum ProgressStage {
    CheckingCache = 1,
    WaitingForUpstream = 2,
    Fetching = 3,
    Processing = 4,
    Complete = 5,
}

impl ProgressStage {
    fn message(self) -> &'static str {
        match self {
            Self::CheckingCache => "Checking cache",
            Self::WaitingForUpstream => "Waiting for upstream",
            Self::Fetching => "Fetching data",
            Self::Processing => "Processing data",
            Self::Complete => "Complete",
        }
    }
}

struct State {
    last: u8,
    remaining_bytes: usize,
}

/// Clones share one monotonic five-event budget. Missing tokens disable progress.
/// Intermediate updates may be dropped; the final tool result remains authoritative.
#[derive(Clone)]
pub struct ProgressReporter {
    peer: Peer<RoleServer>,
    token: Option<ProgressToken>,
    request: CancellationToken,
    closed: CancellationToken,
    deadline: Instant,
    state: Arc<Mutex<State>>,
}

impl ProgressReporter {
    pub(crate) fn new(
        peer: Peer<RoleServer>,
        token: Option<ProgressToken>,
        request: CancellationToken,
        deadline: Instant,
        budget: usize,
    ) -> Self {
        Self {
            peer,
            token,
            request,
            closed: CancellationToken::new(),
            deadline,
            state: Arc::new(Mutex::new(State {
                last: 0,
                remaining_bytes: budget,
            })),
        }
    }

    pub(crate) fn close_guard(&self) -> tokio_util::sync::DropGuard {
        self.closed.clone().drop_guard()
    }

    /// Report a strictly advancing stage if there is an interested, live caller.
    /// Transport/backpressure failures drop this optional update. Caller cancellation
    /// and the absolute execution deadline continue to be enforced by the handler.
    pub async fn report(&self, stage: ProgressStage) -> Result<(), ToolError> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        tokio::select! {
            biased;
            () = self.request.cancelled() => {},
            () = self.closed.cancelled() => {},
            () = tokio::time::sleep_until(self.deadline) => {},
            () = async {
                let mut state = self.state.lock().await;
                if self.closed.is_cancelled() || self.request.is_cancelled() || stage as u8 <= state.last {
                    return;
                }
                state.last = stage as u8;
                let params = ProgressNotificationParam::new(token.clone(), stage as u8 as f64)
                    .with_total(5.0)
                    .with_message(stage.message());
                let notification = ServerJsonRpcMessage::notification(ServerNotification::ProgressNotification(Notification::new(params.clone())));
                // The only dynamic data is the validated 128-byte token. Include SSE framing.
                let Ok(bytes) = serde_json::to_vec(&notification) else { return; };
                let needed = bytes.len().saturating_add(32);
                if needed > state.remaining_bytes { return; }
                state.remaining_bytes -= needed;
                // A slow progress consumer must not indefinitely stall useful processing.
                let send_deadline = self.deadline.min(Instant::now() + std::time::Duration::from_millis(100));
                let _ = tokio::time::timeout_at(send_deadline, self.peer.notify_progress(params)).await;
            } => {},
        }
        Ok(())
    }
}

/// Tokens are caller supplied and retained by connection/request bookkeeping.
pub(crate) fn validate_progress_token(token: &ProgressToken) -> bool {
    !matches!(&token.0, NumberOrString::String(value) if value.len() > 128)
}
