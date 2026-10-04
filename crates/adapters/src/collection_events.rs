//! Collection wakeups are hints; callers always re-read durable admission state.
use openlegal_domain::legal::DatabaseError;
use sqlx::{
    PgPool,
    postgres::{PgListener, PgPoolOptions},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub const CHANNEL: &str = "openlegal_collection";

struct ListenerTask {
    task: JoinHandle<()>,
    pool: PgPool,
}
impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One connection and one continuously drained task, shared by a process's workers.
/// A watch channel collapses arbitrarily many notifications into one wakeup.
#[derive(Clone)]
pub struct CollectionEvents {
    receiver: watch::Receiver<u64>,
    _task: Arc<ListenerTask>,
}
impl CollectionEvents {
    pub async fn open(pool: &PgPool) -> Result<Self, DatabaseError> {
        // Do not consume a runtime-pool slot indefinitely: minimum runtime pools
        // have two slots, and admission must still be able to read and settle.
        let listener_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy_with((*pool.connect_options()).clone());
        let mut listener = tokio::time::timeout(Duration::from_secs(5), async {
            let mut listener = PgListener::connect_with(&listener_pool).await?;
            listener.listen(CHANNEL).await?;
            Ok::<_, sqlx::Error>(listener)
        })
        .await
        .map_err(|_| DatabaseError::StorageUnavailable)?
        .map_err(|_| DatabaseError::StorageUnavailable)?;
        let (sender, receiver) = watch::channel(0u64);
        let task = tokio::spawn(async move {
            let mut sequence = 0u64;
            loop {
                let received = listener.try_recv().await;
                // None means the listener reconnected. It is also a wakeup:
                // anything committed while disconnected must be rediscovered.
                sequence = sequence.wrapping_add(1);
                sender.send_replace(sequence);
                if received.is_err() {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
        Ok(Self {
            receiver,
            _task: Arc::new(ListenerTask {
                task,
                pool: listener_pool,
            }),
        })
    }

    pub async fn close(&self) {
        self._task.task.abort();
        self._task.pool.close().await;
    }

    /// Register/listen first, query state next, and wait last to avoid lost wakeups.
    pub async fn wait(
        &mut self,
        cancel: &CancellationToken,
        maximum: Duration,
    ) -> Result<(), DatabaseError> {
        tokio::select! {
            _ = cancel.cancelled() => Err(DatabaseError::Cancelled),
            _ = tokio::time::sleep(maximum) => Ok(()),
            result = self.receiver.changed() => result.map_err(|_| DatabaseError::StorageUnavailable),
        }
    }
}
