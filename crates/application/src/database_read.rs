//! Ten-minute immutable content sessions; readers never substitute a later HEAD.
use crate::{Clock, database::DatabaseService};
use futures::future::BoxFuture;
use openlegal_domain::legal::{
    DatabaseError as E, GetRequest, GetResult, ObjectId, RevisionSelector,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
pub trait ReadRetention: Send + Sync + 'static {
    fn pin(&self, id: String, capture: String, now: u64) -> BoxFuture<'static, Result<(), E>>;
    fn check(&self, id: String, now: u64) -> BoxFuture<'static, Result<(), E>>;
    fn new_id(&self) -> Result<String, E>;
}
#[derive(Clone)]
struct Session {
    object: ObjectId,
    capture: String,
    expires: u64,
}
pub struct DatabaseReader {
    database: Arc<DatabaseService>,
    retention: Arc<dyn ReadRetention>,
    clock: Arc<dyn Clock>,
    sessions: Mutex<BTreeMap<String, Session>>,
}
impl DatabaseReader {
    pub fn new(
        database: Arc<DatabaseService>,
        retention: Arc<dyn ReadRetention>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            retention,
            clock,
            sessions: Mutex::new(BTreeMap::new()),
        }
    }
    pub async fn get(
        &self,
        request: GetRequest,
        session: Option<String>,
        cancel: CancellationToken,
    ) -> Result<(GetResult, String), E> {
        request.object.validate()?;
        request.selector.validate()?;
        let time = self.clock.now();
        if let Some(id) = session {
            if !openlegal_domain::history::valid_snapshot_id(&id) {
                return Err(E::InvalidInput);
            }
            let session = {
                let sessions = self.sessions.lock().map_err(|_| E::Capacity)?;
                sessions.get(&id).cloned().ok_or(E::SessionExpired)?
            };
            if session.expires <= time {
                return Err(E::SessionExpired);
            }
            if request.object != session.object
                || request.selector
                    != (RevisionSelector::Capture {
                        id: session.capture,
                    })
            {
                return Err(E::InvalidInput);
            }
            self.retention.check(id.clone(), time).await?;
            let result = self.database.get(request, cancel).await?;
            self.retention.check(id.clone(), self.clock.now()).await?;
            return Ok((result, id));
        }
        let result = self.database.get(request, cancel).await?;
        let id = self.retention.new_id()?;
        {
            let mut sessions = self.sessions.lock().map_err(|_| E::Capacity)?;
            sessions.retain(|_, s| s.expires > time);
            if sessions.len() >= 64 {
                return Err(E::Capacity);
            }
            if sessions.contains_key(&id) {
                return Err(E::Capacity);
            }
            sessions.insert(
                id.clone(),
                Session {
                    object: result.capture.record.object.clone(),
                    capture: result.capture.capture_id.clone(),
                    expires: time.saturating_add(600),
                },
            );
        }
        if let Err(e) = self
            .retention
            .pin(id.clone(), result.capture.capture_id.clone(), time)
            .await
        {
            self.sessions.lock().map_err(|_| E::Capacity)?.remove(&id);
            return Err(e);
        }
        self.retention.check(id.clone(), self.clock.now()).await?;
        Ok((result, id))
    }
}
