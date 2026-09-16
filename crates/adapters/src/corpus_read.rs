//! Content-page retention uses the same durable pins and withdrawal fence as search.
use crate::corpus::PgCorpusStore;
use futures::future::BoxFuture;
use openlegal_application::database_read::ReadRetention;
use openlegal_domain::legal::DatabaseError;
#[derive(Clone)]
pub struct CorpusReadRetention(pub std::sync::Arc<PgCorpusStore>);
impl ReadRetention for CorpusReadRetention {
    fn pin(
        &self,
        id: String,
        capture: String,
        now: u64,
    ) -> BoxFuture<'static, Result<(), DatabaseError>> {
        let store = self.0.clone();
        Box::pin(async move {
            let generation = store.watermark().await?;
            store.pin_session(id, generation, vec![capture], now).await
        })
    }
    fn check(&self, id: String, now: u64) -> BoxFuture<'static, Result<(), DatabaseError>> {
        let store = self.0.clone();
        Box::pin(async move { store.check_session(&id, now).await })
    }
    fn new_id(&self) -> Result<String, DatabaseError> {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(|_| DatabaseError::Capacity)?;
        Ok(bytes.iter().map(|v| format!("{v:02x}")).collect())
    }
}
