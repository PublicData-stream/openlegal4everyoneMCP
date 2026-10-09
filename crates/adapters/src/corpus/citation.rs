//! Durable, capture-scoped citation leases. Evidence is permanently archived;
//! lease admission serializes with withdrawal on the corpus control row.
use super::*;
use openlegal_application::citation::{
    CITATION_LEASE_SECONDS, CitationLease, MAX_CITATION_LEASES, MAX_CITATION_SEARCH_RESULTS,
};
use std::collections::BTreeMap;

impl PgCorpusStore {
    async fn renew_citation_leases(
        &self,
        objects: Vec<(ObjectId, String)>,
        origin_session: Option<&str>,
        now: u64,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        check(&cancel)?;
        if objects.len() > MAX_CITATION_SEARCH_RESULTS {
            return Err(DatabaseError::InvalidInput);
        }
        let mut selected = BTreeMap::new();
        for (object, capture_id) in objects {
            let object_key = key(&object)?;
            if !openlegal_domain::history::valid_snapshot_id(&capture_id) {
                return Err(DatabaseError::InvalidInput);
            }
            if let Some((previous, _)) = selected.insert(capture_id, (object, object_key.clone()))
                && key(&previous)? != object_key
            {
                return Err(DatabaseError::InvalidInput);
            }
        }
        if selected.is_empty() && origin_session.is_none() {
            return Ok(());
        }
        self.gate().await?;
        let mut tx = begin_storage(&self.pool, "renew_citation_leases").await?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        check(&cancel)?;
        // Even a HEAD capture cannot legitimize a result from a generation
        // invalidated by withdrawal of another object. Check the origin while
        // holding the same lock that withdrawal and lease admission acquire.
        if let Some(id) = origin_session {
            let row = sqlx::query(
                "SELECT invalidated,expires_at::text FROM openlegal.corpus_session WHERE id=$1",
            )
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(DatabaseError::SessionExpired)?;
            if row.try_get::<bool, _>("invalidated").map_err(db)? {
                return Err(DatabaseError::SnapshotInvalidated);
            }
            if unsigned(&row, "expires_at")? <= now {
                return Err(DatabaseError::SessionExpired);
            }
        }
        // Withdrawal and lease admission hold the same control lock. Lease expiry
        // bounds bookkeeping without making archived evidence unavailable.
        sqlx::query("DELETE FROM openlegal.corpus_citation_lease l WHERE l.expires_at<=$1::text::numeric OR EXISTS(SELECT 1 FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=l.capture_id AND o.withdrawn)")
            .bind(now.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        let mut additional = 0usize;
        for (capture_id, (object, object_key)) in &selected {
            let row = sqlx::query("SELECT c.object_key,o.identity,o.withdrawn,EXISTS(SELECT 1 FROM openlegal.corpus_citation_lease l WHERE l.capture_id=c.id) AS leased FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=$1")
                .bind(capture_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(DatabaseError::RevisionUnavailable)?;
            if row.try_get::<String, _>("object_key").map_err(db)? != *object_key {
                return Err(DatabaseError::RevisionUnavailable);
            }
            let stored: ObjectId =
                serde_json::from_value(row.try_get("identity").map_err(db)?).map_err(corrupt)?;
            if &stored != object {
                return Err(DatabaseError::StorageCorrupt);
            }
            if row.try_get::<bool, _>("withdrawn").map_err(db)? {
                return Err(DatabaseError::Withdrawn);
            }
            additional += usize::from(!row.try_get::<bool, _>("leased").map_err(db)?);
        }
        if usize::try_from(active)
            .map_err(corrupt)?
            .saturating_add(additional)
            > MAX_CITATION_LEASES
        {
            return Err(DatabaseError::Capacity);
        }
        let expiry = now.saturating_add(CITATION_LEASE_SECONDS).to_string();
        for capture_id in selected.keys() {
            check(&cancel)?;
            sqlx::query("INSERT INTO openlegal.corpus_citation_lease(capture_id,expires_at) VALUES($1,$2::text::numeric) ON CONFLICT(capture_id) DO UPDATE SET expires_at=GREATEST(openlegal.corpus_citation_lease.expires_at,EXCLUDED.expires_at)")
                .bind(capture_id)
                .bind(&expiry)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        check(&cancel)?;
        tx.commit().await.map_err(db)
    }

    pub(crate) async fn renew_search_citation_leases(
        &self,
        objects: Vec<(ObjectId, String)>,
        origin_session: &str,
        now: u64,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        self.renew_citation_leases(objects, Some(origin_session), now, cancel)
            .await
    }
}

impl CitationLease for PgCorpusStore {
    fn renew(
        &self,
        objects: Vec<(ObjectId, String)>,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<(), DatabaseError>> {
        let this = self.clone();
        Box::pin(async move { this.renew_citation_leases(objects, None, now, cancel).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::FsBlobStore;
    use openlegal_application::{database::Publication, persistence::PersistentStore};
    use openlegal_domain::legal::{Dataset, LegalRecord};

    struct FixedClock;
    impl openlegal_application::Clock for FixedClock {
        fn now(&self) -> u64 {
            0
        }
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn invalidated_origin_cannot_lease_unwithdrawn_head_or_an_empty_page() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = FsBlobStore::open(&fixture.directory.path().join("citation-origin"))
            .await
            .unwrap();
        let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixedClock));
        let selected = ObjectId {
            jurisdiction: "kr".into(),
            provider: "fictional_test".into(),
            dataset: Dataset::NationalStatute,
            id: "selected".into(),
        };
        let other = ObjectId {
            id: "withdrawn".into(),
            ..selected.clone()
        };
        let mut captures = Vec::new();
        for object in [&selected, &other] {
            captures.push(
                store
                    .publish(
                        Publication {
                            record: LegalRecord {
                                object: object.clone(),
                                revision_id: "r1".into(),
                                title: "Fictional fixture".into(),
                                body: "Fixture text".into(),
                                metadata: Default::default(),
                                publication_date: None,
                                effective_date: None,
                                source_url: "https://example.test/fictional".into(),
                                representation: "provider_text_v1".into(),
                                sections: vec![],
                            },
                            additional_evidence: vec![],
                            raw: b"Fixture text".to_vec(),
                            processor_version: "fixture_v1".into(),
                            retrieved_at: 100,
                            now: 100,
                            expected_version: 0,
                            install_head: true,
                            job_id: None,
                        },
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap(),
            );
        }
        let session = "a".repeat(64);
        store
            .pin_session(
                session.clone(),
                store.watermark().await.unwrap(),
                vec![],
                110,
            )
            .await
            .unwrap();
        store
            .renew_search_citation_leases(vec![], &session, 110, CancellationToken::new())
            .await
            .unwrap();
        let version = store.state(&other).await.unwrap().version;
        store.withdraw(&other, version, 111).await.unwrap();
        let selected_capture = captures[0].capture_id.clone();
        let objects = vec![(selected.clone(), selected_capture.clone())];
        assert_eq!(
            store
                .renew_search_citation_leases(
                    objects.clone(),
                    &session,
                    112,
                    CancellationToken::new()
                )
                .await,
            Err(DatabaseError::SnapshotInvalidated)
        );
        assert_eq!(
            store
                .renew_search_citation_leases(vec![], &session, 112, CancellationToken::new())
                .await,
            Err(DatabaseError::SnapshotInvalidated)
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
            .fetch_one(&base.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        // The selected HEAD remains eligible for an independent public read;
        // only the invalidated search generation is barred from returning it.
        store
            .renew(objects, 112, CancellationToken::new())
            .await
            .unwrap();
        store.release_session(&session).await.unwrap();
        assert_eq!(
            store
                .renew_search_citation_leases(vec![], &session, 112, CancellationToken::new())
                .await,
            Err(DatabaseError::SessionExpired)
        );
        base.close().await.unwrap();
    }
}
