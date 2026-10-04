//! Original evidence is byte-exact, rights-gated and capture-scoped.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{blob::FsBlobStore, corpus::PgCorpusStore};
use openlegal_application::{database::Publication, persistence::PersistentStore};
use openlegal_domain::{
    legal::*,
    rights::{OriginalResource, SourceRights},
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

struct FixtureClock;
impl openlegal_application::Clock for FixtureClock {
    fn now(&self) -> u64 {
        0
    }
}
fn object() -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional_test".into(),
        dataset: Dataset::NationalStatute,
        id: "001".into(),
    }
}
fn resource(ordinal: u32, rights: SourceRights, retained: bool) -> OriginalResource {
    OriginalResource {
        ordinal,
        title: format!("Original {ordinal}"),
        media_type: "application/octet-stream".into(),
        source_url: "https://example.test/evidence".into(),
        retained,
        rights,
    }
}
async fn publish(
    store: &PgCorpusStore,
    resources: Option<Vec<OriginalResource>>,
    raw: &[u8],
) -> Capture {
    publish_with_mapping(store, resources, raw, None).await
}
async fn publish_with_mapping(
    store: &PgCorpusStore,
    resources: Option<Vec<OriginalResource>>,
    raw: &[u8],
    mapping: Option<&str>,
) -> Capture {
    let body = if resources.as_ref().is_some_and(|resources| {
        resources
            .iter()
            .any(|r| r.ordinal == 0 && !r.rights.can_process())
    }) {
        ""
    } else {
        "provider text"
    };
    let mut metadata = BTreeMap::new();
    if let Some(resources) = resources {
        metadata.insert(
            "original_resources".into(),
            serde_json::to_string(&resources).unwrap(),
        );
    }
    if let Some(mapping) = mapping {
        metadata.insert("attachment_evidence_ordinals".into(), mapping.into());
    }
    store
        .publish(
            Publication {
                record: LegalRecord {
                    object: object(),
                    revision_id: "r1".into(),
                    title: "Fictional evidence".into(),
                    body: body.into(),
                    metadata,
                    publication_date: None,
                    effective_date: None,
                    source_url: "https://example.test/fictional".into(),
                    representation: "provider_text_v1".into(),
                    sections: vec![],
                },
                raw: raw.to_vec(),
                additional_evidence: vec![b"\0unmodified attachment\xff".to_vec()],
                processor_version: "fixture_v1".into(),
                retrieved_at: 100,
                now: 100,
                expected_version: store.state(&object()).await.unwrap().version,
                install_head: true,
                job_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn original_evidence_returns_exact_authorized_bytes_and_preserves_withdrawal() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("original-resources"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let rights = SourceRights::kogl(
        4,
        "https://example.test/license".into(),
        "Fictional issuer".into(),
    );
    assert!(rights.can_store());
    assert!(!rights.can_process());
    let capture = publish(
        &store,
        Some(vec![
            resource(0, SourceRights::legal_information(), true),
            resource(1, rights.clone(), true),
        ]),
        b"provider bytes",
    )
    .await;
    // Corrections to a revision must not redirect its earlier capture's originals.
    let corrected = publish(
        &store,
        Some(vec![resource(0, SourceRights::legal_information(), true)]),
        b"corrected provider bytes",
    )
    .await;
    assert_ne!(capture.capture_id, corrected.capture_id);
    store.maintain(3_000_000, 3_000_000).await.unwrap();
    let application = openlegal_application::database::DatabaseService::new(
        Arc::new(store.clone()),
        Arc::new(FixtureClock),
    );
    let primary = application
        .original_evidence(&capture.capture_id, 0, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(primary.bytes, b"provider bytes");
    let original = store
        .original_evidence(&capture.capture_id, 1, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(original.bytes, b"\0unmodified attachment\xff");
    assert_eq!(original.media_type, "application/octet-stream");
    assert_eq!(original.title, "Original 1");
    assert_eq!(original.rights, rights);
    assert!(
        original
            .rights
            .warnings()
            .contains(&"no_derivatives_original_only")
    );
    assert_eq!(
        store
            .original_evidence(&capture.capture_id, 2, CancellationToken::new())
            .await
            .unwrap_err(),
        DatabaseError::RevisionUnavailable
    );
    let version = store.state(&object()).await.unwrap().version;
    store.withdraw(&object(), version, 3_000_001).await.unwrap();
    assert_eq!(
        store
            .original_evidence(&capture.capture_id, 1, CancellationToken::new())
            .await
            .unwrap_err(),
        DatabaseError::Withdrawn
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn original_evidence_has_no_raw_fallback_for_missing_or_unverified_rights() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("original-denial"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    for (resources, raw) in [
        (None, "legacy"),
        (
            Some(vec![resource(0, SourceRights::default(), true)]),
            "unverified",
        ),
        (
            Some(vec![resource(0, SourceRights::legal_information(), false)]),
            "not retained",
        ),
    ] {
        let capture = publish(&store, resources, raw.as_bytes()).await;
        assert_eq!(
            store
                .original_evidence(&capture.capture_id, 0, CancellationToken::new())
                .await
                .unwrap_err(),
            DatabaseError::RevisionUnavailable
        );
    }
    assert_eq!(
        store
            .original_evidence("../../arbitrary-path", 0, CancellationToken::new())
            .await
            .unwrap_err(),
        DatabaseError::InvalidInput
    );
    let capture = publish(
        &store,
        Some(vec![resource(0, SourceRights::legal_information(), true)]),
        b"valid",
    )
    .await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        store
            .original_evidence(&capture.capture_id, 0, cancelled)
            .await
            .unwrap_err(),
        DatabaseError::Cancelled
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn original_evidence_integrity_failure_closes_serving() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let root = fixture.directory.path().join("original-integrity");
    let blobs = FsBlobStore::open(&root).await.unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let capture = publish(
        &store,
        Some(vec![resource(0, SourceRights::legal_information(), true)]),
        b"original provider bytes",
    )
    .await;
    let storage_key: String =
        sqlx::query_scalar("SELECT storage_key FROM openlegal.corpus_capture WHERE id=$1")
            .bind(&capture.capture_id)
            .fetch_one(&base.pool())
            .await
            .unwrap();
    // Modify only this disposable fixture's already-authorized stored evidence.
    tokio::fs::write(root.join(storage_key), b"tampered provider bytes")
        .await
        .unwrap();
    assert_eq!(
        store
            .original_evidence(&capture.capture_id, 0, CancellationToken::new())
            .await
            .unwrap_err(),
        DatabaseError::StorageCorrupt
    );
    assert!(!store.healthy());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn original_evidence_maps_retained_attachment_after_skipped_unverified_material() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("original-mapped-ordinal"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let rights = SourceRights::kogl(3, "https://example.test/license".into(), "issuer".into());
    let capture = publish_with_mapping(
        &store,
        Some(vec![
            resource(0, SourceRights::legal_information(), true),
            resource(1, SourceRights::default(), false),
            resource(2, rights.clone(), true),
        ]),
        b"provider bytes",
        Some("[2]"),
    )
    .await;
    assert_eq!(
        store
            .original_evidence(&capture.capture_id, 1, CancellationToken::new())
            .await
            .unwrap_err(),
        DatabaseError::RevisionUnavailable
    );
    let file = store
        .original_evidence(&capture.capture_id, 2, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(file.bytes, b"\0unmodified attachment\xff");
    assert_eq!(file.rights, rights);
    assert_eq!(file.title, "Original 2");
    base.close().await.unwrap();
}
