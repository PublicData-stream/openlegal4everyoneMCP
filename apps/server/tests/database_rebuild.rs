//! Offline, fictional replay fixtures. No provider credentials or network calls.
#[path = "../../../test-support/postgres.rs"]
mod postgres;
use openlegal_adapters::{
    blob::FsBlobStore, corpus::PgCorpusStore, korean_analysis::KoreanAnalyzer,
    search_index::CorpusIndex,
};
use openlegal_application::{blob::BlobStore, database::Publication, persistence::PersistentStore};
use openlegal_domain::legal::{Capture, DatabaseError, Dataset, LegalRecord, ObjectId};
use openlegal_server::{
    config::{DatabaseConfig, IngestionConfig},
    corpus_runtime::{CorpusRuntime, rebuild_corpus_index},
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

struct FixtureClock;
impl openlegal_application::Clock for FixtureClock {
    fn now(&self) -> u64 {
        0
    }
}

fn config(fixture: &postgres::TestDatabase, destination: &str) -> DatabaseConfig {
    DatabaseConfig {
        blob_path: fixture.directory.path().join("corpus-blobs"),
        index_path: fixture.directory.path().join(destination),
        mecab_dictionary_path: std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY")
            .expect("PostgreSQL gate must provide the pinned dictionary")
            .into(),
        widget_html: "unused.html".into(),
        ingestion: None,
    }
}
async fn publish(store: &PgCorpusStore, id: &str, revision: &str, time: u64) -> Capture {
    let object = ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional".into(),
        dataset: Dataset::NationalStatute,
        id: id.into(),
    };
    store
        .publish(
            Publication {
                expected_version: store.state(&object).await.unwrap().version,
                record: LegalRecord {
                    object,
                    revision_id: revision.into(),
                    title: "Fictional 대한민국".into(),
                    body: "법률 fictional body".into(),
                    metadata: BTreeMap::new(),
                    publication_date: None,
                    effective_date: None,
                    source_url: "https://example.test/fictional".into(),
                    representation: "fictional".into(),
                    sections: vec![],
                },
                raw: revision.as_bytes().to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: time,
                now: time,
                install_head: true,
                job_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn offline_rebuild_preserves_ack_retirement_withdrawal_and_previous_index() {
    let fixture = postgres::TestDatabase::new().await;
    let persistent = fixture.open(100).await;
    let original = config(&fixture, "old-index");
    let blobs = FsBlobStore::open(&original.blob_path).await.unwrap();
    let store = PgCorpusStore::with_publication_clock(
        persistent.pool(),
        blobs.clone(),
        std::sync::Arc::new(FixtureClock),
    );
    let retired = publish(&store, "one", "r1", 100).await;
    let current = publish(&store, "one", "r2", 200).await;
    let withdrawn = publish(&store, "two", "r1", 300).await;
    store
        .withdraw(&withdrawn.record.object, 1, 400)
        .await
        .unwrap();
    assert_eq!(
        rebuild_corpus_index(&original, &persistent, CancellationToken::new())
            .await
            .unwrap(),
        4
    );
    let analyzer = KoreanAnalyzer::open(&original.mecab_dictionary_path).unwrap();
    let old_index = CorpusIndex::open(&original.index_path, analyzer.clone()).unwrap();
    assert_eq!(store.maintain(3_000_000, 250).await.unwrap(), 0);
    let removal = store.watermark().await.unwrap();
    old_index
        .remove_capture(&retired.record.object, &retired.capture_id, removal)
        .unwrap();
    store.acknowledge_index(removal).await.unwrap();
    assert_eq!(store.acknowledged_index().await.unwrap(), removal);
    let missing = config(&fixture, "missing-serving-index");
    let startup = CorpusRuntime::open(&missing, &persistent).await;
    assert!(startup.is_err());
    assert!(
        startup
            .err()
            .unwrap()
            .to_string()
            .contains("--rebuild-corpus-index")
    );
    assert_eq!(store.maintain(3_000_001, 250).await.unwrap(), 1);
    drop(old_index);
    let mut rebuilt_config = config(&fixture, "new-index");
    // Rebuilding must not initialize provider credentials or document workers,
    // even when normal serving would enable them with this configuration.
    rebuilt_config.ingestion = Some(IngestionConfig {
        credential_env: "OPENLEGAL_UNUSED_REBUILD_FIXTURE_CREDENTIAL".into(),
        kubectl: "/nonexistent-rebuild-fixture/kubectl".into(),
        kubeconfig: "/nonexistent-rebuild-fixture/kubeconfig".into(),
        context: "fixture".into(),
        namespace: "fixture".into(),
        worker_image: "fixture@sha256:unused".into(),
        enabled: true,
        retain_history_bodies: false,
    });
    let lease = store.acquire_runtime_lease().await.unwrap();
    assert!(
        rebuild_corpus_index(&rebuilt_config, &persistent, CancellationToken::new())
            .await
            .is_err()
    );
    assert!(!rebuilt_config.index_path.exists());
    lease.close().await.unwrap();
    assert_eq!(
        rebuild_corpus_index(&rebuilt_config, &persistent, CancellationToken::new())
            .await
            .unwrap(),
        removal
    );
    let rebuilt = CorpusIndex::open(&rebuilt_config.index_path, analyzer.clone()).unwrap();
    let rows = rebuilt
        .snapshot()
        .unwrap()
        .batch(
            "",
            100,
            Instant::now() + Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.capture.capture_id, current.capture_id);
    assert!(rows[0].1.current);
    let preserved = CorpusIndex::open(&original.index_path, analyzer).unwrap();
    assert_eq!(preserved.snapshot().unwrap().generation, removal);
    assert!(
        rebuild_corpus_index(&rebuilt_config, &persistent, CancellationToken::new())
            .await
            .is_err()
    );
    store.acknowledge_index(removal).await.unwrap();
    blobs.close().await.unwrap();
    persistent.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn missing_evidence_leaves_rebuild_unservable_and_old_index_intact() {
    let fixture = postgres::TestDatabase::new().await;
    let persistent = fixture.open(100).await;
    let original = config(&fixture, "old-index");
    let blobs = FsBlobStore::open(&original.blob_path).await.unwrap();
    let store = PgCorpusStore::with_publication_clock(
        persistent.pool(),
        blobs.clone(),
        std::sync::Arc::new(FixtureClock),
    );
    publish(&store, "one", "r1", 100).await;
    rebuild_corpus_index(&original, &persistent, CancellationToken::new())
        .await
        .unwrap();
    blobs.close().await.unwrap();
    std::fs::remove_dir_all(&original.blob_path).unwrap();
    let target = config(&fixture, "broken-index");
    let error = rebuild_corpus_index(&target, &persistent, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<DatabaseError>(),
        Some(&DatabaseError::StorageCorrupt)
    );
    let analyzer = KoreanAnalyzer::open(&original.mecab_dictionary_path).unwrap();
    assert!(CorpusIndex::open(&target.index_path, analyzer.clone()).is_err());
    assert_eq!(
        CorpusIndex::open(&original.index_path, analyzer)
            .unwrap()
            .snapshot()
            .unwrap()
            .generation,
        1
    );
    store.acknowledge_index(1).await.unwrap();
    assert_eq!(store.acknowledged_index().await.unwrap(), 1);
    let cancelled = config(&fixture, "cancelled-index");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = rebuild_corpus_index(&cancelled, &persistent, cancel)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<DatabaseError>(),
        Some(&DatabaseError::Cancelled)
    );
    assert!(!cancelled.index_path.exists());
    persistent.close().await.unwrap();
}
