//! Finite inventories are preserved as source evidence, never legal captures.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{PgCorpusStore, SourceObservationInput},
};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::{legal::DatabaseError, rights::SourceRights};
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

fn input(raw: Option<&[u8]>, observed_at: u64) -> SourceObservationInput {
    SourceObservationInput {
        source_key: "law_go_kr:lsEfYdListGuide:current.page.1".into(),
        raw: raw.map(<[u8]>::to_vec),
        media_type: "application/xml".into(),
        rights: SourceRights::legal_information(),
        metadata: BTreeMap::from([("page".into(), "1".into())]),
        observed_at,
    }
}
fn cancel() -> CancellationToken {
    CancellationToken::new()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corrected_sources_remain_exact_permanent_and_dedup_does_not_charge_again() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("source-observations"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let first = store
        .retain_source_observation(input(Some(b"<list>original</list>"), 100), cancel())
        .await
        .unwrap();
    let second = store
        .retain_source_observation(input(Some(b"<list>corrected</list>"), 200), cancel())
        .await
        .unwrap();
    assert_ne!(first.observation_id, second.observation_id);
    let duplicate = store
        .retain_source_observation(input(Some(b"<list>original</list>"), 300), cancel())
        .await
        .unwrap();
    assert_eq!(duplicate.observation_id, first.observation_id);
    assert_eq!(duplicate.observed_at, 100);
    assert_eq!(duplicate.validated_at, 300);
    let accounting: (i64,i64,i64) = sqlx::query_as("SELECT raw_bytes,staged_bytes,(SELECT count(*) FROM openlegal.corpus_source_observation) FROM openlegal.corpus_control WHERE singleton")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(
        accounting,
        (
            (b"<list>original</list>".len() + b"<list>corrected</list>".len()) as i64,
            0,
            2
        )
    );
    // Simulate an erroneous legacy deletion queue entry. Reference protection
    // removes it before any BlobStore deletion takes place.
    sqlx::query("INSERT INTO openlegal.corpus_blob_deletion SELECT storage_key,raw_sha256,raw_size FROM openlegal.corpus_source_observation WHERE id=$1")
        .bind(&first.observation_id).execute(&pool).await.unwrap();
    store.maintain(90 * 86400, 90 * 86400).await.unwrap();
    assert_eq!(
        store
            .source_observation_bytes(&first.observation_id, cancel())
            .await
            .unwrap(),
        b"<list>original</list>"
    );
    assert_eq!(
        store
            .source_observation_bytes(&second.observation_id, cancel())
            .await
            .unwrap(),
        b"<list>corrected</list>"
    );
    let captures: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_capture")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(captures, 0);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn source_archive_shares_capacity_without_removing_previous_evidence() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("source-capacity"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.configure_archive_capacity(Some(4)).await.unwrap();
    let first = store
        .retain_source_observation(input(Some(b"four"), 100), cancel())
        .await
        .unwrap();
    assert_eq!(
        store
            .retain_source_observation(input(Some(b"five!"), 200), cancel())
            .await,
        Err(DatabaseError::Capacity)
    );
    assert_eq!(
        store
            .source_observation_bytes(&first.observation_id, cancel())
            .await
            .unwrap(),
        b"four"
    );
    let accounting: (i64, i64) = sqlx::query_as(
        "SELECT raw_bytes,staged_bytes FROM openlegal.corpus_control WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(accounting, (4, 0));
    store.configure_archive_capacity(None).await.unwrap();
    store
        .retain_source_observation(input(Some(b"five!"), 200), cancel())
        .await
        .unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn unverified_material_is_metadata_only_and_source_keys_are_closed() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("source-rights"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let mut unverified = input(Some(b"restricted"), 100);
    unverified.rights = SourceRights::default();
    assert_eq!(
        store
            .retain_source_observation(unverified.clone(), cancel())
            .await,
        Err(DatabaseError::InvalidInput)
    );
    unverified.raw = None;
    let metadata = store
        .retain_source_observation(unverified, cancel())
        .await
        .unwrap();
    assert!(!metadata.retained());
    assert_eq!(
        store
            .source_observation_bytes(&metadata.observation_id, cancel())
            .await,
        Err(DatabaseError::RevisionUnavailable)
    );
    for key in [
        "https://www.law.go.kr/DRF/lawSearch.do?OC=sample",
        "law_go_kr:inventedGuide:page.1",
        "law_go_kr:lsEfYdListGuide:",
    ] {
        let mut invalid = input(None, 200);
        invalid.source_key = key.into();
        assert_eq!(
            store.retain_source_observation(invalid, cancel()).await,
            Err(DatabaseError::InvalidInput)
        );
    }
    let bytes: i64 =
        sqlx::query_scalar("SELECT raw_bytes FROM openlegal.corpus_control WHERE singleton")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(bytes, 0);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn observation_read_and_duplicate_validation_detect_modified_bytes() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let root = fixture.directory.path().join("source-integrity");
    let blobs = FsBlobStore::open(&root).await.unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let observed = store
        .retain_source_observation(input(Some(b"unaltered"), 100), cancel())
        .await
        .unwrap();
    let key: String = sqlx::query_scalar(
        "SELECT storage_key FROM openlegal.corpus_source_observation WHERE id=$1",
    )
    .bind(&observed.observation_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    tokio::fs::write(root.join(key), b"modified!")
        .await
        .unwrap();
    assert_eq!(
        store
            .retain_source_observation(input(Some(b"unaltered"), 200), cancel())
            .await,
        Err(DatabaseError::StorageCorrupt)
    );
    let validated: String = sqlx::query_scalar(
        "SELECT validated_at::text FROM openlegal.corpus_source_observation WHERE id=$1",
    )
    .bind(&observed.observation_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(validated, "100");
    base.close().await.unwrap();
}
