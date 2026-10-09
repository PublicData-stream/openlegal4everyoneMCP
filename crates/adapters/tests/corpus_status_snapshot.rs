//! Snapshot-consistent status aggregation under corpus growth and concurrent writes.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CloneView, PgCorpusStore, SourceObservationInput, SupplementJobStatus},
    law_go_kr::supplements,
    postgres::PostgresStore,
};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::{
    legal::{DatabaseError, Dataset, ObjectId},
    rights::SourceRights,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

async fn setup() -> (support::TestDatabase, Arc<PostgresStore>, PgCorpusStore) {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("status-snapshot"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    (fixture, base, store)
}

async fn empty_stable_views(store: &PgCorpusStore, pool: &sqlx::PgPool) {
    for view in CloneView::all() {
        store.clone_cursor(view).await.unwrap();
    }
    sqlx::query("UPDATE openlegal.provider_clone_view SET stable_cycles=2,cycle=3")
        .execute(pool)
        .await
        .unwrap();
}

fn statute_view(historical: bool) -> CloneView {
    CloneView {
        dataset: Dataset::NationalStatute,
        historical,
        treaty_class: None,
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn status_counts_twenty_thousand_bodies_without_job_or_history_join_duplicates() {
    let (_fixture, base, store) = setup().await;
    let pool = base.pool();
    empty_stable_views(&store, &pool).await;
    // Synthetic retained rows exercise status aggregation independently of the
    // publication engine. Their keys and payloads are not legal source evidence.
    sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) SELECT repeat(md5(n::text),2),'{}'::jsonb FROM generate_series(1,20000) n")
        .execute(&pool).await.unwrap();
    sqlx::query(r#"INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key)
        SELECT repeat(md5('capture:'||n),2),repeat(md5(n::text),2),'r1',1,100,n,
        jsonb_build_object('record',jsonb_build_object('body',repeat('synthetic retained body ',400),'metadata',
            jsonb_build_object('attachment_status',CASE WHEN n%11=0 THEN 'incomplete' ELSE 'complete' END,
            'body_status',CASE WHEN n%13=0 THEN 'response_identity_unverified_metadata_only' ELSE 'complete' END))),
        decode(repeat('00',32),'hex'),decode(repeat('00',32),'hex'),0,'fixture:'||n FROM generate_series(1,20000) n"#)
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,latest_capture,last_sequence,captured_at) SELECT object_key,revision_id,id,sequence,captured_at FROM openlegal.corpus_capture")
        .execute(&pool).await.unwrap();
    for historical in [false, true] {
        sqlx::query("INSERT INTO openlegal.provider_clone_member(view_key,object_key,revision_id,seen_cycle,required_body) SELECT $1,repeat(md5(n::text),2),'r1',CASE WHEN n%7=0 THEN 1 ELSE 2 END,true FROM generate_series(1,20000) n WHERE NOT $2 OR n%2=0")
            .bind(statute_view(historical).key().unwrap()).bind(historical)
            .execute(&pool).await.unwrap();
    }
    // Different effective-date jobs for one object/revision must still count
    // one missing body. Failure in an obsolete member must not count at all.
    sqlx::query("INSERT INTO openlegal.corpus_job(object_key,revision_id,effective_date,expected_version,status,created_at) SELECT repeat(md5(n::text),2),'r1',d,0,'failed',100 FROM generate_series(1,20000) n CROSS JOIN (VALUES('date-a'),('date-b')) dates(d) WHERE n%17=0")
        .execute(&pool).await.unwrap();
    for request in supplements::global_requests(1).unwrap() {
        store.enqueue_supplement(&request, 100).await.unwrap();
    }
    let progress = store.clone_progress().await.unwrap();
    for historical in [false, true] {
        let key = statute_view(historical).key().unwrap();
        let view = progress["views"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["view"] == key)
            .unwrap();
        let relevant = |n: &u32| !n.is_multiple_of(7) && (!historical || n.is_multiple_of(2));
        let observed = (1_u32..=20000).filter(relevant).count();
        let missing = (1_u32..=20000)
            .filter(relevant)
            .filter(|n| n.is_multiple_of(11) || n.is_multiple_of(13) || n.is_multiple_of(17))
            .count();
        assert_eq!(view["observed"], observed);
        assert_eq!(view["missing_bodies"], missing);
    }
    assert_eq!(
        progress["supplementary"],
        store.supplement_progress().await.unwrap()
    );
    assert_eq!(progress["active_jobs"], 0);
    assert_eq!(progress["initial_canonical_clone_complete"], false);
    assert_eq!(progress["full_available_clone_complete"], false);
    assert_eq!(progress["atomic_upstream_snapshot"], false);
    sqlx::query("DELETE FROM openlegal.provider_clone_member WHERE view_key=$1")
        .bind(statute_view(false).key().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM openlegal.provider_clone_view WHERE view_key=$1")
        .bind(statute_view(false).key().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false,
        "a missing expected view never establishes completion"
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn status_watermark_and_active_jobs_share_one_snapshot_during_atomic_updates() {
    let (_fixture, base, store) = setup().await;
    let pool = base.pool();
    empty_stable_views(&store, &pool).await;
    sqlx::query(
        "INSERT INTO openlegal.corpus_object(object_key,identity) VALUES(repeat('a',64),'{}')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_job(object_key,revision_id,expected_version,status,created_at) VALUES(repeat('a',64),'r1',0,'done',100)")
        .execute(&pool).await.unwrap();
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        for n in 0..100 {
            let pending = n % 2 == 0;
            let mut tx = writer_pool.begin().await.unwrap();
            sqlx::query("UPDATE openlegal.corpus_control SET next_event=2,index_ack=$1")
                .bind(if pending { 0_i64 } else { 1_i64 })
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE openlegal.corpus_job SET status=$1")
                .bind(if pending { "pending" } else { "done" })
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    for _ in 0..100 {
        let progress = store.clone_progress().await.unwrap();
        let active = progress["active_jobs"].as_u64().unwrap();
        let ready = progress["index_ready"].as_bool().unwrap();
        assert_eq!(
            ready,
            active == 0,
            "one statement cannot mix committed epochs"
        );
        assert_eq!(progress["initial_canonical_clone_complete"], ready);
    }
    writer.await.unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn status_cancellation_interrupts_pool_wait_and_pre_cancelled_reads() {
    let (_fixture, base, store) = setup().await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        store.clone_progress_cancellable(&cancelled).await,
        Err(DatabaseError::Cancelled)
    );
    let pool = base.pool();
    let mut held = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        held.push(pool.acquire().await.unwrap());
    }
    let cancel = CancellationToken::new();
    let request_cancel = cancel.clone();
    let task = tokio::spawn(async move { store.clone_progress_cancellable(&request_cancel).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(250), task)
            .await
            .unwrap()
            .unwrap(),
        Err(DatabaseError::Cancelled)
    );
    drop(held);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn status_retries_server_rejected_read_without_reporting_empty_completion() {
    let (_fixture, base, store) = setup().await;
    let pool = base.pool();
    empty_stable_views(&store, &pool).await;
    let mut blocked = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE openlegal.provider_clone_member IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocked)
        .await
        .unwrap();
    let task = tokio::spawn(async move { store.clone_progress().await });
    tokio::time::sleep(Duration::from_millis(1200)).await;
    blocked.rollback().await.unwrap();
    let progress = task.await.unwrap().unwrap();
    assert_eq!(progress["initial_canonical_clone_complete"], true);
    assert_eq!(progress["atomic_upstream_snapshot"], false);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn status_supplement_counts_match_global_completion_and_each_outstanding_seed_state() {
    async fn retain(store: &PgCorpusStore, request: &supplements::SupplementRequest) {
        store
            .retain_source_observation(
                SourceObservationInput {
                    source_key: request.observation_key().unwrap(),
                    raw: Some(b"<fixture>synthetic supplementary evidence</fixture>".to_vec()),
                    media_type: "application/xml".into(),
                    rights: SourceRights::legal_information(),
                    metadata: BTreeMap::new(),
                    observed_at: 101,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
    }
    async fn check(store: &PgCorpusStore, complete: bool) -> serde_json::Value {
        let progress = store.clone_progress().await.unwrap();
        assert_eq!(
            progress["supplementary"],
            store.supplement_progress().await.unwrap()
        );
        assert_eq!(progress["supplementary"]["complete"], complete);
        assert_eq!(progress["initial_canonical_clone_complete"], true);
        assert!(progress["unresolved_guides"].as_u64().unwrap() > 0);
        assert_eq!(
            progress["full_available_clone_complete"], false,
            "complete canonical views and supplements do not resolve unverified guides"
        );
        progress
    }
    let (_fixture, base, store) = setup().await;
    let pool = base.pool();
    empty_stable_views(&store, &pool).await;
    check(&store, false).await;
    let globals = supplements::global_requests(1).unwrap();
    for request in &globals {
        store.enqueue_supplement(request, 100).await.unwrap();
        retain(&store, request).await;
    }
    let pending = check(&store, false).await;
    assert_eq!(
        pending["supplementary"]["pending_or_running"],
        globals.len()
    );
    assert_eq!(pending["supplementary"]["completed_global_roots"], 0);
    for index in 0..globals.len() {
        let job = store.claim_supplement(102).await.unwrap().unwrap();
        store
            .settle_supplement(
                &job,
                SupplementJobStatus::Done,
                job.observation_id.as_deref(),
                1,
                103,
            )
            .await
            .unwrap();
        let progress = check(&store, index + 1 == globals.len()).await;
        assert_eq!(
            progress["supplementary"]["completed_global_roots"],
            index + 1
        );
    }
    for id in ["101", "102", "103"] {
        let request = supplements::request(
            supplements::SupplementSource::RelatedStatutes,
            supplements::SupplementSeed::Record {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "law_go_kr".into(),
                    dataset: Dataset::NationalStatute,
                    id: id.into(),
                },
                record_number: id.into(),
            },
            1,
        )
        .unwrap();
        store.enqueue_supplement(&request, 100).await.unwrap();
        retain(&store, &request).await;
    }
    let running = store.claim_supplement(104).await.unwrap().unwrap();
    let deferred = store.claim_supplement(104).await.unwrap().unwrap();
    let incomplete = store.claim_supplement(104).await.unwrap().unwrap();
    store
        .settle_supplement(
            &deferred,
            SupplementJobStatus::Deferred,
            deferred.observation_id.as_deref(),
            0,
            105,
        )
        .await
        .unwrap();
    store
        .settle_supplement(
            &incomplete,
            SupplementJobStatus::Incomplete,
            incomplete.observation_id.as_deref(),
            0,
            105,
        )
        .await
        .unwrap();
    let mixed = check(&store, false).await;
    assert_eq!(
        mixed["supplementary"]["completed_global_roots"],
        globals.len()
    );
    assert_eq!(mixed["supplementary"]["pending_or_running"], 1);
    assert_eq!(mixed["supplementary"]["deferred"], 1);
    assert_eq!(mixed["supplementary"]["incomplete"], 1);
    store
        .settle_supplement(
            &running,
            SupplementJobStatus::Done,
            running.observation_id.as_deref(),
            1,
            106,
        )
        .await
        .unwrap();
    check(&store, false).await;
    // Requeue the synthetic nonterminal fixtures without disabling any schema
    // checks or inventing an observation. Normal claims/settlements below must
    // reuse retained evidence for the same source key and request epoch.
    sqlx::query("UPDATE openlegal.provider_supplement_job SET status='pending',retry_at=NULL,completed_at=NULL WHERE NOT is_global AND status IN ('deferred','incomplete')")
        .execute(&pool).await.unwrap();
    for index in 0..2 {
        let job = store.claim_supplement(107).await.unwrap().unwrap();
        assert!(job.observation_id.is_some());
        store
            .settle_supplement(
                &job,
                SupplementJobStatus::Done,
                job.observation_id.as_deref(),
                1,
                108,
            )
            .await
            .unwrap();
        check(&store, index == 1).await;
    }
    let complete = check(&store, true).await;
    assert_eq!(complete["supplementary"]["done"], globals.len() + 3);
    assert_eq!(
        complete["supplementary"]["completed_global_roots"],
        globals.len()
    );
    assert_eq!(
        complete["supplementary"]["required_global_roots"],
        globals.len()
    );
    base.close().await.unwrap();
}
