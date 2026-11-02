//! Disposable PostgreSQL metadata/ACL tests. All evidence is synthetic;
//! no provider, physical Blob copy, public MCP or six-hour acceptance is implied.
#[path = "../../../test-support/postgres.rs"]
#[allow(dead_code, reason = "shared fixture exposes other suites' helpers")]
mod support;

use openlegal_domain::{
    legal::{Capture, Dataset, GetResult, LegalRecord, MetadataResult, ObjectId},
    rights::{OriginalResource, SourceRights},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

const KINDS: [&str; 15] = [
    "corpus_capture",
    "corpus_capture_catalog",
    "corpus_capture_blob",
    "corpus_source_observation",
    "corpus_object_identity",
    "corpus_revision_identity",
    "cache_snapshot",
    "cache_query_identity",
    "cache_blob_identity",
    "current_inventory_audit",
    "provider_recovery_audit",
    "provider_canary_audit",
    "checkpoint_operation",
    "canary_lifecycle",
    "provider_canary",
];
const PRIVATE: &str = "PRIVATE_QUERY_SENTINEL_NOT_FOR_METADATA";
const BODY: &str = "SYNTHETIC_LEGAL_BODY_NOT_FOR_METADATA";
static NEXT: AtomicUsize = AtomicUsize::new(1);

fn hex(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash(value: &Value) -> Vec<u8> {
    Sha256::digest(serde_json::to_vec(value).unwrap()).to_vec()
}
async fn id(admin: &mut PgConnection) -> Uuid {
    sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(admin)
        .await
        .unwrap()
}
async fn page(actor: &mut PgConnection, kind: &str, after: Value, limit: i32) -> Value {
    sqlx::query_scalar("SELECT openlegal_checkpoint.preservation_page($1,$2,$3)")
        .bind(kind)
        .bind(after)
        .bind(limit)
        .fetch_one(actor)
        .await
        .unwrap()
}
async fn publications(
    actor: &mut PgConnection,
    event: i64,
    after: Option<&str>,
    limit: i32,
) -> Value {
    sqlx::query_scalar("SELECT openlegal_checkpoint.publication_page($1,$2,$3)")
        .bind(event)
        .bind(after)
        .bind(limit)
        .fetch_one(actor)
        .await
        .unwrap()
}

struct Fixture {
    database: support::TestDatabase,
    admin: PgConnection,
    actor: PgConnection,
    owner: String,
    object_key: String,
    observation: String,
    snapshot: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let database = support::TestDatabase::new().await;
        let mut admin = PgConnection::connect(&database.url).await.unwrap();
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let owner = format!("preservation_owner_{suffix}");
        let login = format!("preservation_login_{suffix}");
        let password = id(&mut admin).await.to_string();
        // These identifiers/passwords are exclusively generated fixture values.
        assert!(
            owner
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        );
        assert!(
            login
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {owner} NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
             CREATE ROLE {login} LOGIN PASSWORD '{password}' NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
             GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_checkpoint TO {owner};
             GRANT SELECT ON ALL TABLES IN SCHEMA openlegal,openlegal_admin,openlegal_checkpoint TO {owner};
             GRANT CREATE ON SCHEMA openlegal_checkpoint TO {owner};
             ALTER FUNCTION openlegal_checkpoint.preservation_page(text,jsonb,integer) OWNER TO {owner};
             ALTER FUNCTION openlegal_checkpoint.publication_page(bigint,text,integer) OWNER TO {owner};
             REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM {owner};
             GRANT USAGE ON SCHEMA openlegal_checkpoint TO {login};
             GRANT EXECUTE ON FUNCTION openlegal_checkpoint.preservation_page(text,jsonb,integer),
                 openlegal_checkpoint.publication_page(bigint,text,integer) TO {login};"
        ))).execute(&mut admin).await.unwrap();
        let options = PgConnectOptions::from_str(&database.url)
            .unwrap()
            .username(&login)
            .password(&password);
        let actor = PgConnection::connect_with(&options).await.unwrap();
        let object = ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::NationalStatute,
            id: "synthetic-record".into(),
        };
        let object_key = hex(&Sha256::digest(serde_json::to_vec(&object).unwrap()));
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) VALUES($1,$2)")
            .bind(&object_key)
            .bind(serde_json::to_value(&object).unwrap())
            .execute(&mut admin)
            .await
            .unwrap();
        let observation = "e".repeat(64);
        sqlx::query("INSERT INTO openlegal.corpus_source_observation(id,source_key,raw_size,media_type,rights,metadata,observed_at,validated_at,manifest_sha256)
            VALUES($1,'law_go_kr:fixture:1',0,'application/json','{}',$2,1,1,$3)")
            .bind(&observation).bind(json!({"private":PRIVATE})).bind(vec![1u8;32]).execute(&mut admin).await.unwrap();
        let recovery = id(&mut admin).await;
        sqlx::query("INSERT INTO openlegal_admin.provider_recovery_audit(operation_id,plan,actor,database_actor,actual_writers_stopped_at,applied_at,before_state,result)
            VALUES($1,$2,'synthetic',session_user,1,1,'{}','{}')")
            .bind(recovery).bind(json!({"private":PRIVATE})).execute(&mut admin).await.unwrap();
        let canary = id(&mut admin).await;
        sqlx::query("INSERT INTO openlegal_admin.provider_canary(id,operation_id,started_at,deadline,last_observed_at)
            VALUES($1,$2,now(),now()+interval '6 hours',now())")
            .bind(canary).bind(recovery).execute(&mut admin).await.unwrap();
        sqlx::query("INSERT INTO openlegal_admin.provider_canary_audit(operation_id,canary_id,action,result) VALUES($1,$2,'init',$3)")
            .bind(recovery).bind(canary).bind(json!({"private":PRIVATE})).execute(&mut admin).await.unwrap();
        let checkpoint = id(&mut admin).await;
        sqlx::query("INSERT INTO openlegal_checkpoint.operation(operation_id,intent_sha256,owned,original_generation,installed_generation,clean_state,hold_result)
            VALUES($1,$2,true,$3,$4,'{}',$5)")
            .bind(checkpoint).bind("1".repeat(64)).bind(id(&mut admin).await).bind(id(&mut admin).await)
            .bind(json!({"private":PRIVATE})).execute(&mut admin).await.unwrap();
        sqlx::query("INSERT INTO openlegal_checkpoint.canary_lifecycle(canary_id,recovery_operation_id,intent_sha256,managed_epoch,blob_binding_sha256,
             original_hold_generation,released_generation,activated_at,activation_result)
            VALUES($1,$2,$3,$4,$5,$6,$7,now(),$8)")
            .bind(canary).bind(recovery).bind("2".repeat(64)).bind(id(&mut admin).await).bind("3".repeat(64))
            .bind(id(&mut admin).await).bind(id(&mut admin).await).bind(json!({"private":PRIVATE}))
            .execute(&mut admin).await.unwrap();
        let inventory_audit = id(&mut admin).await;
        sqlx::query("INSERT INTO openlegal.current_inventory_audit(operation_id,manifest_sha256,manifest,applied_at,before_state,after_state)
             VALUES($1,$2,$3,1,'{}','{}')")
            .bind(inventory_audit).bind(vec![4u8;32]).bind(json!({"private":PRIVATE})).execute(&mut admin).await.unwrap();
        let query: Uuid = sqlx::query_scalar("INSERT INTO openlegal.cache_query(namespace,provider,dataset,query_hash,canonical_identity,query,created_at)
            VALUES('fixture','fixture','fixture',$1,$2,$3,1) RETURNING id")
            .bind(vec![5u8;32]).bind(PRIVATE.as_bytes()).bind(json!({"private":PRIVATE})).fetch_one(&mut admin).await.unwrap();
        let cache_key = format!("{}/{}-{}", "06", "06".repeat(32), id(&mut admin).await);
        sqlx::query("INSERT INTO openlegal.blob_object(sha256,size_bytes,storage_key,ready,created_at) VALUES($1,1,$2,true,1)")
            .bind(vec![6u8;32]).bind(cache_key).execute(&mut admin).await.unwrap();
        let snapshot: Uuid = sqlx::query_scalar("INSERT INTO openlegal.cache_snapshot(public_id,query_id,sequence,captured_at,retrieved_at,original_validated_at,
            processor_version,schema_version,raw_blob_sha256,processed_sha256,processed_data,source_reference,envelope_sha256)
            VALUES($1,$2,1,1,1,1,'fixture',1,$3,$4,$5,$6,$7) RETURNING id")
            .bind("7".repeat(64)).bind(query).bind(vec![6u8;32]).bind(vec![8u8;32]).bind(json!({"body":BODY}))
            .bind(format!("https://example.test/source?opaque={PRIVATE}")).bind(vec![9u8;32]).fetch_one(&mut admin).await.unwrap();
        let mut result = Self {
            database,
            admin,
            actor,
            owner,
            object_key,
            observation,
            snapshot,
        };
        result.capture(1).await;
        result.capture(2).await;
        result
    }
    async fn capture(&mut self, sequence: i64) {
        self.capture_revision(sequence, format!("revision-{sequence}"))
            .await;
    }
    async fn capture_revision(&mut self, sequence: i64, revision: String) {
        let capture_id = format!("{sequence:064x}");
        let object: ObjectId = serde_json::from_value(
            sqlx::query_scalar::<_, Value>(
                "SELECT identity FROM openlegal.corpus_object WHERE object_key=$1",
            )
            .bind(&self.object_key)
            .fetch_one(&mut self.admin)
            .await
            .unwrap(),
        )
        .unwrap();
        let mut rights = SourceRights::legal_information();
        rights.evidence_url = format!("https://example.test/license?opaque={PRIVATE}");
        let resources = vec![OriginalResource {
            ordinal: 0,
            title: "synthetic".into(),
            media_type: "application/xml".into(),
            source_url: format!("https://example.test/source?opaque={PRIVATE}"),
            retained: true,
            rights,
        }];
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "original_resources".into(),
            serde_json::to_string(&resources).unwrap(),
        );
        let raw = b"synthetic-original";
        let raw_sha = Sha256::digest(raw).to_vec();
        let raw_hex = hex(&raw_sha);
        let capture = Capture {
            capture_id: capture_id.clone(),
            sequence: sequence as u64,
            record: LegalRecord {
                object,
                revision_id: revision,
                title: "synthetic".into(),
                body: BODY.into(),
                metadata,
                publication_date: None,
                effective_date: None,
                source_url: "https://example.test/source".into(),
                representation: "text".into(),
                sections: vec![],
            },
            retrieved_at: 10,
            captured_at: 10,
            validated_at: 10,
            processor_version: "synthetic".into(),
            raw_sha256: raw_hex.clone(),
        };
        capture.record.validate().unwrap();
        let catalog: MetadataResult = GetResult {
            capture: capture.clone(),
            freshness: None,
        }
        .into();
        let payload = serde_json::to_value(&capture).unwrap();
        let metadata = serde_json::to_value(catalog).unwrap();
        let storage = format!(
            "{}/{}-{}",
            &raw_hex[..2],
            raw_hex,
            id(&mut self.admin).await
        );
        sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key)
            VALUES($1,$2,$3,$4,10,$4,$5,$6,$7,$8,$9)")
            .bind(&capture_id).bind(&self.object_key).bind(&capture.record.revision_id).bind(sequence).bind(&payload)
            .bind(hash(&payload)).bind(&raw_sha).bind(raw.len() as i64).bind(storage).execute(&mut self.admin).await.unwrap();
        sqlx::query("INSERT INTO openlegal.corpus_capture_catalog(id,object_key,revision_id,sequence,captured_at,payload,payload_sha256)
            VALUES($1,$2,$3,$4,10,$5,$6)")
            .bind(&capture_id).bind(&self.object_key).bind(&capture.record.revision_id).bind(sequence).bind(&metadata).bind(hash(&metadata))
            .execute(&mut self.admin).await.unwrap();
        let extra = format!(
            "{}/{}-{}",
            &raw_hex[..2],
            raw_hex,
            id(&mut self.admin).await
        );
        sqlx::query("INSERT INTO openlegal.corpus_capture_blob(capture_id,ordinal,raw_sha256,raw_size,storage_key) VALUES($1,1,$2,$3,$4)")
            .bind(&capture_id).bind(&raw_sha).bind(raw.len() as i64).bind(extra).execute(&mut self.admin).await.unwrap();
        sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,latest_capture,last_sequence,captured_at,last_validated_at)
            VALUES($1,$2,$3,$4,10,10)")
            .bind(&self.object_key).bind(&capture.record.revision_id).bind(&capture_id).bind(sequence).execute(&mut self.admin).await.unwrap();
        sqlx::query("UPDATE openlegal.corpus_object SET head_capture=$2,next_capture=$3,version=$3 WHERE object_key=$1")
            .bind(&self.object_key).bind(&capture_id).bind(sequence+1).execute(&mut self.admin).await.unwrap();
    }
    async fn close(self) {
        self.actor.close().await.unwrap();
        self.admin.close().await.unwrap();
        drop(self.database);
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and registered M32"]
async fn preservation_all_fixed_kinds_are_private_bounded_typed_metadata() {
    let mut f = Fixture::new().await;
    assert!(
        sqlx::query("SELECT set_config('role',$1,false)")
            .bind(&f.owner)
            .execute(&mut f.actor)
            .await
            .is_err()
    );
    for query in [
        "SELECT * FROM openlegal.corpus_capture",
        "UPDATE openlegal_admin.provider_control SET recovery_hold=false",
    ] {
        assert!(sqlx::query(query).execute(&mut f.actor).await.is_err());
    }
    let revision_urls = [
        format!("https://example.test/revision?opaque={PRIVATE}"),
        "가나\"quoted\\revision".into(),
    ];
    for (index, revision) in revision_urls.iter().enumerate() {
        sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,last_sequence) VALUES($1,$2,$3)")
            .bind(&f.object_key).bind(revision).bind(10+i64::try_from(index).unwrap()).execute(&mut f.admin).await.unwrap();
    }
    for ordinal in [10i32, 2] {
        sqlx::query("INSERT INTO openlegal.corpus_capture_blob(capture_id,ordinal,raw_sha256,raw_size,storage_key) SELECT capture_id,$1,raw_sha256,raw_size,left(storage_key,length(storage_key)-36)||pg_catalog.uuidv7()::text FROM openlegal.corpus_capture_blob WHERE ordinal=1 ORDER BY capture_id LIMIT 1")
            .bind(ordinal).execute(&mut f.admin).await.unwrap();
    }
    let mut blob_ordinals = Vec::new();
    let mut revision_keys = Vec::new();
    for kind in KINDS {
        let mut after = Value::Null;
        let mut keys = std::collections::BTreeSet::new();
        let mut pages = 0;
        loop {
            let result = page(&mut f.actor, kind, after, 1).await;
            pages += 1;
            assert!(pages <= 4, "finite synthetic fixture: {kind}");
            assert_eq!(result["schema_version"], 1);
            assert_eq!(result["relation"], kind);
            assert_eq!(result["rows"].as_array().unwrap().len(), 1, "{kind}");
            let row = &result["rows"][0];
            assert!(row["key"].as_array().unwrap().iter().all(Value::is_string));
            assert!(keys.insert(serde_json::to_string(&row["key"]).unwrap()));
            if kind == "corpus_capture_blob" {
                blob_ordinals.push(row["key"][1].as_str().unwrap().to_owned());
            }
            if kind == "corpus_revision_identity" {
                revision_keys.push(row["key"][1].as_str().unwrap().to_owned());
            }
            assert_eq!(row["sha256"].as_str().unwrap().len(), 64);
            let text = serde_json::to_string(&result).unwrap();
            assert!(!text.contains(PRIVATE));
            assert!(!text.contains(BODY));
            assert!(text.len() <= 262144);
            if result["has_more"] == true {
                assert_eq!(result["next_key"], row["key"]);
                after = result["next_key"].clone();
            } else {
                assert!(result["next_key"].is_null());
                break;
            }
        }
    }
    assert_eq!(blob_ordinals, vec!["1", "2", "10", "1"]);
    let mut ordered = revision_keys.clone();
    ordered.sort();
    assert_eq!(revision_keys, ordered);
    for revision in revision_urls {
        assert!(revision_keys.contains(&hex(&Sha256::digest(revision.as_bytes()))));
    }
    for (kind, after, limit) in [
        ("openlegal.corpus_capture;DROP", Value::Null, 1),
        ("corpus_capture", json!({"id":"x"}), 1),
        ("corpus_capture", json!([true]), 1),
        ("corpus_capture", json!([]), 1),
        ("corpus_capture", Value::Null, 0),
        ("corpus_capture", Value::Null, 129),
    ] {
        assert!(
            sqlx::query("SELECT openlegal_checkpoint.preservation_page($1,$2,$3)")
                .bind(kind)
                .bind(after)
                .bind(limit)
                .execute(&mut f.actor)
                .await
                .is_err()
        );
    }
    let bad_key = format!("https://example.test/catalog?opaque={PRIVATE}");
    sqlx::query("INSERT INTO openlegal.corpus_capture_catalog(id,object_key,revision_id,sequence,captured_at,payload,payload_sha256) VALUES($1,$2,'synthetic-invalid-key',99,1,'{}',$3)")
        .bind(bad_key).bind(&f.object_key).bind(vec![1u8;32]).execute(&mut f.admin).await.unwrap();
    let error = sqlx::query_scalar::<_, Value>(
        "SELECT openlegal_checkpoint.preservation_page('corpus_capture_catalog',NULL,128)",
    )
    .fetch_one(&mut f.actor)
    .await
    .unwrap_err();
    assert!(
        !format!("{error:?}").contains(PRIVATE),
        "invalid stored keys must not leak through result/error"
    );
    f.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and registered M32"]
async fn preservation_append_keeps_subset_but_loss_identity_change_and_validation_regression_do_not()
 {
    let mut f = Fixture::new().await;
    let before = page(&mut f.actor, "corpus_capture", Value::Null, 128).await;
    f.capture(3).await;
    let after = page(&mut f.actor, "corpus_capture", Value::Null, 128).await;
    assert_eq!(after["rows"].as_array().unwrap().len(), 3);
    for row in before["rows"].as_array().unwrap() {
        assert!(after["rows"].as_array().unwrap().contains(row));
    }
    let baseline = page(&mut f.actor, "cache_snapshot", Value::Null, 128).await;
    sqlx::query("DELETE FROM openlegal.cache_snapshot WHERE id=$1")
        .bind(f.snapshot)
        .execute(&mut f.admin)
        .await
        .unwrap();
    let deleted = page(&mut f.actor, "cache_snapshot", Value::Null, 128).await;
    assert_eq!(baseline["rows"].as_array().unwrap().len(), 1);
    assert!(deleted["rows"].as_array().unwrap().is_empty());
    // All ready Blob identities remain protected even after their last reference.
    assert_eq!(
        page(&mut f.actor, "cache_blob_identity", Value::Null, 128).await["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    sqlx::query("DELETE FROM openlegal.blob_object WHERE ready")
        .execute(&mut f.admin)
        .await
        .unwrap();
    assert!(
        page(&mut f.actor, "cache_blob_identity", Value::Null, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let audit = page(&mut f.actor, "checkpoint_operation", Value::Null, 128).await;
    sqlx::query("UPDATE openlegal_checkpoint.operation SET released_at=clock_timestamp(),release_result='{}'").execute(&mut f.admin).await.unwrap();
    let audit_changed = page(&mut f.actor, "checkpoint_operation", Value::Null, 128).await;
    assert_ne!(
        audit["rows"][0]["sha256"], audit_changed["rows"][0]["sha256"],
        "baseline nullable audit fields are never waived"
    );
    let old = page(&mut f.actor, "corpus_object_identity", Value::Null, 128).await;
    sqlx::query("UPDATE openlegal.corpus_object SET identity=jsonb_set(identity,'{id}','\"changed\"') WHERE object_key=$1")
        .bind(&f.object_key).execute(&mut f.admin).await.unwrap();
    let changed = page(&mut f.actor, "corpus_object_identity", Value::Null, 128).await;
    assert_ne!(old["rows"][0]["sha256"], changed["rows"][0]["sha256"]);
    let old = page(&mut f.actor, "corpus_source_observation", Value::Null, 128).await;
    sqlx::query("UPDATE openlegal.corpus_source_observation SET validated_at=5 WHERE id=$1")
        .bind(&f.observation)
        .execute(&mut f.admin)
        .await
        .unwrap();
    let advanced = page(&mut f.actor, "corpus_source_observation", Value::Null, 128).await;
    assert_eq!(old["rows"][0]["sha256"], advanced["rows"][0]["sha256"]);
    assert_eq!(advanced["rows"][0]["monotonic"]["validated_at"], "5");
    sqlx::query("UPDATE openlegal.corpus_source_observation SET validated_at=2 WHERE id=$1")
        .bind(&f.observation)
        .execute(&mut f.admin)
        .await
        .unwrap();
    let regressed = page(&mut f.actor, "corpus_source_observation", Value::Null, 128).await;
    assert_eq!(
        regressed["rows"][0]["sha256"],
        advanced["rows"][0]["sha256"]
    );
    assert!(
        regressed["rows"][0]["monotonic"]["validated_at"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            < advanced["rows"][0]["monotonic"]["validated_at"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
    );
    f.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and registered M32"]
async fn publication_is_real_capture_metadata_not_job_done_and_excludes_unusable_rights() {
    let mut f = Fixture::new().await;
    let first = publications(&mut f.actor, 0, None, 1).await;
    assert_eq!(first["rows"].as_array().unwrap().len(), 1);
    assert_eq!(first["has_more"], true);
    let cursor = &first["next_after"];
    let second = publications(
        &mut f.actor,
        cursor["event_sequence"].as_str().unwrap().parse().unwrap(),
        cursor["capture_id"].as_str(),
        1,
    )
    .await;
    assert_ne!(
        first["rows"][0]["capture_id"],
        second["rows"][0]["capture_id"]
    );
    assert_eq!(second["has_more"], false);
    assert!(second["next_after"].is_null());
    let row = &first["rows"][0];
    assert_eq!(row["body_sha256"], hex(&Sha256::digest(BODY.as_bytes())));
    assert_eq!(row["body_bytes"], BODY.len().to_string());
    assert_eq!(row["blob_refs"].as_array().unwrap().len(), 2);
    assert_eq!(row["primary_rights"]["derivatives"], true);
    let serialized = serde_json::to_string(&first).unwrap();
    assert!(!serialized.contains(PRIVATE));
    assert!(!serialized.contains(BODY));
    assert!(!serialized.contains("https://"));
    let private_revision = format!("https://example.test/revision?opaque={PRIVATE}");
    f.capture_revision(3, private_revision.clone()).await;
    let url_candidate = publications(&mut f.actor, 2, None, 1).await;
    assert_eq!(
        url_candidate["rows"][0]["revision_id_sha256"],
        hex(&Sha256::digest(private_revision.as_bytes()))
    );
    assert!(url_candidate["rows"][0].get("revision_id").is_none());
    assert!(
        !serde_json::to_string(&url_candidate)
            .unwrap()
            .contains(PRIVATE)
    );
    let original_resources: String=sqlx::query_scalar("SELECT payload#>>'{record,metadata,original_resources}' FROM openlegal.corpus_capture ORDER BY id LIMIT 1").fetch_one(&mut f.admin).await.unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_job(object_key,revision_id,expected_version,status,created_at,completed_at) VALUES($1,'job-done-without-capture',0,'done',1,2)").bind(&f.object_key).execute(&mut f.admin).await.unwrap();
    // Only this disposable corruption fixture disables the immutable trigger.
    sqlx::raw_sql("ALTER TABLE openlegal.corpus_capture DISABLE TRIGGER immutable_corpus_capture;
      UPDATE openlegal.corpus_capture SET payload=jsonb_set(payload,'{record,metadata,body_status}','\"rights_unverified_metadata_only\"');").execute(&mut f.admin).await.unwrap();
    assert!(
        publications(&mut f.actor, 0, None, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // clone_body_complete deliberately remains true for rights-unverified rows.
    let complete: bool =
        sqlx::query_scalar("SELECT bool_and(clone_body_complete) FROM openlegal.corpus_capture")
            .fetch_one(&mut f.admin)
            .await
            .unwrap();
    assert!(complete);
    sqlx::raw_sql(
        "UPDATE openlegal.corpus_capture SET payload=payload#-'{record,metadata,body_status}';",
    )
    .execute(&mut f.admin)
    .await
    .unwrap();
    assert!(
        !publications(&mut f.actor, 0, None, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    sqlx::query("UPDATE openlegal.corpus_object SET identity=jsonb_set(identity,'{body}',to_jsonb($1::text))")
        .bind(BODY).execute(&mut f.admin).await.unwrap();
    sqlx::query("UPDATE openlegal.corpus_capture SET payload=jsonb_set(payload,'{record,object,body}',to_jsonb($1::text))")
        .bind(BODY).execute(&mut f.admin).await.unwrap();
    assert!(
        publications(&mut f.actor, 0, None, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty(),
        "extra object fields cannot be returned as metadata"
    );
    sqlx::raw_sql(
        "UPDATE openlegal.corpus_object SET identity=identity-'body';
        UPDATE openlegal.corpus_capture SET payload=payload#-'{record,object,body}';",
    )
    .execute(&mut f.admin)
    .await
    .unwrap();
    sqlx::query("UPDATE openlegal.corpus_capture SET payload=jsonb_set(payload,'{record,metadata,original_resources}',to_jsonb($1::text))")
        .bind(format!("malformed-{PRIVATE}")).execute(&mut f.admin).await.unwrap();
    let error =
        sqlx::query_scalar::<_, Value>("SELECT openlegal_checkpoint.publication_page(0,NULL,128)")
            .fetch_one(&mut f.actor)
            .await
            .unwrap_err();
    assert!(!format!("{error:?}").contains(PRIVATE));
    let mut restricted: Value = serde_json::from_str(&original_resources).unwrap();
    restricted[0]["rights"]["derivatives"] = json!(false);
    sqlx::query("UPDATE openlegal.corpus_capture SET payload=jsonb_set(payload#-'{record,metadata,body_status}','{record,metadata,original_resources}',to_jsonb($1::text))")
        .bind(serde_json::to_string(&restricted).unwrap()).execute(&mut f.admin).await.unwrap();
    assert!(
        publications(&mut f.actor, 0, None, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty(),
        "done jobs and nonderivative body rows do not qualify"
    );
    sqlx::query("UPDATE openlegal.corpus_capture SET payload=jsonb_set(payload,'{record,metadata,original_resources}',to_jsonb($1::text))")
        .bind(original_resources).execute(&mut f.admin).await.unwrap();
    sqlx::raw_sql(
        "UPDATE openlegal.corpus_capture SET payload=payload#-'{record,metadata,body_status}';
      UPDATE openlegal.corpus_object SET pending=true,current_inventory_conflict=true;",
    )
    .execute(&mut f.admin)
    .await
    .unwrap();
    assert!(
        publications(&mut f.actor, 0, None, 128).await["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for (event, after, limit) in [(-1, None, 1), (0, Some("not-a-capture"), 1), (0, None, 129)] {
        assert!(
            sqlx::query("SELECT openlegal_checkpoint.publication_page($1,$2,$3)")
                .bind(event)
                .bind(after)
                .bind(limit)
                .execute(&mut f.actor)
                .await
                .is_err()
        );
    }
    f.close().await;
}
