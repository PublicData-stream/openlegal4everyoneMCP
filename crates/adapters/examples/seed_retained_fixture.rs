//! Disposable image-test fixture only; never packaged in the production image.
//! Uses the normal publication API and requires an empty, migrated fixture DB.
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::PgCorpusStore,
    postgres::{PostgresOptions, PostgresStore, PostgresTls, StartupMode},
};
use openlegal_application::{
    Clock, StoredPayload, SystemClock,
    blob::BlobStore,
    database::Publication,
    persistence::{
        HistoryKey, PersistentKey, PersistentStore, PublicationOutcome, PublicationRequest,
        RetentionPolicy, digest_hex,
    },
};
use openlegal_domain::{
    Provenance, Query, Record, RetrievalData,
    legal::{Dataset, LegalRecord, ObjectId},
};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Never print arguments, environment values, URLs or driver diagnostics.
    let result = tokio::time::timeout(Duration::from_secs(60), seed()).await;
    match result {
        Ok(Ok(expected)) => {
            println!("{expected}");
            Ok(())
        }
        _ => Err("retained image fixture seeding failed".into()),
    }
}

async fn seed() -> Result<serde_json::Value, Error> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: seed_retained_fixture CACHE_BLOBS CORPUS_BLOBS CA_FILE".into());
    }
    let cache_path = PathBuf::from(&args[0]);
    let corpus_path = PathBuf::from(&args[1]);
    let ca_path = PathBuf::from(&args[2]);
    if !cache_path.is_absolute()
        || !corpus_path.is_absolute()
        || !ca_path.is_absolute()
        || cache_path.starts_with(&corpus_path)
        || corpus_path.starts_with(&cache_path)
    {
        return Err("fixture requires separate absolute blob roots and an absolute CA path".into());
    }
    let url = std::env::var("OPENLEGAL_DATABASE_URL")?;
    let now = SystemClock::default().now();
    let cache_blobs = FsBlobStore::open(&cache_path).await?;
    let persistent = PostgresStore::open(
        &url,
        PostgresOptions {
            max_connections: 4,
            tls: PostgresTls::VerifyFull {
                ca_file: Some(ca_path),
            },
        },
        cache_blobs,
        RetentionPolicy::default(),
        now,
        StartupMode::Serve,
    )
    .await?;
    let corpus_blobs = FsBlobStore::open(&corpus_path).await?;
    let corpus = PgCorpusStore::new(persistent.pool(), corpus_blobs.clone());
    let lease = corpus.acquire_runtime_lease().await?;
    let outcome = async {
        // Refuse to seed an existing corpus, even when the fictional ID is absent.
        if corpus.watermark().await? != 0 || corpus.acknowledged_index().await? != 0 {
            return Err::<_, Error>("fixture corpus must be empty".into());
        }
        // One expired synthetic cache occurrence lets the real maintenance CLI
        // prove pruning with cache-only mounts before corpus serving begins.
        let captured_at = now - 31 * 86400;
        let key = PersistentKey {
            history: HistoryKey {
                namespace: "retained-image-fixture".into(),
                provider: "synthetic".into(),
                dataset: "records".into(),
                query: Query::Get {
                    source: "fixture".into(),
                    id: "expired".into(),
                },
            },
            processor_version: "fixture-v1".into(),
            schema_version: 1,
        };
        let data = RetrievalData::Get(Record {
            source: "fixture".into(),
            id: "expired".into(),
            title: "Expired fictional cache fixture".into(),
            body: "Expired fictional body".into(),
            synthetic: true,
        });
        let raw = serde_json::to_vec(&data)?;
        let expected = persistent
            .lookup(key.clone(), captured_at, CancellationToken::new())
            .await?
            .observation;
        let published = persistent
            .publish(PublicationRequest {
                key,
                expected,
                now: captured_at,
                authorize: Arc::new(|| true),
                cancellation: CancellationToken::new(),
                value: Arc::new(StoredPayload {
                    bytes: raw.len() + 2048,
                    data,
                    snapshot: None,
                    provenance: Provenance {
                        provider: "synthetic".into(),
                        dataset: "records".into(),
                        source_reference: "https://example.test/fictional/expired".into(),
                        payload_sha256: digest_hex(&raw),
                        processor_version: "fixture-v1".into(),
                        retrieved_at: captured_at,
                        validated_at: captured_at,
                    },
                    raw,
                }),
            })
            .await?;
        if !matches!(published, PublicationOutcome::Accepted(_)) {
            return Err("fixture cache publication conflicted".into());
        }
        let object = ObjectId {
            jurisdiction: "kr".into(),
            provider: "fictional".into(),
            dataset: Dataset::NationalStatute,
            id: "retained-image".into(),
        };
        let mut captures = Vec::new();
        let body = "Fictional retained body version two. 대한민국 법률 fixture.\n";
        for (revision, text) in [
            (
                "r1",
                "Fictional retained body version one. 대한민국 법률 fixture.\n",
            ),
            ("r2", body),
        ] {
            let capture = corpus
                .publish(
                    Publication {
                        expected_version: corpus.state(&object).await?.version,
                        record: LegalRecord {
                            object: object.clone(),
                            revision_id: revision.into(),
                            title: "Fictional 대한민국 retention fixture".into(),
                            body: text.into(),
                            metadata: BTreeMap::new(),
                            publication_date: None,
                            effective_date: None,
                            source_url: "https://example.test/fictional/retained-image".into(),
                            representation: "fictional_v1".into(),
                            sections: vec![],
                        },
                        raw: text.as_bytes().to_vec(),
                        additional_evidence: vec![],
                        processor_version: "fictional_v1".into(),
                        retrieved_at: now,
                        now,
                        install_head: true,
                        job_id: None,
                    },
                    CancellationToken::new(),
                )
                .await?;
            captures.push(capture.capture_id);
        }
        Ok(json!({
            "object": object,
            "head_capture_id": captures.last(),
            "captures": captures,
            "body": body,
            "query": "대한민국",
        }))
    }
    .await;
    // Release exclusion and all storage handles before starting the real server.
    let lease_closed = lease.close().await;
    let corpus_closed = corpus_blobs.close().await;
    let persistent_closed = persistent.close().await;
    lease_closed?;
    corpus_closed?;
    persistent_closed?;
    outcome
}
