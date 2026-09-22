//! Synthetic durable observations through PostgreSQL 18, blobs and comparison workers.
#[path = "../../../test-support/postgres.rs"]
mod postgres;
use futures::future::BoxFuture;
use openlegal_application::{Clock, FetchedPayload, RetrievalService, Source, Upstream};
use openlegal_domain::{FreshnessRequirement, Query, Record, RetrievalData, RetrievalError};
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits, SourceOffer},
    demo::DemoTools,
    http::HttpEndpoint,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

struct Time(AtomicU64);
impl Clock for Time {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct Mock {
    revision: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl Upstream for Mock {
    fn fetch(
        &self,
        query: Query,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let revision = self.revision.load(Ordering::SeqCst);
        Box::pin(async move {
            let Query::Get { source, id } = query else {
                return Err(RetrievalError::InvalidInput);
            };
            let data = RetrievalData::Get(Record {
                source,
                id,
                title: format!("Fictional title {revision}"),
                body: format!("Fictional body {revision}\n"),
                synthetic: true,
            });
            Ok(FetchedPayload {
                raw: serde_json::to_vec(&data).unwrap(),
                data,
                source_reference: "https://example.test/synthetic".into(),
            })
        })
    }
}

async fn call(url: &str, name: &str, arguments: Value) -> Value {
    let response = reqwest::Client::new().post(url).header("host", "history.test")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28").header("mcp-method", "tools/call").header("mcp-name", name)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments,"_meta":{
            "io.modelcontextprotocol/protocolVersion":"2026-07-28", "io.modelcontextprotocol/clientInfo":{"name":"history-test","version":"1"}, "io.modelcontextprotocol/clientCapabilities":{}
        }}})).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    if text.trim_start().starts_with('{') {
        serde_json::from_str::<Value>(&text).unwrap()["result"].clone()
    } else {
        text.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .find(|v| v["id"] == 1)
            .unwrap()["result"]
            .clone()
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn changes_reversions_exact_history_and_comparison_preserve_capture_origin() {
    let database = postgres::TestDatabase::new().await;
    let exe = Path::new(env!("CARGO_BIN_EXE_openlegal-server"));
    let store = database.open(1000).await;
    let time = Arc::new(Time(AtomicU64::new(1000)));
    let revision = Arc::new(AtomicUsize::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let service = RetrievalService::with_persistence(
        vec![Source {
            id: "layout_a".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "test-v1".into(),
            upstream: Arc::new(Mock {
                revision: revision.clone(),
                calls: calls.clone(),
            }),
        }],
        time.clone(),
        Box::new(openlegal_adapters::MemoryCache::new()),
        store,
        "a".repeat(64),
    )
    .unwrap();
    let query = Query::Get {
        source: "layout_a".into(),
        id: "001".into(),
    };
    let mut captures = Vec::new();
    for (at, value) in [(1000, 1), (1061, 1), (1122, 2), (1183, 1)] {
        time.0.store(at, Ordering::SeqCst);
        revision.store(value, Ordering::SeqCst);
        captures.push(
            service
                .retrieve(
                    query.clone(),
                    FreshnessRequirement::FreshOnly,
                    CancellationToken::new(),
                    None,
                )
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        captures[0].snapshot, captures[1].snapshot,
        "unchanged refresh reuses immutable occurrence"
    );
    assert_ne!(
        captures[0].snapshot, captures[3].snapshot,
        "reversion is a new occurrence"
    );
    let comparison = openlegal_server::text_diff::service(exe).await.unwrap();
    let source = SourceOffer::new("https://example.test/source").unwrap();
    let mut registry = openlegal_server::registry::server_info_registry(source.clone()).unwrap();
    registry
        .register_module(DemoTools {
            service: service.clone(),
            comparison: Some(comparison.clone()),
        })
        .unwrap();
    registry
        .register_module(openlegal_server::text_diff::TextDiffTools {
            service: comparison.clone(),
        })
        .unwrap();
    let html = "<html><meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">Synthetic history test</html>";
    let mut resources = openlegal_server::demo::widget_resources(html.into(), &source).unwrap();
    resources
        .extend(openlegal_server::text_diff::widget_resources(html.into(), &source).unwrap())
        .unwrap();
    let mut builder =
        ServerBuilder::new(registry, Limits::default(), source).with_resources(resources);
    let retrieval_worker = service.clone();
    builder
        .register_worker("retrieval", move |shutdown| async move {
            retrieval_worker.run(shutdown).await?;
            Ok(())
        })
        .unwrap();
    let comparison_worker = comparison.clone();
    builder
        .register_worker("comparison", move |shutdown| async move {
            comparison_worker.run(shutdown).await?;
            Ok(())
        })
        .unwrap();
    builder
        .register_endpoint(HttpEndpoint {
            bind: "127.0.0.1:0".parse().unwrap(),
            access: AccessPolicy {
                allowed_hosts: vec!["history.test".into()],
                allowed_origins: vec![],
            },
        })
        .unwrap();
    let bound = builder.bind().await.unwrap();
    let url = format!("http://{}/mcp", bound.addresses()[0].1[0]);
    let stop = CancellationToken::new();
    let running = tokio::spawn(bound.run(stop.clone()));
    let listed = call(
        &url,
        "demo_list_snapshots",
        json!({"query":query,"limit":2}),
    )
    .await;
    let shown_capabilities = call(&url, "demo_show_records", json!({"records":[]})).await;
    assert_eq!(
        shown_capabilities["structuredContent"]["capabilities"]["processor_versions"],
        json!({"layout_a":"test-v1"})
    );
    let page = &listed["structuredContent"];
    assert_eq!(page["snapshots"].as_array().unwrap().len(), 2);
    let rest = call(
        &url,
        "demo_list_snapshots",
        json!({"query":query,"cursor":page["next_cursor"],"limit":2}),
    )
    .await;
    assert_eq!(
        rest["structuredContent"]["snapshots"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let first = &captures[0].snapshot.as_ref().unwrap().snapshot_id;
    let second = &captures[2].snapshot.as_ref().unwrap().snapshot_id;
    let exact = call(
        &url,
        "demo_get_snapshot",
        json!({"query":query,"snapshot_id":first}),
    )
    .await;
    assert_eq!(
        exact["structuredContent"]["provenance"]["validated_at"],
        1000
    );
    assert!(exact["structuredContent"].get("freshness").is_none());
    let result = call(&url,"demo_compare_record_snapshots",json!({"source":"layout_a","id":"001","before_snapshot_id":first,"after_snapshot_id":second})).await;
    assert_ne!(result["isError"], true, "{result}");
    let summary = &result["structuredContent"];
    assert_eq!(summary["origin"]["before"]["snapshot_id"], *first);
    assert_eq!(summary["origin"]["projection"], "title_lf_lf_body_v1");
    let original = call(
        &url,
        "get_text_diff_page",
        json!({"comparison_id":summary["comparison_id"],"view":"before","page":0}),
    )
    .await;
    assert_eq!(
        original["structuredContent"]["text"],
        "Fictional title 1\n\nFictional body 1\n"
    );
    let shown = call(
        &url,
        "show_text_diff",
        json!({"comparison_id":summary["comparison_id"]}),
    )
    .await;
    assert_eq!(
        shown["structuredContent"]["comparison"]["origin"],
        summary["origin"]
    );
    let wrong = call(&url,"demo_compare_record_snapshots",json!({"source":"layout_a","id":"002","before_snapshot_id":first,"after_snapshot_id":second})).await;
    assert_eq!(wrong["isError"], true);
    let supplied = call(&url, "compare_texts", json!({"before":"a","after":"b"})).await;
    assert!(supplied["structuredContent"].get("origin").is_none());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "history/comparison make no upstream requests"
    );
    stop.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn startup_widget_failure_closes_persistence_without_damaging_storage() {
    let database = postgres::TestDatabase::new().await;
    let root = database.directory.path().join("blobs");
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
    config.as_table_mut().unwrap().remove("text_diff");
    config["cache"]["blob"]["path"] = toml::Value::String(root.to_str().unwrap().into());
    config["demo"]["widget_html"] = toml::Value::String(
        database
            .directory
            .path()
            .join("missing.html")
            .to_str()
            .unwrap()
            .into(),
    );
    let path = database.directory.path().join("server.toml");
    tokio::fs::write(&path, toml::to_string(&config).unwrap())
        .await
        .unwrap();
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_openlegal-server"))
            .arg(path)
            .env("OPENLEGAL_DATABASE_URL", &database.url)
            .env_remove("OPENLEGAL_MIGRATION_DATABASE_URL")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(root.is_dir(), "failure occurred after opening blob storage");
    let reopened = database.open(1000).await;
    openlegal_application::persistence::PersistentStore::close(reopened.as_ref())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn unrelated_concurrent_publications_keep_both_memory_entries() {
    let database = postgres::TestDatabase::new().await;
    let store = database.open(1000).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let service = RetrievalService::with_persistence(
        vec![Source {
            id: "layout_a".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "test-v1".into(),
            upstream: Arc::new(Mock {
                revision: Arc::new(AtomicUsize::new(1)),
                calls: calls.clone(),
            }),
        }],
        Arc::new(Time(AtomicU64::new(1000))),
        Box::new(openlegal_adapters::MemoryCache::new()),
        store,
        "b".repeat(64),
    )
    .unwrap();
    let query = |id: &str| Query::Get {
        source: "layout_a".into(),
        id: id.into(),
    };
    let (a, b) = tokio::join!(
        service.retrieve(
            query("001"),
            FreshnessRequirement::FreshOnly,
            CancellationToken::new(),
            None
        ),
        service.retrieve(
            query("002"),
            FreshnessRequirement::FreshOnly,
            CancellationToken::new(),
            None
        )
    );
    assert!(a.is_ok(), "{a:?}");
    assert!(b.is_ok(), "{b:?}");
    assert_eq!(service.metrics().cache_entries, 2);
    let disk_hits = service.metrics().persistent.hits;
    service
        .retrieve(
            query("001"),
            FreshnessRequirement::FreshOnly,
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    service
        .retrieve(
            query("002"),
            FreshnessRequirement::FreshOnly,
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        service.metrics().persistent.hits,
        disk_hits,
        "unrelated writes preserve L1"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn cancelled_publication_does_not_install_a_head_and_storage_remains_usable() {
    use openlegal_application::{
        StoredPayload,
        persistence::{
            HistoryKey, PersistentKey, PersistentStore, PublicationOutcome, PublicationRequest,
        },
    };
    use openlegal_domain::Provenance;
    use sha2::{Digest, Sha256};
    let database = postgres::TestDatabase::new().await;
    let store = database.open(1000).await;
    let key = PersistentKey {
        history: HistoryKey {
            namespace: "c".repeat(64),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "layout_a".into(),
                id: "001".into(),
            },
        },
        processor_version: "test-v1".into(),
        schema_version: 1,
    };
    let data = RetrievalData::Get(Record {
        source: "layout_a".into(),
        id: "001".into(),
        title: "Fictional".into(),
        body: "Fictional".into(),
        synthetic: true,
    });
    let raw = serde_json::to_vec(&data).unwrap();
    let digest = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let value = Arc::new(StoredPayload {
        bytes: raw.len() + 2048,
        raw,
        data,
        snapshot: None,
        provenance: Provenance {
            provider: "synthetic".into(),
            dataset: "records".into(),
            source_reference: "https://example.test/synthetic".into(),
            payload_sha256: digest,
            processor_version: "test-v1".into(),
            retrieved_at: 1000,
            validated_at: 1000,
        },
    });
    let token = CancellationToken::new();
    let cancel = token.clone();
    let expected = store
        .lookup(key.clone(), 1000, CancellationToken::new())
        .await
        .unwrap()
        .observation;
    let result = store
        .publish(PublicationRequest {
            key: key.clone(),
            value: value.clone(),
            expected,
            now: 1000,
            authorize: Arc::new(move || {
                cancel.cancel();
                false
            }),
            cancellation: token,
        })
        .await;
    assert!(matches!(result, Err(RetrievalError::Cancelled)));
    assert!(store.healthy());
    assert!(
        store
            .lookup(key.clone(), 1000, CancellationToken::new())
            .await
            .unwrap()
            .value
            .is_none(),
        "cancelled publication leaves no committed head"
    );
    let expected = store
        .lookup(key.clone(), 1000, CancellationToken::new())
        .await
        .unwrap()
        .observation;
    let accepted = store
        .publish(PublicationRequest {
            key,
            value,
            expected,
            now: 1000,
            authorize: Arc::new(|| true),
            cancellation: CancellationToken::new(),
        })
        .await
        .unwrap();
    let PublicationOutcome::Accepted(accepted) = accepted else {
        panic!("uncontended publication conflicted");
    };
    assert!(accepted.payload.snapshot.is_some());
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn migration_uses_only_admin_secret_and_never_initializes_serving() {
    let database = postgres::TestDatabase::new().await;
    let root = database.directory.path().join("never-opened-blobs");
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
    config["http"]["bind"] = toml::Value::String(occupied.local_addr().unwrap().to_string());
    config["cache"]["blob"]["path"] = toml::Value::String(root.to_str().unwrap().into());
    config["demo"]["widget_html"] = toml::Value::String("missing-record-widget.html".into());
    config["text_diff"]["widget_html"] =
        toml::Value::String("missing-comparison-widget.html".into());
    let path = database.directory.path().join("migrate.toml");
    tokio::fs::write(&path, toml::to_string(&config).unwrap())
        .await
        .unwrap();
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_openlegal-server"))
            .arg("--migrate")
            .arg(path)
            .env(
                "OPENLEGAL_DATABASE_URL",
                "invalid-runtime-url-must-not-be-read",
            )
            .env("OPENLEGAL_MIGRATION_DATABASE_URL", &database.url)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "migration command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!root.exists(), "migration must not initialize blob storage");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&database.url));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(&database.url));
}

async fn maintenance_cli_prunes_and_reopens(processes: usize) {
    use openlegal_adapters::{
        blob::FsBlobStore,
        postgres::{PostgresStore, StartupMode},
    };
    use openlegal_application::{
        SystemClock,
        persistence::{HistoryKey, PersistentStore, RetentionPolicy},
    };
    let database = postgres::TestDatabase::new().await;
    let now = SystemClock::default().now();
    let store = database.open(now).await;
    let time = Arc::new(Time(AtomicU64::new(now - 180)));
    let revision = Arc::new(AtomicUsize::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let namespace = "d".repeat(64);
    let service = RetrievalService::with_persistence(
        vec![Source {
            id: "layout_a".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "test-v1".into(),
            upstream: Arc::new(Mock {
                revision: revision.clone(),
                calls: calls.clone(),
            }),
        }],
        time.clone(),
        Box::new(openlegal_adapters::MemoryCache::new()),
        store.clone(),
        namespace.clone(),
    )
    .unwrap();
    let query = Query::Get {
        source: "layout_a".into(),
        id: "001".into(),
    };
    let mut captures = Vec::new();
    for n in 0..3 {
        time.0.store(now - 180 + n * 61, Ordering::SeqCst);
        revision.store(n as usize + 1, Ordering::SeqCst);
        captures.push(
            service
                .retrieve(
                    query.clone(),
                    FreshnessRequirement::FreshOnly,
                    CancellationToken::new(),
                    None,
                )
                .await
                .unwrap(),
        );
    }
    let history = HistoryKey {
        namespace: namespace.clone(),
        provider: "synthetic".into(),
        dataset: "records".into(),
        query: query.clone(),
    };
    assert_eq!(
        store
            .list(history, None, 20, now, CancellationToken::new())
            .await
            .unwrap()
            .snapshots
            .len(),
        3
    );
    service.shutdown().await.unwrap();
    drop(service);
    drop(store);

    let root = database.directory.path();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
    config["http"]["bind"] = toml::Value::String(occupied.local_addr().unwrap().to_string());
    config["cache"]["blob"]["path"] =
        toml::Value::String(root.join("blobs").to_str().unwrap().into());
    for limit in ["max_snapshots_per_query", "max_snapshots", "max_queries"] {
        config["cache"][limit] = toml::Value::Integer(1);
    }
    config["demo"]["widget_html"] =
        toml::Value::String(root.join("missing-demo.html").to_str().unwrap().into());
    config["text_diff"]["widget_html"] =
        toml::Value::String(root.join("missing-diff.html").to_str().unwrap().into());
    config["webtransport"]["certificate"] =
        toml::Value::String(root.join("missing.crt").to_str().unwrap().into());
    config["webtransport"]["private_key"] =
        toml::Value::String(root.join("missing.key").to_str().unwrap().into());
    let unused = root.join("unused-corpus");
    config.as_table_mut().unwrap().insert(
        "database".into(),
        toml::Value::Table(toml::map::Map::from_iter([
            (
                "blob_path".into(),
                toml::Value::String(unused.join("blobs").to_str().unwrap().into()),
            ),
            (
                "index_path".into(),
                toml::Value::String(unused.join("index").to_str().unwrap().into()),
            ),
            (
                "mecab_dictionary_path".into(),
                toml::Value::String(unused.join("dictionary").to_str().unwrap().into()),
            ),
            (
                "widget_html".into(),
                toml::Value::String(unused.join("widget.html").to_str().unwrap().into()),
            ),
        ])),
    );
    let path = root.join("maintain.toml");
    tokio::fs::write(&path, toml::to_string(&config).unwrap())
        .await
        .unwrap();
    // Separate OS processes, identical configuration, and all publishers closed.
    // Start both before awaiting either; idempotent completion does not imply
    // that Kubernetes provides exactly-once Job execution.
    let mut children = Vec::new();
    for _ in 0..processes {
        children.push(
            tokio::process::Command::new(env!("CARGO_BIN_EXE_openlegal-server"))
                .arg("--maintain")
                .arg(&path)
                .env("OPENLEGAL_DATABASE_URL", &database.url)
                .env(
                    "OPENLEGAL_MIGRATION_DATABASE_URL",
                    "invalid-admin-url-must-not-be-read",
                )
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        );
    }
    for child in children {
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        assert!(output.status.success(), "cache-maintenance command failed");
        assert!(!String::from_utf8_lossy(&output.stdout).contains(&database.url));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(&database.url));
    }
    assert!(
        !unused.exists(),
        "maintenance must not initialize unused corpus storage"
    );
    let blobs = FsBlobStore::open(&root.join("blobs")).await.unwrap();
    let reopened = PostgresStore::open(
        &database.url,
        postgres::options(),
        blobs,
        RetentionPolicy {
            max_snapshots_per_query: 1,
            max_snapshots: 1,
            max_queries: 1,
            ..RetentionPolicy::default()
        },
        now,
        StartupMode::Serve,
    )
    .await
    .unwrap();
    // Serve-mode reopen verifies persisted accounting against actual rows.
    assert_eq!(reopened.metrics().snapshots, 1);
    assert_eq!(reopened.metrics().queries, 1);
    assert!(reopened.metrics().bytes > 0);
    let history = HistoryKey {
        namespace,
        provider: "synthetic".into(),
        dataset: "records".into(),
        query,
    };
    let page = reopened
        .list(history.clone(), None, 20, now, CancellationToken::new())
        .await
        .unwrap();
    let latest = captures.last().unwrap().snapshot.as_ref().unwrap();
    assert_eq!(page.snapshots.len(), 1);
    assert_eq!(page.snapshots[0].snapshot_id, latest.snapshot_id);
    let exact = reopened
        .get(
            history.clone(),
            latest.snapshot_id.clone(),
            now,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(exact.data, captures.last().unwrap().data);
    assert_eq!(exact.provenance, captures.last().unwrap().provenance);
    assert_eq!(
        reopened
            .get(
                history,
                captures[0].snapshot.as_ref().unwrap().snapshot_id.clone(),
                now,
                CancellationToken::new()
            )
            .await
            .unwrap_err(),
        RetrievalError::SnapshotUnavailable
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "administration never fetches upstreams"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn maintenance_cli_prunes_without_unused_serving_inputs() {
    maintenance_cli_prunes_and_reopens(1).await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn duplicate_maintenance_processes_preserve_retained_evidence_and_accounting() {
    maintenance_cli_prunes_and_reopens(2).await;
}
