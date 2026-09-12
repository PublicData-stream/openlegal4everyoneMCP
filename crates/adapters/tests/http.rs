#[path = "../examples/support/mod.rs"]
mod support;
use openlegal_adapters::{DestinationMode, HttpUpstream};
use openlegal_application::Upstream;
use openlegal_domain::{Query, RetrievalData, RetrievalError};
use openlegal_normalization::{LayoutAProcessor, LayoutBProcessor};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

async fn serve(router: axum::Router) -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let stop = CancellationToken::new();
    let signal = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(signal.cancelled_owned())
            .await
            .unwrap();
    });
    (url, stop, task)
}

#[tokio::test]
async fn both_json_layouts_fetch_and_preserve_original_bytes() {
    let (url, stop, task) = serve(support::router()).await;
    for (source, processor) in [
        (
            "layout_a",
            Arc::new(LayoutAProcessor) as Arc<dyn openlegal_normalization::PayloadProcessor>,
        ),
        ("layout_b", Arc::new(LayoutBProcessor)),
    ] {
        let upstream = HttpUpstream::new(&url, DestinationMode::MockLoopback, processor).unwrap();
        let result = upstream
            .fetch(
                Query::Search {
                    source: source.into(),
                    query: String::new(),
                    page: 0,
                    page_size: 5,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let RetrievalData::Search(page) = result.data else {
            panic!("wrong result kind")
        };
        assert_eq!(page.total, 12);
        assert_eq!(page.records.len(), 5);
        assert_eq!(page.records[0].id, "001");
        assert_eq!(page.records[0].source, source);
        assert!(!result.raw.is_empty());
        let detail = upstream
            .fetch(
                Query::Get {
                    source: source.into(),
                    id: "001".into(),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(detail.source_reference, format!("synthetic:{source}:001"));
        assert_eq!(
            upstream
                .fetch(
                    Query::Get {
                        source: source.into(),
                        id: "absent".into()
                    },
                    CancellationToken::new()
                )
                .await
                .err(),
            Some(RetrievalError::NotFound)
        );
    }
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn transport_rejects_redirect_compression_oversize_and_invalid_content() {
    use axum::{
        Router,
        http::{StatusCode, header},
        response::IntoResponse,
        routing::get,
    };
    for mode in ["redirect", "compressed", "oversize", "html", "throttled"] {
        let router = Router::new().route(
            "/layout_a/records/001",
            get(move || async move {
                match mode {
                    "redirect" => (
                        StatusCode::FOUND,
                        [(header::LOCATION, "http://127.0.0.1:1")],
                        "redirect",
                    )
                        .into_response(),
                    "compressed" => (
                        [
                            (header::CONTENT_ENCODING, "gzip"),
                            (header::CONTENT_TYPE, "application/json"),
                        ],
                        "{}",
                    )
                        .into_response(),
                    "oversize" => (
                        [(header::CONTENT_TYPE, "application/json")],
                        "x".repeat(1024 * 1024 + 1),
                    )
                        .into_response(),
                    "throttled" => (
                        StatusCode::TOO_MANY_REQUESTS,
                        [(header::RETRY_AFTER, "120")],
                        "busy",
                    )
                        .into_response(),
                    _ => ([(header::CONTENT_TYPE, "text/html")], "not JSON").into_response(),
                }
            }),
        );
        let (url, stop, task) = serve(router).await;
        let upstream = HttpUpstream::new(
            &url,
            DestinationMode::MockLoopback,
            Arc::new(LayoutAProcessor),
        )
        .unwrap();
        let error = upstream
            .fetch(
                Query::Get {
                    source: "layout_a".into(),
                    id: "001".into(),
                },
                CancellationToken::new(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            error,
            match mode {
                "oversize" => RetrievalError::ResourceLimit,
                "throttled" => RetrievalError::Throttled {
                    retry_after_secs: Some(120)
                },
                _ => RetrievalError::InvalidPayload,
            }
        );
        stop.cancel();
        task.await.unwrap();
    }
}
