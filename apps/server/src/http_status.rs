//! Preserve the admitted modern HTTP error mapping across SDK maintenance.
//!
//! Only SDK-produced SSE is inspected. Its serializer emits one compact JSON data
//! line per message. Other framing is replayed unchanged, never reinterpreted.
use axum::{
    body::{Body, BodyDataStream, Bytes},
    http::{StatusCode, header, response::Parts},
    response::Response,
};
use futures::{StreamExt, stream};
use serde::Deserialize;
use std::{io, ops::Range};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct FirstData {
    scan: usize,
    line: usize,
    data: Option<Range<usize>>,
    multiline: bool,
}
impl FirstData {
    // Scan each byte once and borrow the SDK's JSON line without another buffer.
    fn scan(&mut self, bytes: &[u8]) -> Option<Option<Range<usize>>> {
        while let Some(end) = bytes[self.scan..].iter().position(|b| *b == b'\n') {
            let end = self.scan + end;
            let start = self.line;
            let line = bytes[start..end]
                .strip_suffix(b"\r")
                .unwrap_or(&bytes[start..end]);
            self.scan = end + 1;
            self.line = self.scan;
            if line.is_empty() {
                if let Some(data) = self.data.take() {
                    return Some((!self.multiline).then_some(data));
                }
            } else if let Some(value) = line.strip_prefix(b"data:") {
                let value = value.strip_prefix(b" ").unwrap_or(value);
                let offset = start + line.len() - value.len();
                self.multiline |= self.data.is_some();
                self.data = Some(offset..offset + value.len());
            }
        }
        self.scan = bytes.len();
        None
    }
}

#[derive(Deserialize)]
struct ErrorCode {
    code: i32,
}
#[derive(Deserialize)]
struct ErrorHeader<'a> {
    #[serde(borrow)]
    jsonrpc: &'a str,
    #[serde(rename = "id")]
    _id: serde::de::IgnoredAny,
    error: ErrorCode,
}
fn invalid_params(data: &[u8]) -> bool {
    serde_json::from_slice::<ErrorHeader<'_>>(data)
        .is_ok_and(|message| message.jsonrpc == "2.0" && message.error.code == -32602)
}

fn replay(
    parts: Parts,
    prefix: Vec<u8>,
    deferred: Option<Result<Bytes, axum::Error>>,
    source: BodyDataStream,
) -> Response {
    let prefix = (!prefix.is_empty()).then(|| Ok(Bytes::from(prefix)));
    Response::from_parts(
        parts,
        Body::from_stream(
            stream::iter(prefix)
                .chain(stream::iter(deferred))
                .chain(source),
        ),
    )
}
fn failed(parts: Parts, message: &'static str) -> Response {
    Response::from_parts(
        parts,
        Body::from_stream(stream::once(async move {
            Err::<Bytes, _>(io::Error::other(message))
        })),
    )
}

pub(crate) async fn preserve_invalid_params_status(
    response: Response,
    modern: bool,
    limit: usize,
    deadline: Instant,
    cancel: &CancellationToken,
) -> Response {
    if !modern
        || response.status() != StatusCode::OK
        || response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            != Some("text/event-stream")
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let mut source = body.into_data_stream();
    // At most one ceiling-sized prefix and, only for a replacement, one JSON copy.
    // Borrowed header decoding creates no full JSON tree. The existing four-copy
    // request reservation and final response-byte ceiling continue to apply.
    let mut prefix = Vec::with_capacity(limit);
    let mut first = FirstData::default();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancel.cancelled() => return failed(parts, "server shutdown"),
            result = tokio::time::timeout_at(deadline, source.next()) => match result {
                Ok(chunk) => chunk,
                Err(_) => return failed(parts, "response deadline"),
            },
        };
        let Some(chunk) = chunk else {
            return replay(parts, prefix, None, source);
        };
        let bytes = match chunk {
            Ok(bytes) if bytes.len() <= limit - prefix.len() => bytes,
            deferred => return replay(parts, prefix, Some(deferred), source),
        };
        prefix.extend_from_slice(&bytes);
        drop(bytes);
        if let Some(data) = first.scan(&prefix) {
            if let Some(data) = data.filter(|range| invalid_params(&prefix[range.clone()])) {
                // Exactly the old SDK first-terminal-message mapping. A progress
                // notification first keeps HTTP 200 and its complete original SSE.
                parts.status = StatusCode::BAD_REQUEST;
                parts.headers.insert(
                    header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
                parts.headers.remove(header::CONTENT_LENGTH);
                return Response::from_parts(
                    parts,
                    Body::from(Bytes::copy_from_slice(&prefix[data])),
                );
            }
            return replay(parts, prefix, None, source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use std::time::Duration;

    fn sse(chunks: Vec<Vec<u8>>) -> Response {
        let body = Body::from_stream(stream::iter(
            chunks
                .into_iter()
                .map(|bytes| Ok::<_, io::Error>(Bytes::from(bytes))),
        ));
        Response::builder()
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(body)
            .unwrap()
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn fragmented_first_error_preserves_json_status_id_and_payload() {
        let json = br#"{"jsonrpc":"2.0","id":"request-a","error":{"code":-32602,"message":"invalid tool arguments"}}"#;
        for newline in ["\n", "\r\n"] {
            let wire = format!(
                ": keepalive{newline}{newline}event: message{newline}data: {}{newline}{newline}",
                std::str::from_utf8(json).unwrap()
            );
            let chunks = wire.as_bytes().iter().map(|b| vec![*b]).collect();
            let response = preserve_invalid_params_status(
                sse(chunks),
                true,
                1024,
                deadline(),
                &CancellationToken::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
                json
            );
        }
    }

    #[tokio::test]
    async fn first_notification_replays_comments_and_later_error_exactly() {
        let wire = b": keepalive\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602,\"message\":\"invalid\"}}\n\n";
        let response = preserve_invalid_params_status(
            sse(vec![wire.to_vec()]),
            true,
            1024,
            deadline(),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            wire
        );
    }

    #[tokio::test]
    async fn legacy_other_errors_and_unsupported_framing_are_not_rewritten() {
        for (modern, wire) in [
            (
                false,
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602}}\n\n",
            ),
            (
                true,
                "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32603}}\n\n",
            ),
            (
                true,
                "data: {\"jsonrpc\":\"2.0\",\ndata: \"id\":1,\"error\":{\"code\":-32602}}\n\n",
            ),
        ] {
            let response = preserve_invalid_params_status(
                sse(vec![wire.as_bytes().to_vec()]),
                modern,
                1024,
                deadline(),
                &CancellationToken::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
                wire.as_bytes()
            );
        }
    }

    #[tokio::test]
    async fn prefix_ceiling_replays_the_overlimit_chunk_without_extending_probe() {
        let wire = b"data: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602}}\n\n";
        let response = preserve_invalid_params_status(
            sse(vec![wire[..4].to_vec(), wire[4..].to_vec()]),
            true,
            16,
            deadline(),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            wire
        );
    }

    #[tokio::test]
    async fn cancellation_and_absolute_deadline_drop_a_stalled_probe() {
        for canceled in [false, true] {
            let cancel = CancellationToken::new();
            if canceled {
                cancel.cancel();
            }
            let response = Response::builder()
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(
                    stream::pending::<Result<Bytes, io::Error>>(),
                ))
                .unwrap();
            let response = tokio::time::timeout(
                Duration::from_secs(1),
                preserve_invalid_params_status(
                    response,
                    true,
                    1024,
                    Instant::now() + Duration::from_millis(10),
                    &cancel,
                ),
            )
            .await
            .unwrap();
            assert!(to_bytes(response.into_body(), 1024).await.is_err());
        }
    }
}
