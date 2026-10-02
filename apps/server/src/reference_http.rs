//! Human-readable exact-capture references; provider text is always inert.
use crate::handler::McpHandler;
use axum::{
    extract::{OriginalUri, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use openlegal_domain::{
    citation::CitationSource,
    legal::{DatabaseError, Dataset},
};
use tokio_util::sync::CancellationToken;

fn escape(value: &str) -> String {
    value.chars().fold(String::new(), |mut out, c| {
        out.push_str(match c {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => "&quot;",
            '\'' => "&#39;",
            _ => {
                out.push(c);
                return out;
            }
        });
        out
    })
}

fn language(query: Option<&str>) -> Result<bool, ()> {
    let query = query.unwrap_or_default();
    if query.len() > 64 {
        return Err(());
    }
    let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
    match pairs.as_slice() {
        [] => Ok(false),
        [(key, value)] if key == "lang" && value == "en" => Ok(false),
        [(key, value)] if key == "lang" && value == "ko" => Ok(true),
        _ => Err(()),
    }
}

fn timestamp(seconds: u64) -> String {
    i64::try_from(seconds)
        .ok()
        .and_then(|v| jiff::Timestamp::from_second(v).ok())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "Unknown".into())
}

fn render(source: &CitationSource, korean: bool) -> String {
    let label = |en: &str, ko: &str| if korean { ko.to_owned() } else { en.to_owned() };
    let meta = &source.metadata;
    let mut html = format!(
        "<!doctype html><html lang=\"{}\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{}</title><style>body{{max-width:64rem;margin:2rem auto;padding:0 1rem;font:17px/1.6 system-ui}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}}dd,code{{overflow-wrap:anywhere}}dt{{font-weight:600}}nav a{{margin-right:1rem}}</style><body><nav><a href=\"?lang=en\">English</a><a href=\"?lang=ko\">한국어</a></nav><main><h1>{}</h1><p>{}</p><dl>",
        if korean { "ko" } else { "en" },
        escape(&source.descriptor.title),
        escape(&meta.title),
        label(
            "Retained source evidence. Capture time does not establish legal applicability.",
            "저장된 출처 자료입니다. 수집 시각은 법률의 적용 여부를 확정하지 않습니다."
        )
    );
    let rows = [
        (label("Provider", "제공기관"), meta.object.provider.clone()),
        (
            label("Dataset", "자료 유형"),
            match meta.object.dataset {
                Dataset::NationalStatute => label("National statute", "법령"),
                Dataset::AdministrativeRule => label("Administrative rule", "행정규칙"),
                Dataset::Ordinance => label("Ordinance", "자치법규"),
                Dataset::Treaty => label("Treaty", "조약"),
                Dataset::Precedent => label("Court decision", "판례"),
                Dataset::ConstitutionalDecision => {
                    label("Constitutional decision", "헌법재판 결정")
                }
                Dataset::LegalInterpretation => label("Legal interpretation", "법령해석"),
                Dataset::AdministrativeAppeal => label("Administrative appeal", "행정심판"),
            },
        ),
        (
            label("Source record", "자료 식별자"),
            meta.object.id.clone(),
        ),
        (label("Revision", "제공기관 버전"), meta.revision_id.clone()),
        (label("Capture", "저장 버전"), meta.capture_id.clone()),
        (
            label("Publication date", "공포일"),
            meta.publication_date
                .clone()
                .unwrap_or_else(|| label("Unknown", "미확인")),
        ),
        (
            label("Effective date", "시행일"),
            meta.effective_date
                .clone()
                .unwrap_or_else(|| label("Unknown", "미확인")),
        ),
        (
            label("Retrieved", "수집 시각"),
            timestamp(meta.retrieved_at),
        ),
        (label("Captured", "저장 시각"), timestamp(meta.captured_at)),
        (
            label("Validated", "검증 시각"),
            timestamp(meta.validated_at),
        ),
        (
            label("Evidence SHA-256", "원문 SHA-256"),
            meta.raw_sha256.clone(),
        ),
        (
            label("Processor version", "처리기 버전"),
            meta.processor_version.clone(),
        ),
    ];
    for (key, value) in rows {
        html.push_str(&format!(
            "<dt>{}</dt><dd>{}</dd>",
            escape(&key),
            escape(&value)
        ));
    }
    html.push_str("</dl>");
    if let Some(url) = &source.descriptor.official_url {
        html.push_str(&format!(
            "<p><a rel=\"noopener noreferrer\" href=\"{}\">{}</a> — {}</p>",
            escape(url),
            label("Official source", "공식 웹사이트"),
            label(
                "The provider page may have been corrected after this capture.",
                "공식 페이지는 이 자료를 저장한 뒤 정정되었을 수 있습니다."
            )
        ));
    } else if meta.object.jurisdiction == "kr" && meta.object.provider == "law_go_kr" {
        html.push_str(&format!(
            "<p><a rel=\"noopener noreferrer\" href=\"https://www.law.go.kr/\">{}</a> — {}</p>",
            label("Official source lookup", "공식 웹사이트 검색"),
            label(
                "An exact official document link has not been verified.",
                "해당 자료의 정확한 공식 웹 링크는 확인되지 않았습니다."
            )
        ));
    } else {
        html.push_str(&format!(
            "<p>{}</p>",
            label(
                "An official browser link has not been verified for this provider.",
                "이 제공기관의 공식 웹 링크는 확인되지 않았습니다."
            )
        ));
    }
    if let Some(document) = &source.document {
        let kind = document.metadata.get("section_kind").map(String::as_str);
        let text_label = if kind == Some("ocr")
            || document
                .metadata
                .get("derived_ocr")
                .is_some_and(|v| v == "true")
        {
            label(
                "OCR-derived text; verify it against the source document.",
                "OCR로 추출한 텍스트입니다. 출처 문서와 대조해야 합니다.",
            )
        } else if kind == Some("extracted") {
            label("Extracted document text.", "문서에서 추출한 텍스트입니다.")
        } else if kind == Some("mixed") {
            label(
                "This projection includes extracted document text. Verify the source document for the original layout.",
                "문서에서 추출한 텍스트를 포함합니다. 원래 형식은 출처 문서에서 확인해야 합니다.",
            )
        } else {
            label(
                "Retained text projection; language selection changes page labels only.",
                "저장된 텍스트입니다. 언어 선택은 페이지의 안내 문구에만 적용됩니다.",
            )
        };
        html.push_str(&format!(
            "<h2>{}</h2><p>{}</p><pre>{}</pre>",
            escape(&document.title),
            text_label,
            escape(&document.text)
        ));
        for key in [
            "section",
            "byte_start",
            "byte_end",
            "representation",
            "section_kind",
            "derived_ocr",
            "source_document_sha256",
            "page",
        ] {
            if let Some(value) = document.metadata.get(key) {
                html.push_str(&format!("<p><b>{}</b>: {}</p>", escape(key), escape(value)));
            }
        }
    } else {
        html.push_str(&format!("<p role=\"status\">{}</p>", if source.unavailable {
            label("The body is unavailable. Only document metadata remains; the requested article or range cannot be verified. No current-version text has been substituted.", "본문을 이용할 수 없습니다. 문서 메타데이터만 남아 있어 요청한 조문이나 구간을 확인할 수 없습니다. 현재 버전의 본문으로 대체하지 않습니다.")
        } else { label("Document metadata reference.", "문서 메타데이터 참조입니다.") }));
    }
    html.push_str("<nav>");
    for (target, en, ko) in [
        (&source.previous, "Previous passage", "이전 구간"),
        (&source.next, "Next passage", "다음 구간"),
    ] {
        if let Some(target) = target {
            html.push_str(&format!(
                "<a href=\"{}?lang={}\">{}</a>",
                escape(&target.url),
                if korean { "ko" } else { "en" },
                label(en, ko)
            ));
        }
    }
    html.push_str("</nav></main></body></html>");
    html
}

pub(crate) async fn source_page(
    State(handler): State<McpHandler>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let Some(id) = uri.path().strip_prefix("/source/") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if id.len() > 4096 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(korean) = language(uri.query()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();
    match handler.citation_page(id, cancel).await {
        Ok(source) => {
            let body = render(&source, korean);
            if body.len() > (128 * 1024).min(handler.message_limit()) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let status = if source.unavailable {
                StatusCode::GONE
            } else {
                StatusCode::OK
            };
            (status, [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
                (header::CONTENT_SECURITY_POLICY, "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                (header::REFERRER_POLICY, "no-referrer"),
            ], body).into_response()
        }
        Err(error) => match error {
            DatabaseError::InvalidInput => StatusCode::BAD_REQUEST,
            DatabaseError::Withdrawn | DatabaseError::RevisionUnavailable => StatusCode::GONE,
            DatabaseError::NotFound | DatabaseError::NotObserved => StatusCode::NOT_FOUND,
            DatabaseError::Capacity => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_domain::{
        citation::{CitationDescriptor, CitationDocument},
        legal::{MetadataResult, ObjectId},
    };
    use std::collections::BTreeMap;

    fn fixture() -> CitationSource {
        let text = "<script>alert('x')</script> 제1조 & 자료";
        CitationSource {
            descriptor: CitationDescriptor {
                id: "fixture".into(),
                uri: "openlegal://source/fixture".into(),
                url: "https://example.test/source/fixture".into(),
                title: "<Fixture>".into(),
                body_available: Some(true),
                official_url: Some("https://example.test/?a=1&b=2".into()),
            },
            metadata: MetadataResult {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "fictional".into(),
                    dataset: Dataset::NationalStatute,
                    id: "one".into(),
                },
                revision_id: "r1".into(),
                capture_id: "capture".into(),
                title: "<Fixture>".into(),
                metadata: BTreeMap::new(),
                publication_date: None,
                effective_date: None,
                source_url: "https://example.test".into(),
                retrieved_at: 1,
                captured_at: 1,
                validated_at: 1,
                processor_version: "fixture".into(),
                raw_sha256: "digest".into(),
                freshness: None,
                collection_notices: vec![],
            },
            document: Some(CitationDocument {
                id: "fixture".into(),
                title: "Article".into(),
                text: text.into(),
                url: "https://example.test/source/fixture".into(),
                metadata: BTreeMap::from([
                    ("section_kind".into(), "ocr".into()),
                    ("page".into(), "3".into()),
                ]),
            }),
            previous: None,
            next: None,
            unavailable: false,
        }
    }

    #[test]
    fn rendering_preserves_inert_source_text_and_qualifies_ocr_and_expiry() {
        let mut source = fixture();
        let en = render(&source, false);
        let ko = render(&source, true);
        let expected = format!(
            "<pre>{}</pre>",
            escape(&source.document.as_ref().unwrap().text)
        );
        assert!(en.contains(&expected) && ko.contains(&expected));
        assert!(!en.contains("<script>"));
        assert!(en.contains("OCR-derived text") && ko.contains("OCR로 추출"));
        assert!(en.contains("href=\"https://example.test/?a=1&amp;b=2\""));
        source.document = None;
        source.unavailable = true;
        let expired = render(&source, false);
        assert!(expired.contains("requested article or range cannot be verified"));
        assert!(expired.contains("capture") && !expired.contains("<pre>"));
    }
    #[test]
    fn text_and_attributes_are_inert_and_language_is_bounded() {
        assert_eq!(
            escape("<script x=\"a'&\">"),
            "&lt;script x=&quot;a&#39;&amp;&quot;&gt;"
        );
        assert_eq!(language(None), Ok(false));
        assert_eq!(language(Some("lang=ko")), Ok(true));
        assert!(language(Some("lang=en&lang=ko")).is_err());
        assert!(language(Some("lang=unknown")).is_err());
    }
}
