//! Fictional fixtures shared by the local mock and isolated adapter tests.
use axum::{
    Json, Router,
    extract::{Path, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Search {
    #[serde(default)]
    query: String,
    #[serde(default)]
    page: u32,
    #[serde(default = "page_size")]
    page_size: u32,
}
fn page_size() -> u32 {
    5
}

fn row(index: u32, layout: &str) -> Value {
    let id = format!("{index:03}");
    let title = format!("Fictional library notice {id}");
    let body = format!(
        "Synthetic record {id}: the imaginary harbor library lends telescopes. This is test data, not law."
    );
    if layout == "layout_a" {
        json!({"id":id,"title":title,"body":body})
    } else {
        json!({"key":id,"label":title,"text":body})
    }
}

async fn search(
    Path(layout): Path<String>,
    Query(input): Query<Search>,
) -> axum::response::Response {
    let query = openlegal_domain::Query::Search {
        source: layout.clone(),
        query: input.query.clone(),
        page: input.page,
        page_size: input.page_size,
    };
    if !matches!(layout.as_str(), "layout_a" | "layout_b") || query.validate().is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let rows: Vec<_> = (1..=12)
        .map(|i| row(i, &layout))
        .filter(|row| {
            row.get("title")
                .or_else(|| row.get("label"))
                .and_then(Value::as_str)
                .is_some_and(|title| title.contains(&input.query))
        })
        .collect();
    let total = rows.len();
    let rows: Vec<_> = rows
        .into_iter()
        .skip((input.page * input.page_size) as usize)
        .take(input.page_size as usize)
        .collect();
    Json(if layout=="layout_a" { json!({"kind":"synthetic-layout-a","items":rows,"page":input.page,"page_size":input.page_size,"total":total}) }
    else { json!({"format":"synthetic-layout-b","results":rows,"pagination":{"index":input.page,"size":input.page_size,"count":total}}) }).into_response()
}
async fn detail(Path((layout, id)): Path<(String, String)>) -> axum::response::Response {
    if !matches!(layout.as_str(), "layout_a" | "layout_b") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(index) = id
        .parse::<u32>()
        .ok()
        .filter(|i| (1..=12).contains(i) && format!("{i:03}") == id)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let record = row(index, &layout);
    Json(if layout == "layout_a" {
        json!({"kind":"synthetic-layout-a","item":record})
    } else {
        json!({"format":"synthetic-layout-b","document":record})
    })
    .into_response()
}

pub fn router() -> Router {
    Router::new()
        .route("/{layout}/search", get(search))
        .route("/{layout}/records/{id}", get(detail))
}
