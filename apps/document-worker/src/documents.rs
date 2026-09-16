//! Binary-format extraction. All filesystem access is to immutable bundled fonts
//! and models; this module is never linked into the API server.
use openlegal_application::document::*;
use xberg::{
    ExtractionConfig,
    core::config::{ExtractInput, OcrConfig, PageConfig},
};

const FONT: &str = "/opt/fonts/NotoSansCJKkr-Regular.otf";
const MODELS: &str = "/opt/tessdata";
const MAX_RASTER_SIDE: u32 = 4000;

fn config(ocr: bool) -> Result<ExtractionConfig, DocumentError> {
    // Explicitly avoid configuration discovery, caching, quality rewriting,
    // runtime model selection, remote services and native-text/OCR replacement.
    serde_json::from_value(serde_json::json!({
        "use_cache": false, "enable_quality_processing": false,
        "postprocessor": {"enabled": false},
        "disable_ocr": !ocr, "force_ocr": false,
        "extraction_timeout_secs": 280,
        "max_concurrent_extractions": 1,
        "security_limits": {
            "max_archive_size": 104857600, "max_compression_ratio": 100,
            "max_files_in_archive": 10000, "max_nesting_depth": 64,
            "max_entity_length": 1048576, "max_content_size": 64000000,
            "max_iterations": 1000000, "max_xml_depth": 64,
            "max_table_cells": 100000, "max_pages": 500
        },
        "max_embedded_file_bytes": 0,
        "pages": {"extract_pages": true, "insert_page_markers": false}
    }))
    .map_err(|_| DocumentError::ProcessingFailed)
}

async fn run_xberg(
    bytes: Vec<u8>,
    mime: &str,
    ocr: bool,
) -> Result<xberg::types::ExtractedDocument, DocumentError> {
    let mut config = config(ocr)?;
    if ocr {
        // Missing installed language data is a deployment failure, never a
        // reason to let Xberg's optional downloader fetch a model at runtime.
        for lang in ["kor", "eng"] {
            if !std::path::Path::new(MODELS)
                .join(format!("{lang}.traineddata"))
                .is_file()
            {
                return Err(DocumentError::SandboxUnavailable);
            }
        }
        config.ocr = Some(OcrConfig {
            backend: "tesseract".into(),
            language: vec!["kor".into(), "eng".into()],
            tessdata_path: Some(MODELS.into()),
            tesseract_config: Some(xberg::types::TesseractConfig {
                language: vec!["kor".into(), "eng".into()],
                // Markdown/hOCR applies a dictionary line filter that can
                // discard Korean and unfamiliar legal terms. Preserve plain
                // recognition output without table or quality rewriting.
                output_format: "text".into(),
                enable_table_detection: false,
                psm: Some(3),
                use_cache: false,
                preprocessing: None,
                min_confidence: 0.0,
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    config.pages = Some(PageConfig {
        extract_pages: true,
        ..Default::default()
    });
    let mut result = xberg::extract(ExtractInput::from_bytes(bytes, mime, None), &config)
        .await
        .map_err(|_| DocumentError::ProcessingFailed)?;
    if !result.errors.is_empty() || result.results.len() != 1 {
        return Err(DocumentError::ProcessingFailed);
    }
    result.results.pop().ok_or(DocumentError::ProcessingFailed)
}

fn push_page(
    output: &mut DocumentOutput,
    page: usize,
    text: String,
    ocr: bool,
) -> Result<(), DocumentError> {
    let total = output.text.len() + output.ocr_pages.iter().map(|p| p.text.len()).sum::<usize>();
    if total.saturating_add(text.len()) > MAX_DOCUMENT_OUTPUT_BYTES / 3 {
        return Err(DocumentError::ResourceLimit);
    }
    if ocr {
        output.ocr_pages.push(DocumentPage { page, text });
    } else {
        output.text.push_str(&text);
        output.text.push('\n');
        output.pages.push(DocumentPage { page, text });
    }
    Ok(())
}

pub async fn extract(
    input: &DocumentInput,
    output: &mut DocumentOutput,
) -> Result<(), DocumentError> {
    output.processor_version = "document-v1;rhwp=680111ec7bea;xberg=1.2.2;resvg=0.47.0;tessdata=4.1.0;NotoSansCJK=2.004;render=150dpi-max4000;ocr=plaintext".into();
    match input.format {
        DocumentFormat::Pdf => pdf(input, output).await?,
        DocumentFormat::Hwp5 | DocumentFormat::Hwpx => hwp(input, output).await?,
        _ => return Err(DocumentError::UnsupportedFormat),
    }
    if input.ocr {
        output
            .diagnostics
            .push("ocr_is_derived_not_authoritative".into());
    }
    Ok(())
}

async fn pdf(input: &DocumentInput, output: &mut DocumentOutput) -> Result<(), DocumentError> {
    if !input.raw.starts_with(b"%PDF-") {
        return Err(DocumentError::InvalidDocument);
    }
    let doc = xberg_native_pdf::PdfDocument::from_bytes(input.raw.clone())
        .map_err(|_| DocumentError::InvalidDocument)?;
    if doc.is_encrypted() {
        return Err(DocumentError::InvalidDocument);
    }
    let count = doc
        .page_count()
        .map_err(|_| DocumentError::InvalidDocument)?;
    if count == 0 || count > MAX_DOCUMENT_PAGES {
        return Err(DocumentError::ResourceLimit);
    }
    let native = run_xberg(input.raw.clone(), "application/pdf", false).await?;
    if !native.processing_warnings.is_empty() {
        output.diagnostics.push("native_extraction_warning".into());
    }
    let pages = native.pages.ok_or(DocumentError::ProcessingFailed)?;
    if pages.len() != count {
        return Err(DocumentError::InvalidDocument);
    }
    for (index, page) in pages.into_iter().enumerate() {
        push_page(output, index + 1, page.content, false)?;
    }
    if input.ocr {
        let options = xberg_native_pdf::rendering::RenderOptions::with_dpi(150);
        for index in 0..count {
            let (left, bottom, right, top) = doc
                .get_page_media_box(index)
                .map_err(|_| DocumentError::InvalidDocument)?;
            let width = ((right - left).abs() * 150.0 / 72.0).ceil();
            let height = ((top - bottom).abs() * 150.0 / 72.0).ceil();
            if !width.is_finite() || !height.is_finite() || width < 1.0 || height < 1.0 {
                return Err(DocumentError::InvalidDocument);
            }
            let rendered = xberg_native_pdf::rendering::render_page_fit(
                &doc,
                index,
                (width as u32).min(MAX_RASTER_SIDE),
                (height as u32).min(MAX_RASTER_SIDE),
                &options,
            )
            .map_err(|_| DocumentError::ProcessingFailed)?;
            if u64::from(rendered.width) * u64::from(rendered.height) > 16_000_000 {
                return Err(DocumentError::ResourceLimit);
            }
            let ocr = run_xberg(rendered.data, "image/png", true).await?;
            if !ocr.processing_warnings.is_empty() {
                add_diagnostic(output, "ocr_extraction_warning");
            }
            push_page(output, index + 1, ocr.content, true)?;
        }
    }
    Ok(())
}

fn add_diagnostic(output: &mut DocumentOutput, code: &str) {
    if !output.diagnostics.iter().any(|v| v == code) {
        output.diagnostics.push(code.into());
    }
}

async fn hwp(input: &DocumentInput, output: &mut DocumentOutput) -> Result<(), DocumentError> {
    let valid_magic = match input.format {
        DocumentFormat::Hwp5 => input
            .raw
            .starts_with(&[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1]),
        DocumentFormat::Hwpx => input.raw.starts_with(b"PK\x03\x04"),
        _ => false,
    };
    if !valid_magic {
        return Err(DocumentError::InvalidDocument);
    }
    if !std::path::Path::new(FONT).is_file() {
        return Err(DocumentError::SandboxUnavailable);
    }
    let doc =
        rhwp::DocumentCore::from_bytes(&input.raw).map_err(|_| DocumentError::InvalidDocument)?;
    if !doc.validation_report().warnings.is_empty() {
        output.diagnostics.push("hwp_validation_warning".into());
    }
    let count = doc.page_count() as usize;
    if count == 0 || count > MAX_DOCUMENT_PAGES {
        return Err(DocumentError::ResourceLimit);
    }
    output
        .diagnostics
        .push("hwp_rendered_text_projection".into());
    output.diagnostics.push("font_substitution_possible".into());
    let mut options = resvg::usvg::Options::default();
    options
        .fontdb_mut()
        .load_font_file(FONT)
        .map_err(|_| DocumentError::SandboxUnavailable)?;
    options.font_family = "Noto Sans CJK KR".into();
    // SVGs from HWP often request fonts absent from the image. Map every
    // generic fallback to the one explicitly bundled font; leaving fontdb's
    // platform defaults can silently drop all text during rasterization.
    options.fontdb_mut().set_serif_family("Noto Sans CJK KR");
    options
        .fontdb_mut()
        .set_sans_serif_family("Noto Sans CJK KR");
    options
        .fontdb_mut()
        .set_monospace_family("Noto Sans CJK KR");
    options.fontdb_mut().set_cursive_family("Noto Sans CJK KR");
    options.fontdb_mut().set_fantasy_family("Noto Sans CJK KR");
    options.image_href_resolver.resolve_string = Box::new(|_, _| None);
    for index in 0..count {
        let layout = doc
            .get_page_text_layout_native(index as u32)
            .map_err(|_| DocumentError::ProcessingFailed)?;
        if layout.len() > MAX_DOCUMENT_OUTPUT_BYTES {
            return Err(DocumentError::ResourceLimit);
        }
        let layout: serde_json::Value =
            serde_json::from_str(&layout).map_err(|_| DocumentError::InvalidDocument)?;
        let runs = layout
            .get("runs")
            .and_then(|v| v.as_array())
            .ok_or(DocumentError::InvalidDocument)?;
        let mut text = String::new();
        for run in runs {
            let value = run
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or(DocumentError::InvalidDocument)?;
            super::append_text(&mut text, value)?;
            super::append_text(&mut text, "\n")?;
        }
        push_page(output, index + 1, text, false)?;
        if input.ocr {
            let svg = doc
                .render_page_svg_legacy_native(index as u32)
                .map_err(|_| DocumentError::ProcessingFailed)?;
            if svg.len() > MAX_DOCUMENT_BYTES {
                return Err(DocumentError::ResourceLimit);
            }
            let tree = resvg::usvg::Tree::from_str(&svg, &options)
                .map_err(|_| DocumentError::ProcessingFailed)?;
            let size = tree.size();
            let scale = (150.0 / 96.0_f32)
                .min(MAX_RASTER_SIDE as f32 / size.width())
                .min(MAX_RASTER_SIDE as f32 / size.height());
            let width = (size.width() * scale).ceil() as u32;
            let height = (size.height() * scale).ceil() as u32;
            if width == 0 || height == 0 || u64::from(width) * u64::from(height) > 16_000_000 {
                return Err(DocumentError::ResourceLimit);
            }
            let mut pixmap =
                resvg::tiny_skia::Pixmap::new(width, height).ok_or(DocumentError::ResourceLimit)?;
            pixmap.fill(resvg::tiny_skia::Color::WHITE);
            resvg::render(
                &tree,
                resvg::tiny_skia::Transform::from_scale(scale, scale),
                &mut pixmap.as_mut(),
            );
            let png = pixmap
                .encode_png()
                .map_err(|_| DocumentError::ProcessingFailed)?;
            let ocr = run_xberg(png, "image/png", true).await?;
            if !ocr.processing_warnings.is_empty() {
                add_diagnostic(output, "ocr_extraction_warning");
            }
            push_page(output, index + 1, ocr.content, true)?;
        }
    }
    Ok(())
}
