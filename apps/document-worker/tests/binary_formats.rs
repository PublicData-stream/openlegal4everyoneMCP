//! Run only in the isolated image with its pinned fonts and Korean/English models.
//! Documents are generated from a blank template and explicitly fictional text.
#![cfg(feature = "documents")]

use openlegal_application::document::{DocumentFormat, DocumentInput};
use sha2::{Digest, Sha256};

const TEXT: &str = "FICTIONAL OPENLEGAL TEST 12345. 가상 문서 검증.";

async fn check(format: DocumentFormat, ocr: bool, outlined_pdf: bool) {
    let mut document = rhwp::DocumentCore::new_empty();
    document.create_blank_document_native().unwrap();
    document.insert_text_native(0, 0, 0, TEXT).unwrap();
    let svg = document.render_page_svg_legacy_native(0).unwrap();
    let rendered_text: String = roxmltree::Document::parse(&svg)
        .unwrap()
        .descendants()
        .filter(|node| node.is_text() && node.parent().is_some_and(|p| p.has_tag_name("text")))
        .filter_map(|node| node.text())
        .collect();
    assert!(rendered_text.contains("FICTIONAL"));
    let raw = match format {
        DocumentFormat::Pdf => document
            .render_document_pdf_native_with_options(&rhwp::renderer::pdf::PdfExportOptions {
                fallback_serif: "Noto Sans CJK KR".into(),
                fallback_sans: "Noto Sans CJK KR".into(),
                fallback_mono: "Noto Sans CJK KR".into(),
                font_paths: vec!["/opt/fonts".into()],
                embed_text: !outlined_pdf,
                ..Default::default()
            })
            .unwrap(),
        DocumentFormat::Hwp5 => document.export_hwp_native().unwrap(),
        DocumentFormat::Hwpx => document.export_hwpx_native().unwrap(),
        _ => unreachable!(),
    };
    let source_sha256 = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let result = openlegal_document_worker::process(DocumentInput {
        format,
        raw,
        source_sha256,
        ocr,
    })
    .await
    .unwrap();
    assert_eq!(result.pages.len(), 1);
    if outlined_pdf {
        assert!(
            result.text.trim().is_empty(),
            "outlined PDF gained a native text layer"
        );
    } else {
        assert!(
            result.text.contains("FICTIONAL"),
            "native text missing: {:?}",
            result.text
        );
        assert!(result.text.contains("가상"), "Korean native text missing");
    }
    assert_eq!(result.ocr_pages.len(), usize::from(ocr));
    if ocr {
        assert!(
            result.ocr_pages[0].text.contains("OPENLEGAL"),
            "OCR did not recognize the fictional fixture: {:?}",
            result.ocr_pages[0].text
        );
        let korean: String = result.ocr_pages[0]
            .text
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            korean.contains("가상"),
            "Korean OCR missing: {:?}",
            result.ocr_pages[0].text
        );
        assert!(
            result
                .diagnostics
                .iter()
                .any(|code| code == "ocr_is_derived_not_authoritative")
        );
    }
}

macro_rules! fixture_test {
    ($name:ident, $format:ident, $ocr:literal) => {
        #[test]
        #[ignore = "requires isolated worker image with pinned fonts and OCR models"]
        fn $name() {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(check(DocumentFormat::$format, $ocr, false));
        }
    };
}

fixture_test!(pdf_native, Pdf, false);
fixture_test!(hwp5_native, Hwp5, false);
fixture_test!(hwpx_native, Hwpx, false);
fixture_test!(pdf_ocr, Pdf, true);
fixture_test!(hwp5_ocr, Hwp5, true);
fixture_test!(hwpx_ocr, Hwpx, true);

#[test]
#[ignore = "requires isolated worker image with pinned fonts and OCR models"]
fn pdf_without_text_layer_ocr() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(check(DocumentFormat::Pdf, true, true));
}
