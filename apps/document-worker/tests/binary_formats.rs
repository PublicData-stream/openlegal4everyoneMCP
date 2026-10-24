//! Run only in the isolated image with its pinned fonts and Korean/English models.
//! Documents are generated from a blank template and explicitly fictional text.
#![cfg(feature = "documents")]

use openlegal_application::document::{DocumentError, DocumentFormat, DocumentInput};
use sha2::{Digest, Sha256};

const TEXT: &str = "FICTIONAL OPENLEGAL TEST 12345. 가상 문서 검증.";

#[tokio::test]
async fn html_error_page_is_not_a_pdf() {
    let raw = b"<!doctype html><html><body>Fictional service busy</body></html>".to_vec();
    let source_sha256 = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert!(matches!(
        openlegal_document_worker::process(DocumentInput {
            format: DocumentFormat::Pdf,
            raw,
            source_sha256,
            ocr: false,
        })
        .await
        .err(),
        Some(DocumentError::InvalidDocument)
    ));
}

#[tokio::test]
async fn pdf_header_with_broken_body_is_invalid_document() {
    let raw = b"%PDF-1.7\nfictional damaged body\n".to_vec();
    let source_sha256 = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        openlegal_document_worker::process(DocumentInput {
            format: DocumentFormat::Pdf,
            raw,
            source_sha256,
            ocr: false,
        })
        .await
        .err(),
        Some(DocumentError::InvalidDocument)
    );
}

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

struct ArchiveFixtureDirectory(std::path::PathBuf);

impl ArchiveFixtureDirectory {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("openlegal-archive-{name}-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for ArchiveFixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
#[ignore = "requires isolated worker image with writable disposable scratch"]
fn zip_symlink_cannot_extract_outside_destination() {
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;

    let scratch = ArchiveFixtureDirectory::new("escape");
    let destination = scratch.0.join("destination");
    let outside = scratch.0.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let canary = outside.join("canary.txt");
    std::fs::write(&canary, b"FICTIONAL ORIGINAL").unwrap();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .add_symlink("escape", "../outside", SimpleFileOptions::default())
        .unwrap();
    writer
        .start_file("escape/canary.txt", SimpleFileOptions::default())
        .unwrap();
    writer.write_all(b"FICTIONAL REPLACEMENT").unwrap();
    let raw = writer.finish().unwrap().into_inner();
    let mut archive = zip::ZipArchive::new(Cursor::new(raw)).unwrap();

    // Require ZIP validation itself to reject the target; the worker seccomp
    // profile also denies symlink creation, which must not mask a regression.
    assert!(matches!(
        archive.extract(&destination),
        Err(zip::result::ZipError::InvalidArchive(_))
    ));
    assert_eq!(std::fs::read(&canary).unwrap(), b"FICTIONAL ORIGINAL");
    assert!(!destination.join("escape").exists());
}

#[test]
#[ignore = "requires isolated worker image with pinned font and disposable scratch"]
fn hwpx_symlink_entry_remains_archive_data() {
    use std::io::{Cursor, Read};
    use zip::write::SimpleFileOptions;

    let scratch = ArchiveFixtureDirectory::new("header");
    let mut document = rhwp::DocumentCore::new_empty();
    document.create_blank_document_native().unwrap();
    document.insert_text_native(0, 0, 0, TEXT).unwrap();
    let raw = document.export_hwpx_native().unwrap();
    let mut archive = zip::ZipArchive::new(Cursor::new(raw)).unwrap();
    let mut header = Vec::new();
    archive
        .by_name("Contents/header.xml")
        .unwrap()
        .read_to_end(&mut header)
        .unwrap();
    let target = scratch.0.join("valid-header.xml");
    std::fs::write(&target, &header).unwrap();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for index in 0..archive.len() {
        writer
            .raw_copy_file(archive.by_index(index).unwrap())
            .unwrap();
    }
    writer
        .add_symlink(
            "FICTIONAL-EXTERNAL-SYMLINK.xml",
            target.to_str().unwrap(),
            SimpleFileOptions::default(),
        )
        .unwrap();
    let raw = writer.finish().unwrap().into_inner();
    // The parser used by the worker reads the archive payload, not the file at
    // the symlink target. Merely requiring document rejection would be wrong:
    // upstream supports inert unknown entries and tolerates a plain-text header.
    let mut reader = rhwp::parser::hwpx::reader::HwpxReader::open(&raw).unwrap();
    assert_eq!(
        reader.read_file("FICTIONAL-EXTERNAL-SYMLINK.xml").unwrap(),
        target.to_str().unwrap()
    );
    let source_sha256 = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(openlegal_document_worker::process(DocumentInput {
            format: DocumentFormat::Hwpx,
            raw,
            source_sha256,
            ocr: false,
        }));

    let output = result.unwrap();
    let actual: String = output.text.split_whitespace().collect();
    let expected: String = TEXT.split_whitespace().collect();
    assert!(actual.contains(&expected));
    assert_eq!(std::fs::read(target).unwrap(), header);
}

#[test]
#[ignore = "requires isolated worker image with pinned font"]
fn hwpx_legacy_entry_name_preserves_document_text() {
    use std::io::Cursor;
    use zip::write::SimpleFileOptions;

    let mut document = rhwp::DocumentCore::new_empty();
    document.create_blank_document_native().unwrap();
    document.insert_text_native(0, 0, 0, TEXT).unwrap();
    let raw = document.export_hwpx_native().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let baseline = runtime
        .block_on(openlegal_document_worker::process(DocumentInput {
            format: DocumentFormat::Hwpx,
            source_sha256: Sha256::digest(&raw)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            raw: raw.clone(),
            ocr: false,
        }))
        .unwrap();
    let mut archive = zip::ZipArchive::new(Cursor::new(raw)).unwrap();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for index in 0..archive.len() {
        writer
            .raw_copy_file(archive.by_index(index).unwrap())
            .unwrap();
    }
    const ENTRY: &[u8] = b"FICTIONAL-LEGACY-NAME";
    writer
        .start_file("FICTIONAL-LEGACY-NAME", SimpleFileOptions::default())
        .unwrap();
    let mut raw = writer.finish().unwrap().into_inner();
    let mut changed_headers = 0;
    // ZIP 9 types.rs::name tries UTF-8, then cp437::FromCp437; even the UTF-8
    // flag does not force rejection. Preserve that legacy decoding contract in
    // both headers, while leaving the actual HWPX content entries unchanged.
    for index in 0..raw.len().saturating_sub(46 + ENTRY.len()) {
        let (name, name_length) = match &raw[index..index + 4] {
            b"PK\x03\x04" => (30, 26),
            b"PK\x01\x02" => (46, 28),
            _ => continue,
        };
        let length = u16::from_le_bytes([raw[index + name_length], raw[index + name_length + 1]]);
        if usize::from(length) != ENTRY.len()
            || &raw[index + name..index + name + ENTRY.len()] != ENTRY
        {
            continue;
        }
        raw[index + name] = 0xff;
        changed_headers += 1;
    }
    assert_eq!(changed_headers, 2);
    let archive = zip::ZipArchive::new(Cursor::new(&raw)).unwrap();
    let names = archive.file_names().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(
        names
            .iter()
            .any(|name| name.as_ref() == "\u{00a0}ICTIONAL-LEGACY-NAME")
    );
    let source_sha256 = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let result = runtime.block_on(openlegal_document_worker::process(DocumentInput {
        format: DocumentFormat::Hwpx,
        raw,
        source_sha256,
        ocr: false,
    }));

    let output = result.unwrap();
    assert_eq!(output.text, baseline.text);
    assert_eq!(output.pages, baseline.pages);
    let actual: String = output.text.split_whitespace().collect();
    let expected: String = TEXT.split_whitespace().collect();
    assert!(actual.contains(&expected));
}
