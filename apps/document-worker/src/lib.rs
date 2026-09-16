//! Parser code is linked only into the disposable document worker.
use openlegal_application::document::*;
use sha2::{Digest, Sha256};

#[cfg(feature = "documents")]
mod documents;

pub async fn process(input: DocumentInput) -> Result<DocumentOutput, DocumentError> {
    if input.raw.is_empty()
        || input.raw.len() > MAX_DOCUMENT_BYTES
        || input.source_sha256
            != Sha256::digest(&input.raw)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
    {
        return Err(DocumentError::InvalidInput);
    }
    let mut output = DocumentOutput {
        source_sha256: input.source_sha256.clone(),
        processor_version: "document-v1;xml=roxmltree-0.21.1;html=scraper-0.27.0".into(),
        format: input.format,
        tree: None,
        text: String::new(),
        pages: vec![],
        ocr_pages: vec![],
        diagnostics: vec![],
    };
    match input.format {
        DocumentFormat::Xml => {
            let source =
                std::str::from_utf8(&input.raw).map_err(|_| DocumentError::InvalidDocument)?;
            let document = roxmltree::Document::parse_with_options(
                source,
                roxmltree::ParsingOptions {
                    allow_dtd: false,
                    nodes_limit: MAX_DOCUMENT_NODES as u32,
                    ..Default::default()
                },
            )
            .map_err(|_| DocumentError::InvalidDocument)?;
            let mut count = 0;
            output.tree = Some(xml_node(
                document.root_element(),
                1,
                &mut count,
                &mut output.text,
            )?);
        }
        DocumentFormat::Html => {
            let source =
                std::str::from_utf8(&input.raw).map_err(|_| DocumentError::InvalidDocument)?;
            let document = scraper::Html::parse_document(source);
            let mut count = 0;
            output.tree = Some(html_node(
                document.root_element(),
                1,
                &mut count,
                &mut output.text,
            )?);
            output.diagnostics.push("html_inert_text_projection".into());
        }
        _ => {
            #[cfg(feature = "documents")]
            {
                documents::extract(&input, &mut output).await?;
            }
            #[cfg(not(feature = "documents"))]
            {
                return Err(DocumentError::UnsupportedFormat);
            }
        }
    }
    validate_output(&output, &input)?;
    Ok(output)
}

fn admit(depth: usize, count: &mut usize) -> Result<(), DocumentError> {
    *count += 1;
    // The tagged mixed-content JSON tree uses several serialization levels per
    // element. 32 keeps it below serde_json's default 128-level reader limit.
    if depth > 32 || *count > MAX_DOCUMENT_NODES {
        return Err(DocumentError::ResourceLimit);
    }
    Ok(())
}

fn append_text(target: &mut String, value: &str) -> Result<(), DocumentError> {
    if target.len().saturating_add(value.len()).saturating_add(1) > MAX_DOCUMENT_OUTPUT_BYTES / 2 {
        return Err(DocumentError::ResourceLimit);
    }
    target.push_str(value);
    Ok(())
}

fn xml_node(
    node: roxmltree::Node<'_, '_>,
    depth: usize,
    count: &mut usize,
    text: &mut String,
) -> Result<DocumentNode, DocumentError> {
    admit(depth, count)?;
    if node.is_text() {
        let value = node.text().unwrap_or_default().to_owned();
        append_text(text, &value)?;
        return Ok(DocumentNode::Text { value });
    }
    let tag = node.tag_name();
    let name = expanded(tag.namespace(), tag.name());
    let attributes = node
        .attributes()
        .map(|a| (expanded(a.namespace(), a.name()), a.value().to_owned()))
        .collect();
    let mut children = Vec::new();
    for child in node
        .children()
        .filter(|node| node.is_element() || node.is_text())
    {
        children.push(xml_node(child, depth + 1, count, text)?);
    }
    if !children.is_empty() {
        append_text(text, "\n")?;
    }
    Ok(DocumentNode::Element {
        name,
        attributes,
        children,
    })
}

fn expanded(namespace: Option<&str>, local: &str) -> String {
    namespace.map_or_else(|| local.into(), |ns| format!("{{{ns}}}{local}"))
}

fn html_node(
    element: scraper::ElementRef<'_>,
    depth: usize,
    count: &mut usize,
    text: &mut String,
) -> Result<DocumentNode, DocumentError> {
    admit(depth, count)?;
    let name = element.value().name().to_owned();
    let attributes = element
        .value()
        .attrs()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let mut children = Vec::new();
    if !matches!(
        name.as_str(),
        "script" | "style" | "iframe" | "object" | "embed" | "noscript"
    ) {
        for node in element.children() {
            if let Some(child) = scraper::ElementRef::wrap(node) {
                children.push(html_node(child, depth + 1, count, text)?);
            } else if let Some(value) = node.value().as_text() {
                admit(depth + 1, count)?;
                append_text(text, value)?;
                children.push(DocumentNode::Text {
                    value: value.to_string(),
                });
            }
        }
        if matches!(
            name.as_str(),
            "p" | "div" | "br" | "li" | "tr" | "h1" | "h2" | "h3" | "section" | "article"
        ) {
            append_text(text, "\n")?;
        }
    }
    Ok(DocumentNode::Element {
        name,
        attributes,
        children,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(format: DocumentFormat, raw: &[u8]) -> DocumentInput {
        DocumentInput {
            format,
            source_sha256: Sha256::digest(raw)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            raw: raw.to_vec(),
            ocr: false,
        }
    }
    #[tokio::test]
    async fn xml_retains_repeated_fields_and_leading_zero_identifiers() {
        let result = process(input(
            DocumentFormat::Xml,
            include_bytes!("../tests/fixtures/law.xml"),
        ))
        .await
        .unwrap();
        let DocumentNode::Element { children, .. } = result.tree.unwrap() else {
            panic!("element required")
        };
        assert!(children.iter().any(|node| matches!(node, DocumentNode::Element { name, children, .. } if name == "법령ID" && children == &[DocumentNode::Text { value: "000001".into() }])));
        assert!(result.text.contains("가상 조문"));
    }
    #[tokio::test]
    async fn rejects_external_entities_and_deep_xml() {
        let external = b"<!DOCTYPE x [<!ENTITY data SYSTEM 'file:///etc/passwd'>]><x>&data;</x>";
        assert!(process(input(DocumentFormat::Xml, external)).await.is_err());
        let deep = format!("{}value{}", "<x>".repeat(40), "</x>".repeat(40));
        assert!(
            process(input(DocumentFormat::Xml, deep.as_bytes()))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn html_preserves_identity_but_does_not_emit_active_content() {
        let result = process(input(
            DocumentFormat::Html,
            include_bytes!("../tests/fixtures/precedent.html"),
        ))
        .await
        .unwrap();
        assert!(result.text.contains("가상 판결"));
        assert!(!result.text.contains("fetch("));
        assert!(
            serde_json::to_string(&result.tree)
                .unwrap()
                .contains("000042")
        );
    }
}
