//! Synthetic rows only; no client, provider request, database or real legal body.
use super::*;

fn item(id: &str, revision: &str) -> InventoryItem {
    InventoryItem {
        object: ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::SchoolRule,
            id: id.into(),
        },
        revision_id: revision.into(),
        effective_date: Some("20260101".into()),
        publication_date: None,
        title: "Fictional school rule".into(),
        data_source: None,
        case_number: None,
        treaty_class_code: None,
        amendment_type: None,
    }
}
fn page(items: Vec<InventoryItem>) -> InventoryPage {
    InventoryPage {
        source_evidence: None,
        source_observation: None,
        total: Some(items.len() as u64),
        items,
        done: true,
        rejected_rows: 0,
        incomplete: false,
    }
}

#[test]
fn metadata_rows_are_canonical_complete_and_exclude_descriptive_text() {
    let mut first = item("0007", "00100");
    first.title = "not-a-credential-do-not-emit-title".into();
    first.data_source = Some("not-a-credential-do-not-emit-source".into());
    first.case_number = Some("not-a-credential-do-not-emit-case".into());
    let mut second = first.clone();
    second.publication_date = Some("20251231".into());
    let result = page(vec![first.clone(), first, second]);
    let metadata = result.current_revision_metadata().unwrap();
    assert_eq!(metadata.len(), 4);
    assert_eq!(metadata["current_revision_rows"], "3");
    assert_eq!(
        metadata["current_revision_000"],
        "{\"effective_date\":\"20260101\",\"object\":{\"dataset\":\"school_rule\",\"id\":\"0007\",\"jurisdiction\":\"kr\",\"provider\":\"law_go_kr\"},\"publication_date\":null,\"revision_id\":\"00100\"}"
    );
    assert_eq!(
        metadata["current_revision_000"],
        metadata["current_revision_001"]
    );
    let row: serde_json::Value = serde_json::from_str(&metadata["current_revision_002"]).unwrap();
    assert_eq!(row["publication_date"], "20251231");
    assert!(
        metadata
            .values()
            .all(|value| !value.contains("do-not-emit"))
    );
    assert!(result.conflicting_current_objects().is_empty());
}

#[test]
fn all_distinct_checkpoints_conflict_without_date_or_order_heuristics() {
    let first = item("7", "200");
    for (effective, publication) in [
        (Some("20250101"), Some("20270101")),
        (Some("20260101"), None),
        (Some("20270101"), Some("20250101")),
        (None, None),
    ] {
        let mut second = item("7", "100");
        second.effective_date = effective.map(str::to_owned);
        second.publication_date = publication.map(str::to_owned);
        for rows in [
            vec![first.clone(), second.clone()],
            vec![second, first.clone()],
        ] {
            assert_eq!(
                page(rows).conflicting_current_objects(),
                vec![first.object.clone()]
            );
        }
    }
    let mut same_serial_different_effective = first.clone();
    same_serial_different_effective.effective_date = Some("20260102".into());
    assert_eq!(
        page(vec![first.clone(), same_serial_different_effective]).conflicting_current_objects(),
        vec![first.object]
    );
}

#[test]
fn exact_checkpoint_duplicates_and_publication_changes_are_benign() {
    let first = item("7", "100");
    let mut correction = first.clone();
    correction.publication_date = Some("20250101".into());
    correction.title = "Different fictional descriptive metadata".into();
    let result = page(vec![first.clone(), first, correction]);
    assert!(result.conflicting_current_objects().is_empty());
    assert_eq!(
        result.current_revision_metadata().unwrap()["current_revision_rows"],
        "3"
    );
}

#[test]
fn conflict_results_follow_first_object_appearance_not_conflict_discovery() {
    let a = item("7", "100");
    let b = item("8", "100");
    let result = page(vec![
        a.clone(),
        b.clone(),
        item("8", "200"),
        item("7", "200"),
        item("9", "300"),
    ]);
    assert_eq!(
        result.conflicting_current_objects(),
        vec![a.object, b.object]
    );
}

#[test]
fn namespaces_remain_distinct_even_when_display_ids_and_titles_match() {
    let first = item("7", "100");
    let mut provider = item("7", "200");
    provider.object.provider = "fictional_other".into();
    let mut jurisdiction = item("7", "200");
    jurisdiction.object.jurisdiction = "fictional".into();
    let mut dataset = item("7", "200");
    dataset.object.dataset = Dataset::PublicInstitutionRule;
    let result = page(vec![first, provider.clone(), jurisdiction, dataset]);
    assert!(result.conflicting_current_objects().is_empty());
    let mut rows = result.items;
    let mut provider_second = provider.clone();
    provider_second.revision_id = "300".into();
    rows.push(provider_second);
    assert_eq!(
        page(rows).conflicting_current_objects(),
        vec![provider.object]
    );
}

#[test]
fn metadata_limits_accept_empty_and_full_pages_without_truncation() {
    let empty = page(Vec::new()).current_revision_metadata().unwrap();
    assert_eq!(
        empty,
        BTreeMap::from([("current_revision_rows".into(), "0".into())])
    );
    let items = (1..=100)
        .map(|index| {
            let mut row = item(&format!("{index:0128}"), &format!("{:0128}", index + 100));
            row.title = "x".repeat(512);
            row.publication_date = Some("20251231".into());
            row
        })
        .collect();
    let full = page(items);
    let metadata = full.current_revision_metadata().unwrap();
    assert_eq!(metadata.len(), 101);
    assert_eq!(metadata["current_revision_rows"], "100");
    assert!(metadata.contains_key("current_revision_000"));
    assert!(metadata.contains_key("current_revision_099"));
    assert!(!metadata.contains_key("current_revision_100"));
    assert!(metadata.values().all(|value| value.len() <= 4096));
    assert!(serde_json::to_vec(&metadata).unwrap().len() <= 60 * 1024);
    let mut too_many = full.items;
    too_many.push(item("101", "201"));
    assert_eq!(
        page(too_many).current_revision_metadata(),
        Err(DatabaseError::InvalidInput)
    );
}

#[test]
fn metadata_rejects_invalid_typed_rows_instead_of_dropping_them() {
    let mut invalids = Vec::new();
    let mut invalid = item("7", "100");
    invalid.object.provider = "other".into();
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.object.jurisdiction = "us".into();
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.object.id = "1".repeat(129);
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.revision_id = "1".repeat(4097);
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.effective_date = Some("20260230".into());
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.publication_date = Some("20260230".into());
    invalids.push(invalid);
    let mut invalid = item("7", "100");
    invalid.object.dataset = Dataset::NationalStatute;
    invalid.revision_id = "100:20260102".into();
    invalids.push(invalid);
    for invalid in invalids {
        assert_eq!(
            page(vec![item("8", "200"), invalid]).current_revision_metadata(),
            Err(DatabaseError::InvalidInput)
        );
    }
}

#[test]
fn oversized_excluded_descriptions_do_not_reject_identity_evidence() {
    let ordinary = item("7", "100");
    let mut oversized = ordinary.clone();
    oversized.title = "not-a-credential-excluded-title".repeat(200);
    oversized.data_source = Some("not-a-credential-excluded-source".repeat(200));
    oversized.case_number = Some("not-a-credential-excluded-case".repeat(200));
    oversized.amendment_type = Some("not-a-credential-excluded-amendment".repeat(200));
    assert_eq!(
        oversized.validate_for_detail(),
        Err(DatabaseError::InvalidInput)
    );
    let metadata = page(vec![oversized]).current_revision_metadata().unwrap();
    assert_eq!(
        metadata,
        page(vec![ordinary]).current_revision_metadata().unwrap()
    );
    assert!(metadata.values().all(|value| !value.contains("excluded")));
}

#[test]
fn helper_calls_preserve_all_historical_rows_and_page_evidence() {
    let mut result = page(vec![item("7", "100"), item("7", "200"), item("8", "300")]);
    result.source_observation = Some(crate::corpus::SourceObservation {
        observation_id: "a".repeat(64),
        source_key: "law_go_kr:fictional:history".into(),
        raw_sha256: None,
        raw_size: 0,
        media_type: "application/xml".into(),
        rights: SourceRights::default(),
        metadata: BTreeMap::from([("historical".into(), "true".into())]),
        observed_at: 1,
        validated_at: 1,
    });
    let before = serde_json::to_value(&result.items).unwrap();
    let observation = result.source_observation.clone();
    let _ = result.current_revision_metadata().unwrap();
    let _ = result.conflicting_current_objects();
    assert_eq!(serde_json::to_value(&result.items).unwrap(), before);
    assert_eq!(result.source_observation, observation);
    assert_eq!(result.items.len(), 3);
    assert!(result.done && !result.incomplete);
}
