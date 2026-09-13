use super::*;
use openlegal_domain::{Provenance, Record, SearchPage, history::SnapshotReference};

fn key() -> PersistentKey {
    PersistentKey {
        history: HistoryKey {
            namespace: "fixture".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "mock".into(),
                id: "001".into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    }
}

fn payload() -> StoredPayload {
    StoredPayload {
        data: RetrievalData::Get(Record {
            source: "mock".into(),
            id: "001".into(),
            title: "Fiction".into(),
            body: "Synthetic source".into(),
            synthetic: true,
        }),
        provenance: Provenance {
            provider: "synthetic".into(),
            dataset: "records".into(),
            source_reference: "https://example.test/fixture".into(),
            payload_sha256: digest_hex(b"fixture"),
            processor_version: "v1".into(),
            retrieved_at: u64::MAX - 1,
            validated_at: u64::MAX,
        },
        raw: b"fixture".to_vec(),
        bytes: 2048,
        snapshot: Some(SnapshotReference {
            snapshot_id: "a".repeat(64),
            captured_at: u64::MAX,
        }),
    }
}

#[test]
fn identity_preserves_every_query_dimension_and_unambiguous_fields() {
    let base = key().history;
    let mut variants = vec![base.clone(); 5];
    variants[0].namespace = "other".into();
    variants[1].provider = "other".into();
    variants[2].dataset = "other".into();
    variants[3].query = Query::Get {
        source: "other".into(),
        id: "001".into(),
    };
    variants[4].query = Query::Get {
        source: "mock".into(),
        id: "1".into(),
    };
    for variant in variants {
        assert_ne!(
            canonical_identity(&base).unwrap(),
            canonical_identity(&variant).unwrap()
        );
    }
    let mut left = base.clone();
    left.provider = "a".into();
    left.dataset = "bc".into();
    let mut right = base.clone();
    right.provider = "ab".into();
    right.dataset = "c".into();
    assert_ne!(
        identity_digest(&left).unwrap(),
        identity_digest(&right).unwrap()
    );
    let mut digests = HashSet::new();
    for query in ["law", "Law", " law", "law "] {
        for page in 0..2 {
            for page_size in 1..3 {
                let mut search = base.clone();
                search.query = Query::Search {
                    source: "mock".into(),
                    query: query.into(),
                    page,
                    page_size,
                };
                assert!(digests.insert(identity_digest(&search).unwrap()));
            }
        }
    }
}

#[test]
fn full_unsigned_times_are_preserved_and_capture_metadata_is_checked() {
    let key = key();
    let mut value = payload();
    validate_payload(&key, &value).unwrap();
    let original = immutable_payload_digest(&key, &value).unwrap();
    value.provenance.retrieved_at -= 1;
    assert_ne!(original, immutable_payload_digest(&key, &value).unwrap());
    value.provenance.retrieved_at += 1;
    value.snapshot.as_mut().unwrap().captured_at -= 1;
    assert_ne!(original, immutable_payload_digest(&key, &value).unwrap());
    value.snapshot.as_mut().unwrap().captured_at += 1;
    value.snapshot.as_mut().unwrap().snapshot_id = "b".repeat(64);
    assert_ne!(original, immutable_payload_digest(&key, &value).unwrap());
    value.provenance.source_reference.push_str("?secret=hidden");
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
}

#[test]
fn restored_evidence_rejects_corruption_wrong_identity_and_incomplete_pages() {
    let mut key = key();
    let mut value = payload();
    value.raw.push(0);
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
    value.raw.pop();
    let RetrievalData::Get(mut record) = value.data.clone() else {
        panic!()
    };
    record.id = "other".into();
    value.data = RetrievalData::Get(record.clone());
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
    key.history.query = Query::Search {
        source: "mock".into(),
        query: "".into(),
        page: 0,
        page_size: 2,
    };
    value.data = RetrievalData::Search(SearchPage {
        records: vec![record.clone()],
        page: 0,
        page_size: 2,
        total: 2,
    });
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
    let RetrievalData::Search(page) = &mut value.data else {
        panic!()
    };
    page.records.push(record);
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
    let RetrievalData::Search(page) = &mut value.data else {
        panic!()
    };
    page.records[1].id = "distinct".into();
    validate_payload(&key, &value).unwrap();
    key.schema_version = 2;
    assert_eq!(
        validate_payload(&key, &value),
        Err(RetrievalError::StorageCorrupt)
    );
}

#[test]
fn processed_encoding_is_stable_after_json_object_order_changes_and_bounded() {
    let mut value = payload();
    let encoded = processed_bytes(&value.data).unwrap();
    let restored: RetrievalData =
        serde_json::from_value(serde_json::to_value(&value.data).unwrap()).unwrap();
    assert_eq!(encoded, processed_bytes(&restored).unwrap());
    let RetrievalData::Get(record) = &mut value.data else {
        panic!()
    };
    record.body = "x".repeat(MAX_PROCESSED_BYTES);
    assert_eq!(
        processed_bytes(&value.data),
        Err(RetrievalError::ResourceLimit)
    );
}
