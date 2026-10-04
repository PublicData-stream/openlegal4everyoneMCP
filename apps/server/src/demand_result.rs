//! Native result additions for bounded asynchronous demand collection.
use crate::registry::{CollectionReadFailure, ToolError, ToolOptions};
use openlegal_domain::{
    collection::{DemandCollectionState, DemandCollectionStatus},
    legal::DatabaseError,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::borrow::Cow;

#[derive(Serialize, JsonSchema)]
pub(crate) struct WithCollection<T> {
    #[serde(flatten)]
    pub result: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<DemandCollectionStatus>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct PendingCollection {
    pub schema_version: u32,
    pub state: String,
    pub reason: String,
    pub collection: DemandCollectionStatus,
}

#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum ReadResult<T> {
    Ready(WithCollection<T>),
    Pending(PendingCollection),
}
impl<T: JsonSchema> JsonSchema for ReadResult<T> {
    fn schema_name() -> Cow<'static, str> {
        format!("DemandRead_{}", T::schema_name()).into()
    }
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let ready = generator.subschema_for::<WithCollection<T>>();
        let pending = generator.subschema_for::<PendingCollection>();
        let mut object = serde_json::Map::new();
        object.insert("type".into(), serde_json::json!("object"));
        object.insert("anyOf".into(), serde_json::json!([ready, pending]));
        // Native citation descriptors are common optional properties of both
        // ready and pending objects; the registry augments this root map.
        object.insert("properties".into(), serde_json::json!({}));
        object.into()
    }
}

pub(crate) fn options(enabled: bool) -> ToolOptions {
    let mut options = ToolOptions::default();
    options.annotations.read_only_hint = Some(!enabled);
    options
}

pub(crate) fn refreshable_error(error: DatabaseError) -> bool {
    matches!(
        error,
        DatabaseError::NotObserved
            | DatabaseError::ProcessingPending
            | DatabaseError::CollectionIncomplete
            | DatabaseError::FreshnessUnavailable
    )
}

pub(crate) fn pending<T>(
    error: DatabaseError,
    collection: DemandCollectionStatus,
) -> Result<ReadResult<T>, ToolError> {
    if collection.status == DemandCollectionState::Pending && collection.receipt.is_some() {
        let reason = serde_json::to_value(error)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "unavailable".into());
        return Ok(ReadResult::Pending(PendingCollection {
            schema_version: 1,
            state: "pending".into(),
            reason,
            collection,
        }));
    }
    if collection.status == DemandCollectionState::Unavailable {
        let cause = match error {
            DatabaseError::NotObserved => CollectionReadFailure::NotObserved,
            DatabaseError::ProcessingPending => CollectionReadFailure::ProcessingPending,
            DatabaseError::CollectionIncomplete => CollectionReadFailure::CollectionIncomplete,
            DatabaseError::FreshnessUnavailable => CollectionReadFailure::FreshnessUnavailable,
            _ => return Err(crate::database::map_error(error)),
        };
        return Err(ToolError::CollectionUnavailable { cause });
    }
    Err(crate::database::map_error(error))
}

/// Keep collection_term outside local search identity and cursor persistence.
#[derive(JsonSchema)]
pub(crate) struct CollectionSearchInput<T> {
    #[schemars(flatten)]
    pub request: T,
    pub collection_term: Option<String>,
}
impl<'de, T: DeserializeOwned> Deserialize<'de> for CollectionSearchInput<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| serde::de::Error::custom("expected object"))?;
        let collection_term = object
            .remove("collection_term")
            .map(serde_json::from_value::<Option<String>>)
            .transpose()
            .map_err(serde::de::Error::custom)?
            .flatten();
        let request = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(Self {
            request,
            collection_term,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ToolOutput, ToolRegistry};
    use openlegal_domain::collection::CollectionReceipt;
    use openlegal_domain::legal_search::QuerySearchRequest;

    #[derive(Serialize, JsonSchema)]
    struct ReadyFixture {
        schema_version: u32,
        title: String,
    }

    fn queued() -> DemandCollectionStatus {
        DemandCollectionStatus {
            status: DemandCollectionState::Pending,
            receipt: Some(CollectionReceipt {
                request_id: "01900000-0000-7000-8000-000000000001".into(),
                status: "queued".into(),
                retry_after_seconds: 10,
                reason: None,
            }),
            reason: None,
        }
    }

    #[test]
    fn ready_and_pending_outputs_validate_after_citation_reference_schema_augmentation() {
        let mut registry = ToolRegistry::new();
        registry
            .register_demand::<QuerySearchRequest, ReadResult<ReadyFixture>, _, _>(
                "database.get_metadata",
                "Demand read schema fixture",
                options(true),
                |_, _| async { Err(ToolError::Internal) },
            )
            .unwrap();
        registry.enable_citation_references().unwrap();
        let tool = registry.tools.get("database.get_metadata").unwrap();
        let validator = tool.output_validator.as_ref().unwrap();
        assert!(
            tool.definition.output_schema.as_ref().unwrap()["properties"]
                .get("references")
                .is_some()
        );
        let ready = ReadResult::Ready(WithCollection {
            result: ReadyFixture {
                schema_version: 1,
                title: "Fictional statute".into(),
            },
            collection: Some(queued()),
        });
        let pending = pending::<ReadyFixture>(DatabaseError::NotObserved, queued()).unwrap();
        let descriptor = serde_json::json!({
            "id": "capture-fixed-fixture", "uri": "openlegal://source/fixture", "url": "https://example.test/source/fixture", "title": "Fictional source", "body_available": true, "official_url": null,
        });
        for output in [ready, pending] {
            let mut structured = serde_json::to_value(output).unwrap();
            structured["references"] = serde_json::json!([descriptor.clone()]);
            assert!(
                validator.is_valid(&structured),
                "valid ready/pending output: {structured}"
            );
            let mut invalid_reference = structured.clone();
            invalid_reference["references"][0]["id"] = serde_json::json!(123);
            assert!(!validator.is_valid(&invalid_reference));
            structured["collection"]["status"] = serde_json::json!("invalid_status");
            assert!(!validator.is_valid(&structured));
        }
    }

    #[test]
    fn enabled_and_disabled_builtins_advertise_their_collection_side_effect_accurately() {
        for enabled in [false, true] {
            let mut registry = ToolRegistry::new();
            registry
                .register_demand::<QuerySearchRequest, ReadyFixture, _, _>(
                    "database.query",
                    "Collection annotation fixture",
                    options(enabled),
                    |_, _| async {
                        Ok(ToolOutput::new(ReadyFixture {
                            schema_version: 1,
                            title: "Fixture".into(),
                        }))
                    },
                )
                .unwrap();
            let annotations = registry.tools["database.query"]
                .definition
                .annotations
                .as_ref()
                .unwrap();
            assert_eq!(annotations.read_only_hint, Some(!enabled));
            assert_eq!(annotations.destructive_hint, Some(false));
            assert!(
                registry
                    .register_demand::<QuerySearchRequest, ReadyFixture, _, _>(
                        "extension.arbitrary_write",
                        "Unregistered write fixture",
                        options(enabled),
                        |_, _| async { Err(ToolError::Internal) },
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn pending_never_hides_collection_admission_failure_or_manufactures_a_receipt() {
        let mut collection = queued();
        collection.receipt = None;
        assert!(matches!(
            pending::<ReadyFixture>(DatabaseError::NotObserved, collection),
            Err(ToolError::NotObserved)
        ));
        let unavailable =
            DemandCollectionStatus::reason(DemandCollectionState::Unavailable, "capacity");
        assert!(matches!(
            pending::<ReadyFixture>(DatabaseError::FreshnessUnavailable, unavailable),
            Err(ToolError::CollectionUnavailable {
                cause: CollectionReadFailure::FreshnessUnavailable
            })
        ));
        assert!(!refreshable_error(DatabaseError::Withdrawn));
        assert!(!refreshable_error(DatabaseError::RevisionUnavailable));
        assert!(!refreshable_error(DatabaseError::StorageUnavailable));
    }
    #[test]
    fn collection_term_preserves_local_input_validation() {
        let request: CollectionSearchInput<QuerySearchRequest> =
            serde_json::from_value(serde_json::json!({"query":"A OR B", "collection_term":"민법"}))
                .unwrap();
        assert_eq!(request.request.query, "A OR B");
        assert_eq!(request.collection_term.as_deref(), Some("민법"));
        for extra in [
            serde_json::json!({"query":"A","context_lines":0}),
            serde_json::json!({"query":"A","collection_term":42}),
            serde_json::json!({"query":"A","collection_term":"민법","unknown":true}),
        ] {
            assert!(
                serde_json::from_value::<CollectionSearchInput<QuerySearchRequest>>(extra).is_err()
            );
        }
        let input: CollectionSearchInput<QuerySearchRequest> =
            serde_json::from_value(serde_json::json!({"query":"A","collection_term":"invalid.*"}))
                .unwrap();
        assert!(
            openlegal_application::demand_collection::validate_collection_term(
                input.collection_term.as_deref(),
                &input.request.filters.datasets
            )
            .is_err()
        );
        let null_term: CollectionSearchInput<QuerySearchRequest> =
            serde_json::from_value(serde_json::json!({"query":"A","collection_term":null}))
                .unwrap();
        assert!(null_term.collection_term.is_none());
    }
}
