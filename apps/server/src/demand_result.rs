//! Native result additions for bounded asynchronous demand collection.
use crate::registry::{
    CollectionReadFailure, RichToolOutput, RichToolResult, ToolError, ToolOptions, ToolOutput,
};
use openlegal_domain::{
    collection::{DemandCollectionState, DemandCollectionStatus},
    legal::DatabaseError,
    provider_admin::ProviderAdmissionSnapshot,
};
use rmcp::model::{ContentBlock, MetaObject};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::borrow::Cow;

pub(crate) const ADMISSION_META_KEY: &str = "openlegal/provider_admission";

/// Public status is a read-only observation, distinct from a receipt's earlier
/// settlement reason. Private recovery generations and ownership are omitted.
pub(crate) fn add_admission<T>(
    output: &mut RichToolOutput<T>,
    snapshot: Result<ProviderAdmissionSnapshot, DatabaseError>,
) {
    let Ok(snapshot) = snapshot else {
        if let Some(meta) = &mut output.output.meta {
            meta.0.remove(ADMISSION_META_KEY);
        }
        output.additional_content.push(ContentBlock::text(
            "Current provider admission status is unavailable; collection recovery is unknown. Retained results and earlier receipt reasons remain valid.",
        ));
        return;
    };
    let status = serde_json::json!({
        "version": snapshot.version,
        "observed_at": snapshot.observed_at,
        "flags": {
            "provider_recovery_hold": snapshot.recovery_hold,
            "operator_suspended": snapshot.operator_suspended,
            "provider_response_uncertain": snapshot.legacy_uncertain || snapshot.abandoned_slots > 0,
        },
        // Existing legacy flags and abandoned attempt start times do not prove
        // when provider collection actually stopped.
        "paused_since": snapshot.blocked_since,
        "flag_times": {
            "provider_recovery_hold": snapshot.hold_started_at,
            "operator_suspended": snapshot.operator_suspended_at,
            "provider_response_uncertain": null,
        },
        "uncertainty_first_observed_at": snapshot.first_observed_at,
        "continuous": snapshot.continuous,
        "on_demand": snapshot.on_demand,
        "retry_after_semantics": "status_recheck",
    });
    output
        .output
        .meta
        .get_or_insert_with(|| MetaObject(Default::default()))
        .0
        .insert(ADMISSION_META_KEY.into(), status);
    let review =
        snapshot.continuous.requires_operator_review || snapshot.on_demand.requires_operator_review;
    let explanation = if review {
        "Current provider collection requires operator review. Recheck times and receipt retry_after_seconds are status recheck intervals, not promised recovery times. Receipt reasons describe earlier observations."
    } else if !snapshot.continuous.ready || !snapshot.on_demand.ready {
        "Current provider collection is waiting for admission. Recheck times and receipt retry_after_seconds are status recheck intervals, not promised recovery times. Receipt reasons describe earlier observations."
    } else {
        "Current provider admission permits collection; this does not establish successful collection or complete corpus coverage. Receipt retry_after_seconds is a status recheck interval. Receipt reasons describe earlier observations."
    };
    output
        .additional_content
        .push(ContentBlock::text(explanation));
}

pub(crate) async fn admission_output<T>(
    output: ToolOutput<T>,
    store: &openlegal_adapters::corpus::PgCorpusStore,
) -> RichToolOutput<T> {
    let mut output = RichToolOutput {
        output,
        additional_content: Vec::new(),
    };
    add_admission(&mut output, store.provider_admission_snapshot().await);
    output
}

pub(crate) fn object_admission_output(
    mut output: ToolOutput<openlegal_domain::legal::ObjectStatus>,
    snapshot: Result<ProviderAdmissionSnapshot, DatabaseError>,
) -> RichToolOutput<openlegal_domain::legal::ObjectStatus> {
    use openlegal_domain::legal::{ObjectCollectionState, ObjectCompletionEta};
    if output.structured.state == ObjectCollectionState::ProcessingPending {
        match &snapshot {
            Ok(snapshot)
                if snapshot.recovery_hold
                    || snapshot.operator_suspended
                    || snapshot.legacy_uncertain
                    || snapshot.abandoned_slots > 0 =>
            {
                output.structured.eta = ObjectCompletionEta::Unknown {
                    reason: "provider_paused".into(),
                };
            }
            Err(_) if matches!(output.structured.eta, ObjectCompletionEta::Range { .. }) => {
                output.structured.eta = ObjectCompletionEta::Unknown {
                    reason: "provider_admission_unavailable".into(),
                };
            }
            _ => {}
        }
    }
    let mut result = RichToolOutput {
        output,
        additional_content: Vec::new(),
    };
    // The same observation governs the ETA qualification and both sidecars.
    add_admission(&mut result, snapshot);
    result
}

pub(crate) async fn admission_result<T>(
    result: Result<ToolOutput<T>, ToolError>,
    store: &openlegal_adapters::corpus::PgCorpusStore,
) -> RichToolResult<T> {
    match result {
        Ok(result) => RichToolResult::Ready(admission_output(result, store).await),
        Err(error) => admission_failure(error, store.provider_admission_snapshot().await),
    }
}

fn admission_failure<T>(
    error: ToolError,
    snapshot: Result<ProviderAdmissionSnapshot, DatabaseError>,
) -> RichToolResult<T> {
    let mut sidecars = RichToolOutput::new(());
    add_admission(&mut sidecars, snapshot);
    RichToolResult::Failure {
        error,
        meta: sidecars.output.meta,
        additional_content: sidecars.additional_content,
    }
}

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

    fn blocked_snapshot() -> ProviderAdmissionSnapshot {
        use openlegal_domain::provider_admin::ProviderModeDiagnostic;
        let mode = ProviderModeDiagnostic {
            reason: "provider_response_uncertain".into(),
            ready: false,
            recheck_at: Some(3700),
            requires_operator_review: true,
        };
        ProviderAdmissionSnapshot {
            version: 1,
            observed_at: 100,
            recovery_hold: true,
            hold_generation: Some("private-recovery-generation".into()),
            hold_started_at: Some(90),
            operator_suspended: false,
            operator_suspended_at: None,
            legacy_uncertain: false,
            abandoned_slots: 1,
            active_slots: 2,
            first_observed_at: Some(95),
            blocked_since: None,
            continuous: mode.clone(),
            on_demand: mode,
        }
    }

    #[test]
    fn admission_metadata_preserves_receipt_history_and_omits_private_fences() {
        let mut receipt = queued().receipt.unwrap();
        receipt.status = "deferred".into();
        receipt.retry_after_seconds = 17;
        let historical = serde_json::to_value(&receipt).unwrap();
        let mut result = RichToolOutput::new(receipt);
        result.output.meta = Some(MetaObject(serde_json::Map::from_iter([
            (
                "openlegal/collection".into(),
                serde_json::json!({"status":"pending"}),
            ),
            ("fixture-existing".into(), serde_json::json!(true)),
        ])));
        result
            .additional_content
            .push(ContentBlock::text("Existing qualification."));
        add_admission(&mut result, Ok(blocked_snapshot()));
        assert_eq!(
            serde_json::to_value(&result.output.structured).unwrap(),
            historical
        );
        let meta = &result.output.meta.as_ref().unwrap().0;
        assert_eq!(meta["fixture-existing"], true);
        assert_eq!(meta["openlegal/collection"]["status"], "pending");
        let snapshot = &meta[ADMISSION_META_KEY];
        assert_eq!(snapshot["flags"]["provider_response_uncertain"], true);
        assert_eq!(snapshot["flags"]["provider_recovery_hold"], true);
        assert!(snapshot["paused_since"].is_null());
        assert_eq!(snapshot["uncertainty_first_observed_at"], 95);
        assert_eq!(snapshot["flag_times"]["provider_recovery_hold"], 90);
        assert!(snapshot["flag_times"]["provider_response_uncertain"].is_null());
        assert_eq!(snapshot["retry_after_semantics"], "status_recheck");
        for private in [
            "hold_generation",
            "active_slots",
            "abandoned_slots",
            "legacy_uncertain",
        ] {
            assert!(snapshot.get(private).is_none());
        }
        assert!(
            !serde_json::to_string(snapshot)
                .unwrap()
                .contains("private-recovery-generation")
        );
        assert_eq!(result.additional_content.len(), 2);
        let text = serde_json::to_string(&result.additional_content[1]).unwrap();
        assert!(text.contains("requires operator review"));
        assert!(text.contains("not promised recovery"));
    }

    #[test]
    fn diagnostic_failure_preserves_results_and_never_reuses_healthy_metadata() {
        let mut output = RichToolOutput::new(ReadyFixture {
            schema_version: 1,
            title: "Retained source".into(),
        });
        output.output.meta = Some(MetaObject(serde_json::Map::from_iter([
            (ADMISSION_META_KEY.into(), serde_json::json!({"ready":true})),
            ("fixture-existing".into(), serde_json::json!(7)),
        ])));
        add_admission(&mut output, Err(DatabaseError::StorageUnavailable));
        assert_eq!(output.output.structured.title, "Retained source");
        let meta = &output.output.meta.as_ref().unwrap().0;
        assert!(meta.get(ADMISSION_META_KEY).is_none());
        assert_eq!(meta["fixture-existing"], 7);
        assert!(
            serde_json::to_string(&output.additional_content)
                .unwrap()
                .contains("recovery is unknown")
        );
    }

    #[test]
    fn admission_failure_keeps_original_head_error_when_diagnostics_are_unavailable() {
        let original = ToolError::CollectionUnavailable {
            cause: CollectionReadFailure::NotObserved,
        };
        for snapshot in [
            Ok(blocked_snapshot()),
            Err(DatabaseError::StorageUnavailable),
        ] {
            let available = snapshot.is_ok();
            let RichToolResult::Failure {
                error,
                meta,
                additional_content,
            } = admission_failure::<ReadyFixture>(original, snapshot)
            else {
                panic!("a failed HEAD read must remain a tool failure");
            };
            assert_eq!(error, original);
            assert_eq!(
                meta.as_ref()
                    .is_some_and(|meta| meta.0.contains_key(ADMISSION_META_KEY)),
                available
            );
            assert_eq!(additional_content.len(), 1);
            if !available {
                assert!(
                    serde_json::to_string(&additional_content)
                        .unwrap()
                        .contains("recovery is unknown")
                );
            }
        }
    }

    #[test]
    fn object_eta_and_sidecars_use_the_same_blocked_observation() {
        use openlegal_domain::legal::{
            Dataset, ObjectCollectionState, ObjectCompletionEta, ObjectId, ObjectJobStatus,
            ObjectStatus,
        };
        let object = ObjectStatus {
            schema_version: 1,
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: "001".into(),
            },
            state: ObjectCollectionState::ProcessingPending,
            head_capture_id: Some("a".repeat(64)),
            indexed: Some(true),
            job: Some(ObjectJobStatus {
                id: "job-fixture".into(),
                status: "running".into(),
                attempts: 1,
                created_at: 80,
                started_at: Some(90),
                completed_at: None,
                error_category: None,
            }),
            retry_at: None,
            eta: ObjectCompletionEta::Range {
                earliest_at: 110,
                latest_at: 120,
                sample_size: 8,
            },
        };
        let mut before = serde_json::to_value(&object).unwrap();
        before.as_object_mut().unwrap().remove("eta");
        let result =
            object_admission_output(ToolOutput::new(object.clone()), Ok(blocked_snapshot()));
        assert!(
            matches!(result.output.structured.eta, ObjectCompletionEta::Unknown { ref reason } if reason == "provider_paused")
        );
        assert_eq!(
            result.output.meta.as_ref().unwrap().0[ADMISSION_META_KEY]["flags"]["provider_response_uncertain"],
            true
        );
        let mut after = serde_json::to_value(result.output.structured).unwrap();
        after.as_object_mut().unwrap().remove("eta");
        assert_eq!(
            before, after,
            "retained object evidence and processing state stay intact"
        );
        let result = object_admission_output(
            ToolOutput::new(object.clone()),
            Err(DatabaseError::StorageUnavailable),
        );
        assert!(
            matches!(result.output.structured.eta, ObjectCompletionEta::Unknown { ref reason } if reason == "provider_admission_unavailable")
        );
        assert!(result.output.meta.is_none());
        let mut ready = blocked_snapshot();
        ready.recovery_hold = false;
        ready.abandoned_slots = 0;
        ready.continuous.ready = true;
        ready.on_demand.ready = true;
        let mut unknown = object;
        unknown.eta = ObjectCompletionEta::Unknown {
            reason: "insufficient_samples".into(),
        };
        let result = object_admission_output(ToolOutput::new(unknown), Ok(ready));
        assert!(
            matches!(result.output.structured.eta, ObjectCompletionEta::Unknown { ref reason } if reason == "insufficient_samples")
        );
    }

    #[test]
    fn rich_diagnostics_preserve_output_schema_and_builtin_side_effect_scope() {
        let mut plain = ToolRegistry::new();
        plain
            .register_demand::<QuerySearchRequest, ReadResult<ReadyFixture>, _, _>(
                "database.get_metadata",
                "Fixture",
                options(true),
                |_, _| async { Err(ToolError::Internal) },
            )
            .unwrap();
        let mut rich = ToolRegistry::new();
        rich.register_core_rich_result::<QuerySearchRequest, ReadResult<ReadyFixture>, _, _>(
            "database.get_metadata",
            "Fixture",
            options(true),
            |_, _| async { Err(ToolError::Internal) },
        )
        .unwrap();
        assert_eq!(
            plain.tools["database.get_metadata"]
                .definition
                .output_schema,
            rich.tools["database.get_metadata"].definition.output_schema
        );
        assert_eq!(
            plain.tools["database.get_metadata"].definition.annotations,
            rich.tools["database.get_metadata"].definition.annotations
        );
        assert!(
            rich.register_demand_rich::<QuerySearchRequest, ReadyFixture, _, _>(
                "extension.arbitrary_write",
                "Fixture",
                options(true),
                |_, _| async { Err(ToolError::Internal) },
            )
            .is_err()
        );
        assert!(
            rich.register_core_rich_result::<QuerySearchRequest, ReadyFixture, _, _>(
                "extension.arbitrary_write",
                "Fixture",
                options(true),
                |_, _| async { Err(ToolError::Internal) },
            )
            .is_err()
        );
        let mut explicit = ToolRegistry::new();
        explicit
            .register_collection_request::<QuerySearchRequest, CollectionReceipt, _, _>(
                |_, _| async { Err(ToolError::Internal) },
            )
            .unwrap();
        let tool = &explicit.tools["database.request_collection"].definition;
        assert_eq!(
            tool.output_schema.as_ref().unwrap().as_ref(),
            serde_json::to_value(schemars::schema_for!(CollectionReceipt))
                .unwrap()
                .as_object()
                .unwrap()
        );
        assert_eq!(
            tool.annotations.as_ref().unwrap().read_only_hint,
            Some(false)
        );
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
