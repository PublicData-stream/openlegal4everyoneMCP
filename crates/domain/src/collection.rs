//! Explicit, bounded requests for collecting public legal source material.
use crate::legal::{DatabaseError, Dataset, ObjectId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionRequest {
    pub target: CollectionTarget,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CollectionTarget {
    Object {
        object: ObjectId,
    },
    Search {
        mode: CollectionSearchMode,
        term: String,
        #[serde(default)]
        datasets: Vec<Dataset>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollectionSearchMode {
    Query,
    Literal,
}

impl CollectionRequest {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        match &self.target {
            CollectionTarget::Object { object } => {
                object.validate()?;
                if object.jurisdiction != "kr"
                    || object.provider != "law_go_kr"
                    || object.dataset != Dataset::NationalStatute
                    || !object.id.bytes().all(|b| b.is_ascii_digit())
                {
                    return Err(DatabaseError::InvalidInput);
                }
            }
            CollectionTarget::Search {
                mode,
                term,
                datasets,
            } => {
                if term.trim() != term
                    || term.is_empty()
                    || term.len() > 128
                    || term
                        .chars()
                        .any(|c| !(c.is_alphanumeric() || c == ' ' || c == '-'))
                    || datasets.len() > 8
                    || (matches!(mode, CollectionSearchMode::Query)
                        && term.split_whitespace().any(|word| {
                            matches!(word.to_ascii_uppercase().as_str(), "AND" | "OR" | "NOT")
                        }))
                    || datasets
                        .iter()
                        .enumerate()
                        .any(|(i, d)| datasets[..i].contains(d))
                {
                    return Err(DatabaseError::InvalidInput);
                }
            }
        }
        Ok(())
    }

    pub fn normalized(mut self) -> Self {
        if let CollectionTarget::Search { term, datasets, .. } = &mut self.target {
            *term = term.to_lowercase();
            if datasets.is_empty() {
                *datasets = vec![
                    Dataset::NationalStatute,
                    Dataset::AdministrativeRule,
                    Dataset::Ordinance,
                    Dataset::Treaty,
                    Dataset::Precedent,
                    Dataset::ConstitutionalDecision,
                    Dataset::LegalInterpretation,
                    Dataset::AdministrativeAppeal,
                ];
            }
            datasets.sort_by_key(|dataset| format!("{dataset:?}"));
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_requests_accept_only_provider_verifiable_bounded_inputs() {
        let object = ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::NationalStatute,
            id: "123".into(),
        };
        assert!(
            CollectionRequest {
                target: CollectionTarget::Object {
                    object: object.clone()
                }
            }
            .validate()
            .is_ok()
        );
        assert!(
            CollectionRequest {
                target: CollectionTarget::Object {
                    object: ObjectId {
                        dataset: Dataset::Treaty,
                        ..object
                    }
                }
            }
            .validate()
            .is_err()
        );
        for term in ["a.*", "x\ny", "", " x", "a OR b"] {
            let request = CollectionRequest {
                target: CollectionTarget::Search {
                    mode: CollectionSearchMode::Query,
                    term: term.into(),
                    datasets: vec![],
                },
            };
            assert!(request.validate().is_err(), "{term:?}");
        }
        let request = CollectionRequest {
            target: CollectionTarget::Search {
                mode: CollectionSearchMode::Literal,
                term: "민법".into(),
                datasets: vec![Dataset::NationalStatute, Dataset::NationalStatute],
            },
        };
        assert!(request.validate().is_err());
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionReceipt {
    pub request_id: String,
    pub status: String,
    pub retry_after_seconds: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CollectionStatusInput {
    pub request_id: String,
}
