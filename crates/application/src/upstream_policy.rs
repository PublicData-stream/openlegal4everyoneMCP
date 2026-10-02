//! Validated operator request limits, shared by independently configured providers.
use futures::future::BoxFuture;
use openlegal_domain::RetrievalError;
use serde::{Deserialize, Deserializer, Serialize};
use tokio_util::sync::CancellationToken;

/// A positive finite attempt limit or an explicit opt-in unlimited policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RequestLimit {
    Limited(u32),
    #[serde(serialize_with = "serialize_unlimited")]
    Unlimited,
}

fn serialize_unlimited<S: serde::Serializer>(serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("unlimited")
}

impl<'de> Deserialize<'de> for RequestLimit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Input {
            Number(u32),
            Text(String),
        }
        match Input::deserialize(deserializer)? {
            Input::Number(value) if (1..=1_000_000).contains(&value) => Ok(Self::Limited(value)),
            Input::Text(value) if value == "unlimited" => Ok(Self::Unlimited),
            _ => Err(serde::de::Error::custom(
                "expected 1..=1000000 or unlimited",
            )),
        }
    }
}

impl RequestLimit {
    pub fn validate(self) -> Result<(), RetrievalError> {
        match self {
            Self::Limited(value) if !(1..=1_000_000).contains(&value) => {
                Err(RetrievalError::InvalidInput)
            }
            _ => Ok(()),
        }
    }
    pub fn as_option(self) -> Option<u32> {
        match self {
            Self::Limited(value) => Some(value),
            Self::Unlimited => None,
        }
    }
    pub fn allows(self, used: u64) -> bool {
        self.as_option().is_none_or(|limit| used < u64::from(limit))
    }
}

/// Durable daily reservations. Implementations count committed attempts even if
/// later DNS, transport or processing fails, and never perform upstream I/O.
pub trait DailyBudgetStore: Send + Sync + 'static {
    fn configure(
        &self,
        namespace: String,
        provider: String,
        limit: RequestLimit,
    ) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn reserve(
        &self,
        namespace: String,
        provider: String,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>>;
}

/// Per-provider synthetic retrieval policy. Burst and concurrency remain two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DemoRequestPolicy {
    pub daily_limit: RequestLimit,
    pub requests_per_second: u32,
    pub max_attempts: u32,
    pub attempt_timeout_secs: u64,
    pub refresh_timeout_secs: u64,
}

impl Default for DemoRequestPolicy {
    fn default() -> Self {
        Self {
            daily_limit: RequestLimit::Unlimited,
            requests_per_second: 2,
            max_attempts: 2,
            attempt_timeout_secs: 5,
            refresh_timeout_secs: 10,
        }
    }
}

impl DemoRequestPolicy {
    pub fn validate(self) -> Result<(), RetrievalError> {
        self.daily_limit.validate()?;
        if !(1..=1_000).contains(&self.requests_per_second)
            || !(1..=10).contains(&self.max_attempts)
            || !(1..=60).contains(&self.attempt_timeout_secs)
            || !(1..=300).contains(&self.refresh_timeout_secs)
            || self.refresh_timeout_secs < self.attempt_timeout_secs
        {
            return Err(RetrievalError::InvalidInput);
        }
        Ok(())
    }
}
