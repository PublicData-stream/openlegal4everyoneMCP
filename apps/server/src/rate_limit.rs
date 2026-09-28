//! Fixed-size process-local token bucket for valid MCP tool calls.

use crate::config::VerifiedTunnelRateLimitConfig;
use tokio::{sync::Mutex, time::Instant};

const TOKEN: u128 = 1_000_000_000;

struct Bucket {
    tokens: u128,
    at: Instant,
}

pub(crate) struct RateLimiter {
    enabled: bool,
    calls_per_second: u32,
    burst: u32,
    bucket: Mutex<Bucket>,
}

impl RateLimiter {
    pub(crate) fn new(enabled: bool, calls_per_second: u32, burst: u32) -> Self {
        Self {
            bucket: Mutex::new(Bucket {
                tokens: u128::from(burst) * TOKEN,
                at: Instant::now(),
            }),
            enabled,
            calls_per_second,
            burst,
        }
    }

    pub(crate) fn verified_tunnel(config: &VerifiedTunnelRateLimitConfig) -> Self {
        Self::new(true, config.calls_per_second, config.burst)
    }

    pub(crate) async fn try_admit(&self) -> bool {
        if !self.enabled {
            return true;
        }
        let mut bucket = self.bucket.lock().await;
        let now = Instant::now();
        let refill = now
            .saturating_duration_since(bucket.at)
            .as_nanos()
            .saturating_mul(u128::from(self.calls_per_second));
        bucket.tokens = bucket
            .tokens
            .saturating_add(refill)
            .min(u128::from(self.burst) * TOKEN);
        bucket.at = now;
        if bucket.tokens < TOKEN {
            return false;
        }
        bucket.tokens -= TOKEN;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn burst_and_fractional_refill_are_exact() {
        let limiter = RateLimiter::new(true, 2, 3);
        for _ in 0..3 {
            assert!(limiter.try_admit().await);
        }
        assert!(!limiter.try_admit().await);
        tokio::time::advance(Duration::from_millis(499)).await;
        assert!(!limiter.try_admit().await);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(limiter.try_admit().await);
        assert!(!limiter.try_admit().await);
        tokio::time::advance(Duration::from_secs(100)).await;
        for _ in 0..3 {
            assert!(limiter.try_admit().await);
        }
        assert!(!limiter.try_admit().await);
    }

    #[tokio::test]
    async fn disabled_limiter_never_consumes_tokens() {
        let limiter = RateLimiter::new(false, 1, 1);
        for _ in 0..100 {
            assert!(limiter.try_admit().await);
        }
    }
}
