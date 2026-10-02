use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::header::HeaderMap;

// Quota is a recent observation, not an estimate of future replenishment.
const MAX_QUOTA_AGE: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub(crate) enum QuotaHeaderFamily {
    OpenAI,
    Anthropic,
}

struct QuotaObservation {
    percent: u8,
    observed_at: Instant,
}

#[derive(Default)]
struct QuotaState {
    request_generation: u64,
    observation: Option<QuotaObservation>,
}

#[derive(Clone, Default)]
pub(crate) struct QuotaTelemetry {
    state: Arc<Mutex<QuotaState>>,
}

impl QuotaTelemetry {
    pub(crate) fn begin_request(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .expect("quota mutex should not be poisoned");
        state.request_generation = state.request_generation.wrapping_add(1);
        state.observation = None;
        state.request_generation
    }

    pub(crate) fn update(
        &self,
        request_generation: u64,
        headers: &HeaderMap,
        family: QuotaHeaderFamily,
    ) {
        let windows: &[(&str, &str)] = match family {
            QuotaHeaderFamily::OpenAI => &[
                (
                    "x-ratelimit-limit-requests",
                    "x-ratelimit-remaining-requests",
                ),
                ("x-ratelimit-limit-tokens", "x-ratelimit-remaining-tokens"),
            ],
            QuotaHeaderFamily::Anthropic => &[
                (
                    "anthropic-ratelimit-requests-limit",
                    "anthropic-ratelimit-requests-remaining",
                ),
                (
                    "anthropic-ratelimit-tokens-limit",
                    "anthropic-ratelimit-tokens-remaining",
                ),
                (
                    "anthropic-ratelimit-input-tokens-limit",
                    "anthropic-ratelimit-input-tokens-remaining",
                ),
                (
                    "anthropic-ratelimit-output-tokens-limit",
                    "anthropic-ratelimit-output-tokens-remaining",
                ),
            ],
        };
        let percent = windows
            .iter()
            .filter_map(|(limit, remaining)| {
                let limit = headers.get(*limit)?.to_str().ok()?.parse::<u64>().ok()?;
                let remaining = headers
                    .get(*remaining)?
                    .to_str()
                    .ok()?
                    .parse::<u64>()
                    .ok()?;
                if limit == 0 {
                    return None;
                }
                Some((u128::from(remaining.min(limit)) * 100 / u128::from(limit)) as u8)
            })
            .min();
        let mut state = self
            .state
            .lock()
            .expect("quota mutex should not be poisoned");
        // An older response must not replace a later request's low or unavailable quota.
        if request_generation != state.request_generation {
            return;
        }
        state.observation = percent.map(|percent| QuotaObservation {
            percent,
            observed_at: Instant::now(),
        });
    }

    pub(crate) fn remaining_quota_percent(&self) -> Option<u8> {
        self.state
            .lock()
            .expect("quota mutex should not be poisoned")
            .observation
            .as_ref()
            .filter(|observation| observation.observed_at.elapsed() < MAX_QUOTA_AGE)
            .map(|observation| observation.percent)
    }
}
#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::time::{Duration, Instant};

    use super::*;

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: quota header windows reject invalid limits and clamp remaining.
    #[test]
    fn quota_header_windows_reject_invalid_limits_and_clamp_remaining() {
        let cases = [
            ("100", "50", Some(50)),
            ("100", "0", Some(0)),
            ("3", "2", Some(66)),
            ("100", "120", Some(100)),
            ("0", "10", None),
            ("bad", "10", None),
            ("100", "bad", None),
            ("100", "-1", None),
            ("18446744073709551615", "18446744073709551615", Some(100)),
        ];
        let mut observed = Vec::new();
        for (limit, remaining, _) in cases {
            let headers = HeaderMap::from_iter([
                (
                    "x-ratelimit-limit-tokens".parse().expect("header name"),
                    HeaderValue::from_str(limit).expect("limit"),
                ),
                (
                    "x-ratelimit-remaining-tokens".parse().expect("header name"),
                    HeaderValue::from_str(remaining).expect("remaining"),
                ),
            ]);
            let quota = QuotaTelemetry::default();
            let request_generation = quota.begin_request();
            quota.update(request_generation, &headers, QuotaHeaderFamily::OpenAI);
            observed.push(quota.remaining_quota_percent());
        }
        assert_eq!(observed, cases.map(|(_, _, expected)| expected));
    }

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: expired quota does not authorize background work.
    #[test]
    fn expired_quota_does_not_authorize_background_work() {
        let quota = QuotaTelemetry::default();
        quota.state.lock().expect("quota lock").observation = Some(QuotaObservation {
            percent: 90,
            observed_at: Instant::now() - Duration::from_secs(61),
        });
        assert_eq!(quota.remaining_quota_percent(), None);
    }

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: invalid headers clear previously available quota.
    #[test]
    fn invalid_headers_clear_previously_available_quota() {
        let quota = QuotaTelemetry::default();
        let headers = HeaderMap::from_iter([
            (
                "anthropic-ratelimit-tokens-limit"
                    .parse()
                    .expect("header name"),
                HeaderValue::from_static("100"),
            ),
            (
                "anthropic-ratelimit-tokens-remaining"
                    .parse()
                    .expect("header name"),
                HeaderValue::from_static("90"),
            ),
        ]);
        let request_generation = quota.begin_request();
        quota.update(request_generation, &headers, QuotaHeaderFamily::Anthropic);
        let before = quota.remaining_quota_percent();
        let headers = HeaderMap::from_iter([
            (
                "anthropic-ratelimit-tokens-limit"
                    .parse()
                    .expect("header name"),
                HeaderValue::from_static("invalid"),
            ),
            (
                "anthropic-ratelimit-tokens-remaining"
                    .parse()
                    .expect("header name"),
                HeaderValue::from_static("90"),
            ),
        ]);
        let request_generation = quota.begin_request();
        quota.update(request_generation, &headers, QuotaHeaderFamily::Anthropic);
        assert_eq!((before, quota.remaining_quota_percent()), (Some(90), None));
    }

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: older response cannot replace latest request quota.
    #[test]
    fn older_response_cannot_replace_latest_request_quota() {
        let high = HeaderMap::from_iter([
            (
                "x-ratelimit-limit-tokens".parse().expect("header name"),
                HeaderValue::from_static("100"),
            ),
            (
                "x-ratelimit-remaining-tokens".parse().expect("header name"),
                HeaderValue::from_static("90"),
            ),
        ]);
        let low = HeaderMap::from_iter([
            (
                "x-ratelimit-limit-tokens".parse().expect("header name"),
                HeaderValue::from_static("100"),
            ),
            (
                "x-ratelimit-remaining-tokens".parse().expect("header name"),
                HeaderValue::from_static("10"),
            ),
        ]);
        let missing = HeaderMap::new();
        let mut observed = Vec::new();
        for latest_response in [Some(&low), Some(&missing), None] {
            let quota = QuotaTelemetry::default();
            let older_generation = quota.begin_request(); // Starts first, but responds last.
            let latest_generation = quota.begin_request(); // May have no headers or remain in flight.
            if let Some(headers) = latest_response {
                quota.update(latest_generation, headers, QuotaHeaderFamily::OpenAI);
            }
            let before = quota.remaining_quota_percent();
            quota.update(older_generation, &high, QuotaHeaderFamily::OpenAI);
            observed.push((before, quota.remaining_quota_percent()));
        }
        assert_eq!(observed, [(Some(10), Some(10)), (None, None), (None, None)]);
    }
}
