use std::collections::HashMap;

use anyhow::{anyhow, bail, Context};
use log::warn;
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use thiserror::Error;
use tokio::time::{sleep, Duration};
use url::Url;

use crate::config::Encoding;

pub mod middleware;

#[derive(Debug, Clone)]
pub struct HttpService {
    host_url: Url,
    client: reqwest::Client,
    max_retries: u32,
    base_backoff_ms: u64,
}

#[derive(Error, Debug)]
#[error("failed to send request: {source}")]
struct ResponseError {
    source: reqwest::Error,
    should_retry: bool,
}

impl HttpService {
    pub fn new(
        host: String,
        max_retries: Option<u32>,
        base_backoff_ms: Option<u64>,
        connection_timeout_sec: Option<u64>,
        timeout_sec: Option<u64>,
        tcp_keepalive_sec: Option<u64>,
    ) -> anyhow::Result<HttpService> {
        let tcp_keepalive = tcp_keepalive_sec
            .map(Duration::from_secs)
            .or_else(|| Some(Duration::from_secs(300)));

        let connection_timeout = connection_timeout_sec
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(10));

        let timeout = timeout_sec
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));

        let max_retries = max_retries.unwrap_or(5);
        let base_backoff_ms = base_backoff_ms.unwrap_or(1000);

        let client = reqwest::Client::builder()
            .tcp_keepalive(tcp_keepalive)
            .connect_timeout(connection_timeout)
            .timeout(timeout)
            .build()?;

        let host_url = Url::parse(&host)
            .with_context(|| anyhow!("failed to construct the url for host: {}", host))?;

        Ok(HttpService {
            host_url,
            client,
            max_retries,
            base_backoff_ms,
        })
    }

    pub async fn post(
        &mut self,
        path: &str,
        payload: Vec<u8>,
        encoding: Encoding,
        attrs: Option<HashMap<String, String>>,
        is_framed_batch: bool,
    ) -> anyhow::Result<String> {
        let full_url = self
            .host_url
            .join(path)
            .with_context(|| anyhow!("failed to construct the url for path: {}", path))?;

        let headers = self.headers_from_attrs(attrs, encoding, is_framed_batch)?;
        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.max_retries {
            attempt += 1;
            match self
                .execute_post_req(full_url.as_str(), payload.clone(), headers.clone())
                .await
            {
                Ok(response) => return Ok(response),
                Err(err) => {
                    let should_retry = err.should_retry;
                    warn!(
                        "http: failed to post. url: {}. attempt: {}. error: {}",
                        full_url.as_str(),
                        attempt,
                        err
                    );
                    last_error = Some(err);
                    if !should_retry {
                        break;
                    }

                    if attempt < self.max_retries {
                        let backoff = self.calculate_backoff(attempt);
                        sleep(Duration::from_millis(backoff)).await;
                        continue;
                    }
                }
            }
        }

        let err_msg = match last_error {
            Some(err) => format!("{}", err),
            None => "unknown error".to_owned(),
        };

        bail!(
            "failed to post event after {} attempts: {}",
            attempt,
            err_msg
        )
    }

    async fn execute_post_req(
        &self,
        url: &str,
        payload: Vec<u8>,
        headers: HeaderMap,
    ) -> Result<String, ResponseError> {
        let response = self
            .client
            .post(url)
            .headers(headers)
            .body(payload)
            .send()
            .await
            .map_err(|err| ResponseError {
                source: err,
                should_retry: true,
            })?;

        let response_status = response.status();
        match response.error_for_status() {
            Ok(response) => Ok(response.text().await.map_err(|err| ResponseError {
                source: err,
                should_retry: true,
            })?),
            Err(err) => {
                let should_retry = self.is_status_retriable(response_status);
                let err = ResponseError {
                    source: err,
                    should_retry,
                };
                Err(err)
            }
        }
    }

    fn is_status_retriable(&self, status: reqwest::StatusCode) -> bool {
        // Server errors (5xx) are generally retriable
        if status.is_server_error() {
            return true;
        }

        // Special cases of client errors that may be retriable
        match status.as_u16() {
            // Too Many Requests - should retry after respecting Retry-After header
            429 => true,

            // Specific 4xx codes that might be temporary conditions
            408 => true, // Request Timeout
            423 => true, // Locked (WebDAV) - resource is temporarily locked
            425 => true, // Too Early - server is unwilling to risk processing a request that might be replayed

            // All other client errors (4xx) are not retriable
            _ => false,
        }
    }

    fn calculate_backoff(&self, attempt: u32) -> u64 {
        let base = self.base_backoff_ms;

        // shift by max 16 bits to avoid overflow
        let exp_backoff = base * (1 << attempt.min(16));

        // +/- 20%
        let jitter_factor = rand::rng().random_range(0.8..1.2);
        let backoff = exp_backoff as f64 * jitter_factor;

        backoff as u64
    }

    fn headers_from_attrs(
        &self,
        attr: Option<HashMap<String, String>>,
        encoding: Encoding,
        is_framed_batch: bool,
    ) -> anyhow::Result<HeaderMap> {
        let mut hmap = HeaderMap::new();

        if is_framed_batch {
            hmap.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_str("application/x-mstream-framed")?,
            );
        } else {
            let _ = match encoding {
                Encoding::Avro => hmap.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_str("avro/binary")?,
                ),
                Encoding::Json => hmap.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_str("application/json")?,
                ),
                Encoding::Bson => hmap.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_str("application/bson")?,
                ),
            };
        }

        if let Some(attr) = attr {
            for (name, value) in attr {
                let custom_name = format!("X-{}", name);
                let header_name = custom_name.parse::<HeaderName>()?;
                let val = value.parse::<HeaderValue>()?;
                hmap.insert(header_name, val);
            }
        }

        Ok(hmap)
    }
}

#[cfg(test)]
mod tests {
    use super::HttpService;
    use crate::{config::Encoding, sink::encoding::SinkEvent};
    use mockito::Server;
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    // Helper function to create a SinkEvent for testing
    fn create_test_event(
        raw_bytes: Vec<u8>,
        attributes: Option<HashMap<String, String>>,
        encoding: Encoding,
    ) -> SinkEvent {
        SinkEvent {
            raw_bytes,
            attributes,
            encoding,
            is_framed_batch: false,
        }
    }

    #[test]
    fn test_constructor_default_parameters() {
        // Test that constructor uses default values correctly
        let service = HttpService::new(
            "https://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(service.host_url.as_str(), "https://example.com/");
        assert_eq!(service.max_retries, 5);
        assert_eq!(service.base_backoff_ms, 1000);
    }

    #[test]
    fn test_constructor_custom_parameters() {
        // Test constructor with custom parameters
        let service = HttpService::new(
            "https://example.com".to_string(),
            Some(10),
            Some(500),
            Some(20),
            Some(60),
            Some(120),
        )
        .unwrap();

        assert_eq!(service.host_url.as_str(), "https://example.com/");
        assert_eq!(service.max_retries, 10);
        assert_eq!(service.base_backoff_ms, 500);
    }

    #[tokio::test]
    async fn test_custom_headers_from_attributes() {
        // Setup mock to verify custom headers
        let mut server = Server::new_async().await;
        let resource = "/webhook";

        let mock_server = server
            .mock("POST", resource)
            .match_header("X-CustomHeader", "custom-value")
            .match_header("X-ApiKey", "secret-key")
            .with_status(200)
            .with_body("ok")
            .create();

        // Create HttpSink
        let mut sink = HttpService::new(server.url(), None, None, None, None, None).unwrap();

        // Create test event with custom headers
        let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
        let mut attributes = HashMap::new();
        attributes.insert("CustomHeader".to_string(), "custom-value".to_string());
        attributes.insert("ApiKey".to_string(), "secret-key".to_string());
        let event = create_test_event(payload, Some(attributes), Encoding::Json);

        // Call post
        let res = sink
            .post(
                &resource,
                event.raw_bytes,
                event.encoding,
                event.attributes,
                false,
            )
            .await;

        assert!(res.is_ok(), "Failed to post event: {:?}", res);

        // Verify headers were sent correctly
        mock_server.assert();
    }

    #[tokio::test]
    async fn test_retry_for_specific_status_codes() {
        let mut server = Server::new_async().await;
        let resource = "/webhook";

        // Test retry behavior for specific status codes
        for status in [429, 408, 423, 425, 500, 502, 503, 504] {
            // Setup mocks: first attempt fails with status code, second succeeds
            let _mock_fail = server.mock("POST", resource).with_status(status).create();

            let mock_success = server
                .mock("POST", resource)
                .with_status(200)
                .with_body("ok")
                .create();

            // Create HttpSink with fast retries for testing
            let mut sink =
                HttpService::new(server.url(), Some(3), Some(10), None, None, None).unwrap();

            // Create test event
            let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
            let event = create_test_event(payload, None, Encoding::Json);

            // Call publish method
            let result = sink
                .post(
                    resource,
                    event.raw_bytes,
                    event.encoding,
                    event.attributes,
                    false,
                )
                .await;

            // Verify success after retry
            assert!(result.is_ok());
            mock_success.assert();
        }
    }

    #[tokio::test]
    async fn test_no_retry_for_client_errors() {
        let mut server = Server::new_async().await;
        let resource = "/webhook";

        // Test no retry for most client errors (except specific ones like 429)
        for status in [400, 401, 403, 404, 405, 409, 422] {
            // Setup mock with client error - should only be called once
            let mock_error = server
                .mock("POST", resource)
                .with_status(status)
                .expect(1)
                .create();

            // Create HttpSink with multiple retries that shouldn't be used
            let mut sink =
                HttpService::new(server.url(), Some(3), Some(10), None, None, None).unwrap();

            // Create test event
            let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
            let event = create_test_event(payload, None, Encoding::Json);

            // Call publish method
            let result = sink
                .post(
                    resource,
                    event.raw_bytes,
                    event.encoding,
                    event.attributes,
                    false,
                )
                .await;

            // Verify failure without retry
            assert!(result.is_err());
            mock_error.assert();
        }
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        let sink = HttpService::new(
            "https://example.com".to_string(),
            None,
            Some(10),
            None,
            None,
            None,
        )
        .unwrap();

        // Test backoff values for different attempts
        // With base_backoff_ms = 10:
        // Attempt 1: ~10ms (with jitter)
        // Attempt 2: ~20ms (with jitter)
        // Attempt 3: ~40ms (with jitter)

        for attempt in 1..=5 {
            let backoff = sink.calculate_backoff(attempt);

            // Verify backoff is within expected range (allowing for jitter)
            let expected_base = 10 * (1 << attempt.min(16));
            assert!(backoff >= (expected_base as f64 * 0.8) as u64);
            assert!(backoff <= (expected_base as f64 * 1.2) as u64);
        }

        // Test with a very large attempt number (shouldn't overflow)
        let backoff = sink.calculate_backoff(100);
        assert!(backoff > 0);
    }

    #[tokio::test]
    async fn test_headers_with_invalid_values() {
        // Create HttpSink
        let mut sink = HttpService::new(
            "https://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Create attributes with invalid header value
        let mut attributes = HashMap::new();
        attributes.insert("Valid-Header".to_string(), "valid value".to_string());
        attributes.insert("Invalid-Header".to_string(), "invalid\nvalue".to_string());

        // Create test event
        let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
        let event = create_test_event(payload, Some(attributes), Encoding::Json);

        // Call publish
        let result = sink
            .post(
                "/webhook",
                event.raw_bytes,
                event.encoding,
                event.attributes,
                false,
            )
            .await;

        // Should fail due to invalid header value
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_gradually_increasing_retry_delays() {
        let mut server = Server::new_async().await;
        let resource = "/webhook";
        // Setup mock to fail multiple times
        let _mock = server
            .mock("POST", resource)
            .with_status(500)
            .expect(3)
            .create();

        // Create HttpSink with small base backoff for testing
        let mut sink = HttpService::new(server.url(), Some(3), Some(20), None, None, None).unwrap();

        // Create test event
        let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
        let event = create_test_event(payload, None, Encoding::Json);

        // Capture timing of each attempt
        let start = Instant::now();
        let _ = sink
            .post(
                resource,
                event.raw_bytes,
                event.encoding,
                event.attributes,
                false,
            )
            .await;
        let total_time = start.elapsed();

        // With exponential backoff, total should be at least ~20ms + ~40ms = ~60ms
        // Need to be generous with the minimum time due to test environment variability
        assert!(total_time >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_retry_behavior_for_network_errors() {
        // Create HttpSink with an invalid URL to trigger network errors
        let mut sink = HttpService::new(
            "https://non-existent-domain-12345.example".to_string(),
            Some(2),
            Some(10),
            Some(1), // 1-second connection timeout
            None,
            None,
        )
        .unwrap();

        // Create test event
        let payload = r#"{"test":"data"}"#.as_bytes().to_vec();
        let event = create_test_event(payload, None, Encoding::Json);

        // Time the operation to verify retries
        let start = Instant::now();
        let result = sink
            .post(
                "/webhook",
                event.raw_bytes,
                event.encoding,
                event.attributes,
                false,
            )
            .await;

        let elapsed = start.elapsed();

        // Request should fail after retries
        assert!(result.is_err());

        // With 2 retries and 10ms base backoff, should take at least 20ms
        // (being conservative due to test environment variability)
        assert!(elapsed >= Duration::from_millis(20));
    }
}
