//! Test case execution for transform scripts.
//!
//! This module provides the core logic for running test cases against
//! transform scripts and collecting results.

use std::time::Instant;

use serde_json::Value as JsonValue;

use crate::api::types::{TestCase, TestCaseResult};
use crate::config::Encoding;
use crate::middleware::udf::rhai::RhaiMiddleware;
use crate::source::SourceEvent;
use crate::testing::assertions::{check_assertions, check_extra_fields};

// =============================================================================
// Public API
// =============================================================================

/// Run a single test case against the middleware and return the result.
pub async fn run_test_case(middleware: &mut RhaiMiddleware, case: &TestCase) -> TestCaseResult {
    let source_event = create_source_event(case);
    let start = Instant::now();

    match middleware.transform(source_event).await {
        Ok(transformed) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            process_transform_success(&transformed.raw_bytes, case, duration_ms)
        }
        Err(e) => create_error_result(&case.name, e.to_string(), start),
    }
}

/// Run multiple test cases and collect all results.
pub async fn run_test_cases(
    middleware: &mut RhaiMiddleware,
    cases: &[TestCase],
) -> Vec<TestCaseResult> {
    let mut results = Vec::with_capacity(cases.len());
    for case in cases {
        results.push(run_test_case(middleware, case).await);
    }
    results
}

// =============================================================================
// Internal Helpers
// =============================================================================

fn create_source_event(case: &TestCase) -> SourceEvent {
    let payload = serde_json::to_string(&case.input).unwrap_or_default();
    SourceEvent {
        cursor: None,
        attributes: None,
        encoding: Encoding::Json,
        is_framed_batch: false,
        raw_bytes: payload.as_bytes().to_vec(),
        source_timestamp: None,
    }
}

fn process_transform_success(
    raw_bytes: &[u8],
    case: &TestCase,
    duration_ms: u64,
) -> TestCaseResult {
    match serde_json::from_slice::<JsonValue>(raw_bytes) {
        Ok(actual) => evaluate_assertions(&actual, case, duration_ms),
        Err(e) => TestCaseResult {
            name: case.name.clone(),
            passed: false,
            actual: None,
            error: Some(format!("Failed to parse output: {}", e)),
            failures: vec![],
            duration_ms,
        },
    }
}

fn evaluate_assertions(actual: &JsonValue, case: &TestCase, duration_ms: u64) -> TestCaseResult {
    let mut failures = check_assertions(actual, &case.assertions);

    // In strict mode, check for extra fields not covered by assertions
    if case.strict_mode {
        let extra_failures = check_extra_fields(actual, &case.assertions);
        failures.extend(extra_failures);
    }

    let passed = failures.is_empty();

    TestCaseResult {
        name: case.name.clone(),
        passed,
        actual: Some(actual.clone()),
        error: None,
        failures,
        duration_ms,
    }
}

fn create_error_result(name: &str, error: String, start: Instant) -> TestCaseResult {
    TestCaseResult {
        name: name.to_string(),
        passed: false,
        actual: None,
        error: Some(error),
        failures: vec![],
        duration_ms: start.elapsed().as_millis() as u64,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{AssertType, TestAssertion};
    use serde_json::json;

    fn create_test_case(name: &str, input: JsonValue, assertions: Vec<TestAssertion>) -> TestCase {
        TestCase {
            name: name.to_string(),
            input,
            expected: None,
            assertions,
            strict_mode: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_run_test_case_pass() {
        let script = r#"
            fn transform(data, attributes) {
                result(data, attributes)
            }
        "#;
        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();

        let case = create_test_case(
            "passthrough",
            json!({"name": "John"}),
            vec![TestAssertion {
                path: "name".to_string(),
                assert_type: AssertType::Equals,
                expected_value: json!("John"),
            }],
        );

        let result = run_test_case(&mut middleware, &case).await;
        assert!(result.passed);
        assert!(result.error.is_none());
        assert!(result.failures.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_run_test_case_fail() {
        let script = r#"
            fn transform(data, attributes) {
                data.name = "Jane";
                result(data, attributes)
            }
        "#;
        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();

        let case = create_test_case(
            "modified",
            json!({"name": "John"}),
            vec![TestAssertion {
                path: "name".to_string(),
                assert_type: AssertType::Equals,
                expected_value: json!("John"),
            }],
        );

        let result = run_test_case(&mut middleware, &case).await;
        assert!(!result.passed);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0].path, "name");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_run_test_case_script_error() {
        let script = r#"
            fn transform(data, attributes) {
                throw "intentional error";
            }
        "#;
        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();

        let case = create_test_case("error", json!({"name": "John"}), vec![]);

        let result = run_test_case(&mut middleware, &case).await;
        assert!(!result.passed);
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("intentional error"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_run_test_case_strict_mode() {
        let script = r#"
            fn transform(data, attributes) {
                data.extra = "field";
                result(data, attributes)
            }
        "#;
        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();

        let mut case = create_test_case(
            "strict",
            json!({"name": "John"}),
            vec![TestAssertion {
                path: "name".to_string(),
                assert_type: AssertType::Equals,
                expected_value: json!("John"),
            }],
        );
        case.strict_mode = true;

        let result = run_test_case(&mut middleware, &case).await;
        assert!(!result.passed);
        // Should fail due to extra "extra" field
        assert!(result.failures.iter().any(|f| f.path == "extra"));
    }
}
