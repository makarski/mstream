//! Test case generation and execution API handlers.
//!
//! This module provides HTTP endpoints for:
//! - Generating test cases from sample input
//! - Running test cases against scripts

use std::time::Instant;

use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::api::error::ApiError;
use crate::api::types::{
    GeneratedTestCase, Message, TestGenerateRequest, TestGenerateResponse, TestRunRequest,
    TestRunResponse, TestRunSummary,
};
use crate::config::Encoding;
use crate::middleware::udf::rhai::RhaiMiddleware;
use crate::source::SourceEvent;
use crate::testing::{derive_assertions, derive_schema_from_value, run_test_case};

/// POST /transform/test/generate
///
/// Generates a test case by running the script on the sample input and
/// capturing the expected output with derived assertions.
pub async fn transform_test_generate(
    Json(req): Json<TestGenerateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut middleware = RhaiMiddleware::with_script(req.script.clone())?;

    let input_schema = derive_schema_from_value(&req.sample_input);
    let payload = serde_json::to_string(&req.sample_input).unwrap_or_default();

    let source_event = SourceEvent {
        cursor: None,
        attributes: None,
        encoding: Encoding::Json,
        is_framed_batch: false,
        raw_bytes: payload.as_bytes().to_vec(),
    };

    let start = Instant::now();
    let result = middleware.transform(source_event).await;
    let duration_ms = start.elapsed().as_millis() as u64;

    let response = match result {
        Ok(transformed) => {
            let expected = serde_json::from_slice(&transformed.raw_bytes)
                .map_err(|e| ApiError::BadRequest(format!("Invalid output JSON: {}", e)))?;

            let assertions = derive_assertions(&expected);

            TestGenerateResponse {
                input_schema,
                cases: vec![GeneratedTestCase {
                    name: "baseline".to_string(),
                    input: req.sample_input,
                    expected,
                    assertions,
                    duration_ms,
                }],
            }
        }
        Err(e) => {
            return Err(ApiError::BadRequest(format!(
                "Script execution failed: {}",
                e
            )));
        }
    };

    Ok((
        StatusCode::OK,
        Json(Message {
            message: "success".to_string(),
            item: Some(response),
        }),
    ))
}

/// POST /transform/test/run
///
/// Runs provided test cases against a script and returns pass/fail results
/// with detailed assertion failures.
pub async fn transform_test_run(
    Json(req): Json<TestRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut middleware = RhaiMiddleware::with_script(req.script)?;

    let mut results = Vec::with_capacity(req.cases.len());
    for case in &req.cases {
        let result = run_test_case(&mut middleware, case).await;
        results.push(result);
    }

    let passed = results.iter().filter(|r| r.passed).count();
    let failed = results.len() - passed;

    let response = TestRunResponse {
        results,
        summary: TestRunSummary {
            total: passed + failed,
            passed,
            failed,
        },
    };

    Ok((
        StatusCode::OK,
        Json(Message {
            message: "success".to_string(),
            item: Some(response),
        }),
    ))
}
