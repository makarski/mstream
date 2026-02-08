use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::api::error::ApiError;
use crate::api::types::{Message, TransformTestRequest, TransformTestResponse};
use crate::config::Encoding;
use crate::middleware::udf::rhai::{RhaiMiddleware, RhaiMiddlewareError};
use crate::source::SourceEvent;

/// POST /transform/run
pub async fn transform_run(
    Json(transform_req): Json<TransformTestRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut middleware = RhaiMiddleware::with_script(transform_req.script)?;

    let source_event = SourceEvent {
        cursor: None,
        attributes: transform_req.attributes,
        encoding: Encoding::Json,
        is_framed_batch: false,
        raw_bytes: transform_req.payload.as_bytes().to_vec(),
    };

    let transformed = middleware.transform(source_event).await?;
    let json_value: serde_json::Value =
        serde_json::from_slice(&transformed.raw_bytes).map_err(|e| {
            RhaiMiddlewareError::ExecutionError {
                message: format!("Output is not valid JSON: {}", e),
                path: "<inline>".to_string(),
            }
        })?;

    let response = TransformTestResponse {
        document: json_value,
        attributes: transformed.attributes,
    };

    Ok((
        StatusCode::OK,
        Json(Message {
            message: "success".to_string(),
            item: Some(response),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::config::Encoding;
    use crate::middleware::udf::rhai::{RhaiMiddleware, RhaiMiddlewareError};
    use crate::source::SourceEvent;

    #[tokio::test(flavor = "multi_thread")]
    async fn transform_single_document() {
        let script = r#"
            fn transform(data, attributes) {
                data.processed = true;
                result(data, attributes)
            }
        "#;
        let payload = r#"{"name": "test"}"#;

        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
        let source_event = SourceEvent {
            cursor: None,
            attributes: None,
            encoding: Encoding::Json,
            is_framed_batch: false,
            raw_bytes: payload.as_bytes().to_vec(),
        };

        let transformed = middleware.transform(source_event).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

        assert_eq!(json["name"], "test");
        assert_eq!(json["processed"], true);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transform_with_attributes() {
        let script = r#"
            fn transform(data, attributes) {
                attributes["modified"] = "yes";
                result(data, attributes)
            }
        "#;
        let payload = r#"{"value": 42}"#;

        let mut attrs = HashMap::new();
        attrs.insert("source".to_string(), "test".to_string());

        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
        let source_event = SourceEvent {
            cursor: None,
            attributes: Some(attrs),
            encoding: Encoding::Json,
            is_framed_batch: false,
            raw_bytes: payload.as_bytes().to_vec(),
        };

        let transformed = middleware.transform(source_event).await.unwrap();

        let result_attrs = transformed.attributes.unwrap();
        assert_eq!(result_attrs.get("source"), Some(&"test".to_string()));
        assert_eq!(result_attrs.get("modified"), Some(&"yes".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transform_array_payload() {
        // Note: is_framed_batch=false means we treat the JSON array as a single document
        // The script receives the array and processes it directly
        let script = r#"
            fn transform(data, attributes) {
                let results = [];
                for doc in data {
                    doc.batch = true;
                    results.push(doc);
                }
                result(results, attributes)
            }
        "#;
        let payload = r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#;

        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
        let source_event = SourceEvent {
            cursor: None,
            attributes: None,
            encoding: Encoding::Json,
            is_framed_batch: false, // JSON array is parsed as a single value
            raw_bytes: payload.as_bytes().to_vec(),
        };

        let transformed = middleware.transform(source_event).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["batch"], true);
        assert_eq!(arr[1]["batch"], true);
        assert_eq!(arr[2]["batch"], true);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transform_with_masking_functions() {
        let script = r#"
            fn transform(data, attributes) {
                data.email = mask_email(data.email);
                data.phone = mask_phone(data.phone);
                result(data, attributes)
            }
        "#;
        let payload = r#"{"email": "john@example.com", "phone": "+1-555-123-4567"}"#;

        let mut middleware = RhaiMiddleware::with_script(script.to_string()).unwrap();
        let source_event = SourceEvent {
            cursor: None,
            attributes: None,
            encoding: Encoding::Json,
            is_framed_batch: false,
            raw_bytes: payload.as_bytes().to_vec(),
        };

        let transformed = middleware.transform(source_event).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&transformed.raw_bytes).unwrap();

        // mask_email: john@example.com -> j***@example.com
        assert_eq!(json["email"], "j***@example.com");
        // mask_phone: +1-555-123-4567 -> ***********4567
        assert_eq!(json["phone"], "***********4567");
    }

    #[test]
    fn invalid_script_returns_compile_error() {
        let script = r#"
            fn transform(data, attributes {
                result(data, attributes)
            }
        "#;

        let result = RhaiMiddleware::with_script(script.to_string());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RhaiMiddlewareError::CompileError { .. }));
    }

    #[test]
    fn missing_transform_function_returns_error() {
        let script = r#"
            fn process(data, attributes) {
                result(data, attributes)
            }
        "#;

        let result = RhaiMiddleware::with_script(script.to_string());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RhaiMiddlewareError::MissingTransformFunction));
    }
}
