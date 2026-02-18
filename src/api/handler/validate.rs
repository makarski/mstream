use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::middleware::udf::rhai::completions::completions;
use crate::middleware::udf::rhai::validate::validate_script;

#[derive(Debug, Deserialize)]
pub struct ValidateRequest {
    pub script: String,
}

/// POST /transform/validate
pub async fn transform_validate(Json(req): Json<ValidateRequest>) -> impl IntoResponse {
    let diagnostics = validate_script(&req.script);
    let valid = diagnostics.is_empty();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "valid": valid,
            "diagnostics": diagnostics,
        })),
    )
}

/// GET /transform/completions
pub async fn transform_completions() -> impl IntoResponse {
    (StatusCode::OK, Json(completions()))
}
