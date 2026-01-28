use std::collections::HashMap;

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{checkpoint::Checkpoint, config::Masked, kafka::KafkaOffset};

#[derive(Serialize)]
pub struct TransformTestResponse {
    pub document: serde_json::Value,
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct TransformTestRequest {
    pub payload: String,
    pub script: String,
    pub attributes: Option<HashMap<String, String>>,
}

#[derive(Serialize, Clone)]
pub struct Message<T> {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<T>,
}

impl<T: Masked> Masked for Message<T> {
    fn masked(&self) -> Self {
        let masked_item = match &self.item {
            Some(item) => Some(item.masked()),
            None => None,
        };

        Message {
            message: self.message.clone(),
            item: masked_item,
        }
    }
}

impl<T> IntoResponse for Message<T>
where
    T: Serialize + Masked,
{
    fn into_response(self) -> Response {
        MaskedJson(self).into_response()
    }
}

pub struct MaskedJson<T>(pub T);

impl<T> IntoResponse for MaskedJson<T>
where
    T: Serialize + Masked,
{
    fn into_response(self) -> Response {
        let masked_data = self.0.masked();
        let json_data = Json(masked_data);
        json_data.into_response()
    }
}

impl<T> Masked for Vec<T>
where
    T: Masked,
{
    fn masked(&self) -> Self {
        self.iter().map(|item| item.masked()).collect()
    }
}

/// Human-readable checkpoint response for API
#[derive(Clone, Debug, Serialize)]
pub struct CheckpointResponse {
    pub job_name: String,
    pub updated_at: String,
    pub cursor: CursorInfo,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum CursorInfo {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    #[serde(rename = "mongodb")]
    MongoDB {
        data: serde_json::Value,
    },
    Unknown {
        raw_bytes: usize,
    },
}

impl From<Checkpoint> for CheckpointResponse {
    fn from(cp: Checkpoint) -> Self {
        let cursor = decode_cursor(&cp.cursor);
        let updated_at = chrono::DateTime::from_timestamp_millis(cp.updated_at)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        CheckpointResponse {
            job_name: cp.job_name,
            updated_at,
            cursor,
        }
    }
}

pub(super) fn decode_cursor(cursor: &[u8]) -> CursorInfo {
    // Try Kafka offset first
    if let Ok(kafka) = mongodb::bson::from_slice::<KafkaOffset>(cursor) {
        return CursorInfo::Kafka {
            topic: kafka.topic,
            partition: kafka.partition,
            offset: kafka.offset,
        };
    }

    // Try MongoDB cursor (any BSON document - resume token, _id, or custom field)
    if let Ok(bson_doc) = mongodb::bson::from_slice::<mongodb::bson::Document>(cursor) {
        if let Ok(json_value) = serde_json::to_value(&bson_doc) {
            return CursorInfo::MongoDB { data: json_value };
        }
    }

    // Fallback to unknown
    CursorInfo::Unknown {
        raw_bytes: cursor.len(),
    }
}

#[derive(Deserialize)]
pub(crate) struct SchemaQuery {
    pub(crate) resource: String,
    #[serde(default = "default_sample_size")]
    pub(crate) sample_size: usize,
}

fn default_sample_size() -> usize {
    100
}
