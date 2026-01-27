use mongodb::bson::Document;
use serde_json::Value as JsonValue;

mod bson;
mod json;

/// Type alias for JSON Schema representation
pub type JsonSchema = serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum JsonSchemaError {
    #[error("JSON Schema validation error: {0}")]
    ValidationError(String),

    #[error("data is not an object")]
    NotObject,

    #[error("data is not an array")]
    NotArray,

    #[error("schema is missing 'properties' field")]
    MissingPropertiesField,

    #[error("schema is missing 'items' field")]
    MissingItemsField,

    #[error("schema 'properties' field is not an object")]
    PropertiesNotObject,

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("BSON projection error: {0}")]
    BsonError(String),
}

enum SchemaType {
    Object,
    Array,
    String,
    Number,
    Integer,
    Boolean,
    Null,

    // Internal unknown / unsupported schema type
    Unknown,
}

impl From<&JsonSchema> for SchemaType {
    fn from(schema: &JsonSchema) -> Self {
        match schema.get("type").and_then(|t| t.as_str()) {
            Some("object") => SchemaType::Object,
            Some("array") => SchemaType::Array,
            Some("string") => SchemaType::String,
            Some("number") => SchemaType::Number,
            Some("integer") => SchemaType::Integer,
            Some("boolean") => SchemaType::Boolean,
            Some("null") => SchemaType::Null,
            _ => SchemaType::Unknown,
        }
    }
}

/// Applies the given JSON Schema to the provided JSON bytes
pub fn apply_to_vec(json_bytes: &[u8], schema: &JsonSchema) -> Result<Vec<u8>, JsonSchemaError> {
    let parsed: JsonValue = serde_json::from_slice(json_bytes)?;
    apply_to_value(&parsed, schema)
}

/// Applies the given JSON Schema to the provided JSON value
pub fn apply_to_value(
    json_value: &JsonValue,
    schema: &JsonSchema,
) -> Result<Vec<u8>, JsonSchemaError> {
    json::validate(json_value, schema)?;
    let projected = json::project_json(json_value, schema)?;
    Ok(serde_json::to_vec(&projected)?)
}

pub fn apply_to_doc(doc: &Document, schema: &JsonSchema) -> Result<Document, JsonSchemaError> {
    bson::project_bson(doc, schema)
}
