use serde_json::Value as JsonValue;

use crate::encoding::json_schema::{JsonSchema, JsonSchemaError, SchemaType};

pub(super) fn validate(data: &JsonValue, schema: &JsonSchema) -> Result<(), JsonSchemaError> {
    let validator = jsonschema::validator_for(schema).map_err(|err| {
        JsonSchemaError::ValidationError(format!("failed to create validator: {}", err))
    })?;

    let errs: Vec<String> = validator
        .iter_errors(data)
        .map(|err| err.to_string())
        .collect();

    if errs.is_empty() {
        return Ok(());
    }

    Err(JsonSchemaError::ValidationError(errs.join(", ")))
}

pub(super) fn project_json(
    data: &JsonValue,
    schema: &JsonSchema,
) -> Result<JsonValue, JsonSchemaError> {
    match SchemaType::from(schema) {
        SchemaType::Object => project_json_object(data, schema),
        SchemaType::Array => project_json_array(data, schema),
        _ => Ok(data.clone()),
    }
}

fn project_json_object(
    data: &JsonValue,
    schema: &JsonSchema,
) -> Result<JsonValue, JsonSchemaError> {
    let Some(obj) = data.as_object() else {
        return Err(JsonSchemaError::NotObject);
    };

    let mut projected = serde_json::Map::new();

    let Some(props) = schema.get("properties") else {
        return Err(JsonSchemaError::MissingPropertiesField);
    };

    let props = props
        .as_object()
        .ok_or_else(|| JsonSchemaError::PropertiesNotObject)?;

    for (key, field_schema) in props {
        if let Some(json_val) = obj.get(key) {
            projected.insert(key.clone(), project_json(json_val, field_schema)?);
        }
    }

    Ok(JsonValue::Object(projected))
}

fn project_json_array(data: &JsonValue, schema: &JsonSchema) -> Result<JsonValue, JsonSchemaError> {
    let Some(arr) = data.as_array() else {
        return Err(JsonSchemaError::NotArray);
    };

    let Some(items_schema) = schema.get("items") else {
        return Err(JsonSchemaError::MissingItemsField);
    };

    let projected: Result<Vec<JsonValue>, _> = arr
        .iter()
        .map(|item| project_json(item, items_schema))
        .collect();

    Ok(JsonValue::Array(projected?))
}
