use mongodb::bson::{Bson, Document};

use crate::encoding::json_schema::{JsonSchema, JsonSchemaError, SchemaType};

pub(super) fn project_bson(
    doc: &Document,
    schema: &JsonSchema,
) -> Result<Document, JsonSchemaError> {
    match SchemaType::from(schema) {
        SchemaType::Object => project_bson_object(doc, schema),
        _ => {
            return Err(JsonSchemaError::BsonError(String::from(
                "top level bson schema must be an Object",
            )));
        }
    }
}

fn project_bson_object(doc: &Document, schema: &JsonSchema) -> Result<Document, JsonSchemaError> {
    let Some(props) = schema.get("properties") else {
        return Err(JsonSchemaError::MissingPropertiesField);
    };

    let props = props
        .as_object()
        .ok_or_else(|| JsonSchemaError::PropertiesNotObject)?;

    let mut projected = Document::new();
    for (key, field_schema) in props {
        if let Some(bson_val) = doc.get(key) {
            projected.insert(key.clone(), project_bson_value(bson_val, field_schema)?);
        }
    }

    Ok(projected)
}

fn project_bson_value(val: &Bson, schema: &JsonSchema) -> Result<Bson, JsonSchemaError> {
    match (val, SchemaType::from(schema)) {
        (Bson::Document(doc), SchemaType::Object) => {
            Ok(Bson::Document(project_bson_object(doc, schema)?))
        }
        (Bson::Array(arr), SchemaType::Array) => {
            let projected = project_bson_array(arr, schema)?;
            Ok(Bson::Array(projected))
        }
        _ => Ok(val.clone()),
    }
}

fn project_bson_array(arr: &[Bson], schema: &JsonSchema) -> Result<Vec<Bson>, JsonSchemaError> {
    let Some(items_schema) = schema.get("items") else {
        return Err(JsonSchemaError::MissingItemsField);
    };

    let projected: Result<Vec<Bson>, _> = arr
        .iter()
        .map(|item| project_bson_value(item, items_schema))
        .collect();

    Ok(projected?)
}
