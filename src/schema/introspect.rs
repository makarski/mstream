use mongodb::{
    Database,
    bson::{Bson, Document, doc},
};
use serde_derive::Serialize;
use std::collections::{BTreeMap, HashMap};

use crate::encoding::json_schema::{self, JsonSchema};

#[derive(Debug, thiserror::Error)]
pub enum SchemaIntrospectError {
    #[error("MongoDB error: {0}")]
    MongodbError(#[from] mongodb::error::Error),

    #[error("No samples were found in the collection")]
    EmptySample,
}

#[derive(Debug, Serialize)]
pub struct SchemaVariant {
    pub share_percent: f64,
    pub sample_count: usize,
    pub schema: JsonSchema,
}

pub struct SchemaIntrospector {
    db: Database,
    coll_name: String,
}

impl SchemaIntrospector {
    pub fn new(db: Database, coll_name: String) -> Self {
        SchemaIntrospector { db, coll_name }
    }

    pub async fn introspect(
        &self,
        count: usize,
    ) -> Result<Vec<SchemaVariant>, SchemaIntrospectError> {
        let samples = self.make_sample(count).await?;
        if samples.is_empty() {
            return Err(SchemaIntrospectError::EmptySample);
        }

        let total = samples.len();

        // Collect field type stats across all documents
        let field_stats = collect_field_stats(&samples);

        // Find fields with type conflicts (>1 non-null type)
        let conflicts = find_conflicts(&field_stats);

        // Group documents by their conflict fingerprint
        let groups = group_by_conflicts(&samples, &conflicts);

        // Build schema per group
        let mut variants: Vec<SchemaVariant> = groups
            .into_iter()
            .map(|(_fingerprint, group_docs)| {
                let schema = json_schema::build_schema(&group_docs);
                SchemaVariant {
                    share_percent: (group_docs.len() as f64 / total as f64) * 100.0,
                    sample_count: group_docs.len(),
                    schema,
                }
            })
            .collect();

        // Sort by share_percent descending
        variants.sort_by(|a, b| b.share_percent.partial_cmp(&a.share_percent).unwrap());

        Ok(variants)
    }

    async fn make_sample(&self, count: usize) -> Result<Vec<Document>, SchemaIntrospectError> {
        let pipeline = vec![doc! {"$sample": {"size": count as i64}}];

        let coll = self.db.collection::<Document>(&self.coll_name);
        let mut cursor = coll.aggregate(pipeline).await?;

        let mut samples = Vec::with_capacity(count);

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            samples.push(doc);
        }

        Ok(samples)
    }
}

/// Collects field -> type -> count statistics
fn collect_field_stats(docs: &[Document]) -> HashMap<String, HashMap<String, usize>> {
    let mut stats: HashMap<String, HashMap<String, usize>> = HashMap::new();

    for doc in docs {
        collect_doc_types(doc, "", &mut stats);
    }

    stats
}

fn collect_doc_types(
    doc: &Document,
    prefix: &str,
    stats: &mut HashMap<String, HashMap<String, usize>>,
) {
    for (key, value) in doc {
        let field_path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        let type_name = bson_type_name(value);
        stats
            .entry(field_path.clone())
            .or_default()
            .entry(type_name)
            .and_modify(|c| *c += 1)
            .or_insert(1);

        // Recurse into nested documents
        if let Bson::Document(nested) = value {
            collect_doc_types(nested, &field_path, stats);
        }
    }
}

/// Identifies fields with >1 non-null type across all documents
fn find_conflicts(stats: &HashMap<String, HashMap<String, usize>>) -> Vec<String> {
    stats
        .iter()
        .filter_map(|(field_path, type_counts)| {
            let non_null_types: Vec<_> = type_counts.keys().filter(|t| *t != "null").collect();

            if non_null_types.len() > 1 {
                Some(field_path.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Groups documents by their type signature on conflict fields
fn group_by_conflicts(docs: &[Document], conflicts: &[String]) -> BTreeMap<String, Vec<Document>> {
    let mut groups: BTreeMap<String, Vec<Document>> = BTreeMap::new();

    for doc in docs {
        let fingerprint = compute_fingerprint(doc, conflicts);
        groups.entry(fingerprint).or_default().push(doc.clone());
    }

    groups
}

/// Computes a fingerprint string based on types of conflict fields
fn compute_fingerprint(doc: &Document, conflicts: &[String]) -> String {
    if conflicts.is_empty() {
        return String::from("default");
    }

    let mut sig: BTreeMap<&str, String> = BTreeMap::new();

    for conflict_field in conflicts {
        if let Some(value) = get_nested_field(doc, conflict_field) {
            sig.insert(conflict_field, bson_type_name(value));
        } else {
            sig.insert(conflict_field, "missing".to_string());
        }
    }

    format!("{:?}", sig)
}

/// Gets a nested field value by dot-notation path
fn get_nested_field<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;

    for (i, part) in parts.iter().enumerate() {
        match current.get(*part) {
            Some(Bson::Document(nested)) if i < parts.len() - 1 => {
                current = nested;
            }
            Some(value) if i == parts.len() - 1 => {
                return Some(value);
            }
            _ => return None,
        }
    }

    None
}

/// Maps BSON value to type name
fn bson_type_name(value: &Bson) -> String {
    match value {
        Bson::Double(_) => "number",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "boolean",
        Bson::Null => "null",
        Bson::Int32(_) | Bson::Int64(_) => "integer",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binary",
        Bson::Decimal128(_) => "decimal",
        _ => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn test_collect_field_stats_simple() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob", "age": 25 },
        ];

        let stats = collect_field_stats(&docs);

        assert_eq!(stats.get("name").unwrap().get("string").unwrap(), &2);
        assert_eq!(stats.get("age").unwrap().get("integer").unwrap(), &2);
    }

    #[test]
    fn test_collect_field_stats_mixed_types() {
        let docs = vec![
            doc! { "value": 42 },
            doc! { "value": "hello" },
            doc! { "value": 100 },
        ];

        let stats = collect_field_stats(&docs);

        let value_types = stats.get("value").unwrap();
        assert_eq!(value_types.get("integer").unwrap(), &2);
        assert_eq!(value_types.get("string").unwrap(), &1);
    }

    #[test]
    fn test_find_conflicts_no_conflicts() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob", "age": 25 },
        ];

        let stats = collect_field_stats(&docs);
        let conflicts = find_conflicts(&stats);

        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_find_conflicts_with_conflict() {
        let docs = vec![doc! { "value": 42 }, doc! { "value": "hello" }];

        let stats = collect_field_stats(&docs);
        let conflicts = find_conflicts(&stats);

        assert_eq!(conflicts.len(), 1);
        assert!(conflicts.contains(&"value".to_string()));
    }

    #[test]
    fn test_group_by_conflicts_no_conflicts() {
        let docs = vec![doc! { "name": "Alice" }, doc! { "name": "Bob" }];

        let conflicts: Vec<String> = vec![];
        let groups = group_by_conflicts(&docs, &conflicts);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups.get("default").unwrap().len(), 2);
    }

    #[test]
    fn test_group_by_conflicts_with_conflict() {
        let docs = vec![
            doc! { "value": 42 },
            doc! { "value": "hello" },
            doc! { "value": 100 },
        ];

        let conflicts = vec!["value".to_string()];
        let groups = group_by_conflicts(&docs, &conflicts);

        assert_eq!(groups.len(), 2);

        // Find the integer group and string group
        let mut int_count = 0;
        let mut str_count = 0;
        for (_fingerprint, group_docs) in &groups {
            if group_docs[0].get_i32("value").is_ok() {
                int_count = group_docs.len();
            } else {
                str_count = group_docs.len();
            }
        }

        assert_eq!(int_count, 2);
        assert_eq!(str_count, 1);
    }

    #[test]
    fn test_get_nested_field() {
        let doc = doc! {
            "user": {
                "profile": {
                    "name": "Alice"
                }
            }
        };

        let value = get_nested_field(&doc, "user.profile.name");
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_str().unwrap(), "Alice");

        let missing = get_nested_field(&doc, "user.profile.age");
        assert!(missing.is_none());
    }

    #[test]
    fn test_bson_type_name() {
        assert_eq!(bson_type_name(&Bson::String("test".to_string())), "string");
        assert_eq!(bson_type_name(&Bson::Int32(42)), "integer");
        assert_eq!(bson_type_name(&Bson::Int64(42)), "integer");
        assert_eq!(bson_type_name(&Bson::Double(3.14)), "number");
        assert_eq!(bson_type_name(&Bson::Boolean(true)), "boolean");
        assert_eq!(bson_type_name(&Bson::Null), "null");
        assert_eq!(bson_type_name(&Bson::Document(doc! {})), "object");
        assert_eq!(bson_type_name(&Bson::Array(vec![])), "array");
    }
}
