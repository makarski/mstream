use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct CompletionItem {
    pub label: String,
    pub kind: CompletionKind,
    pub detail: String,
    pub documentation: String,
    pub insert_text: String,
    pub is_snippet: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CompletionKind {
    Function,
    Snippet,
}

pub fn completions() -> Vec<CompletionItem> {
    let mut items = Vec::new();
    items.extend(core_completions());
    items.extend(masking_completions());
    items.extend(aggregation_completions());
    items.extend(collection_completions());
    items.extend(utility_completions());
    items
}

fn core_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "transform".into(),
            kind: CompletionKind::Snippet,
            detail: "Transform function template".into(),
            documentation: "Scaffold the required transform(data, attributes) entry point.".into(),
            insert_text: "fn transform(data, attributes) {\n\t$0\n\tresult(data, attributes)\n}"
                .into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "result".into(),
            kind: CompletionKind::Function,
            detail: "(data, attributes?) → TransformResult".into(),
            documentation: "Return the transform result. Pass data and optionally attributes."
                .into(),
            insert_text: "result(${1:data}${2:, attributes})".into(),
            is_snippet: true,
        },
    ]
}

fn masking_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "mask_email".into(),
            kind: CompletionKind::Function,
            detail: "(email: string) → string".into(),
            documentation: "Masks an email address, preserving the first character and domain. \
                            \"john@example.com\" → \"j***@example.com\""
                .into(),
            insert_text: "mask_email(${1:data.email})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "mask_phone".into(),
            kind: CompletionKind::Function,
            detail: "(phone: string) → string".into(),
            documentation:
                "Masks a phone number, showing only last 4 digits. \"555-123-4567\" → \"****4567\""
                    .into(),
            insert_text: "mask_phone(${1:data.phone})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "mask_year_only".into(),
            kind: CompletionKind::Function,
            detail: "(date: string) → string".into(),
            documentation: "Masks date to January 1st of the same year, preserving the date format. \"1990-05-15\" → \"1990-01-01\""
                .into(),
            insert_text: "mask_year_only(${1:data.date})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "hash_sha256".into(),
            kind: CompletionKind::Function,
            detail: "(value: Dynamic) → string".into(),
            documentation: "Returns the SHA-256 hash of the input value as a hex string.".into(),
            insert_text: "hash_sha256(${1:value})".into(),
            is_snippet: true,
        },
    ]
}

fn aggregation_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "sum".into(),
            kind: CompletionKind::Function,
            detail: "(array) → number | (array, key_fn) → number".into(),
            documentation: "Sum numeric values in an array. Optionally pass a key function: \
                            sum(items, |x| x.price)"
                .into(),
            insert_text: "sum(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "avg".into(),
            kind: CompletionKind::Function,
            detail: "(array) → number | (array, key_fn) → number".into(),
            documentation: "Calculate average of numeric values. Returns () for empty arrays."
                .into(),
            insert_text: "avg(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "min".into(),
            kind: CompletionKind::Function,
            detail: "(array) → Dynamic | (array, key_fn) → Dynamic".into(),
            documentation: "Find the minimum value in an array.".into(),
            insert_text: "min(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "max".into(),
            kind: CompletionKind::Function,
            detail: "(array) → Dynamic | (array, key_fn) → Dynamic".into(),
            documentation: "Find the maximum value in an array.".into(),
            insert_text: "max(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "group_by".into(),
            kind: CompletionKind::Function,
            detail: "(array, key_fn) → map".into(),
            documentation: "Group array elements by key. Returns a map: \
                            #{ \"A\": [...], \"B\": [...] }"
                .into(),
            insert_text: "group_by(${1:array}, |${2:x}| ${3:x.key})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "count_by".into(),
            kind: CompletionKind::Function,
            detail: "(array, key_fn) → map".into(),
            documentation: "Count occurrences by key. Returns a map: \
                            #{ \"active\": 5, \"inactive\": 2 }"
                .into(),
            insert_text: "count_by(${1:array}, |${2:x}| ${3:x.key})".into(),
            is_snippet: true,
        },
    ]
}

fn collection_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "unique".into(),
            kind: CompletionKind::Function,
            detail: "(array) → array | (array, key_fn) → array".into(),
            documentation: "Remove duplicate values. Optionally pass a key function: \
                            unique(items, |x| x.id)"
                .into(),
            insert_text: "unique(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "flatten".into(),
            kind: CompletionKind::Function,
            detail: "(array) → array".into(),
            documentation: "Flatten one level of nested arrays. [[1, 2], [3, 4]] → [1, 2, 3, 4]"
                .into(),
            insert_text: "flatten(${1:array})".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "pluck".into(),
            kind: CompletionKind::Function,
            detail: "(array, field: string) → array".into(),
            documentation: "Extract a single field from each object in an array.".into(),
            insert_text: "pluck(${1:array}, \"${2:field}\")".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "pick".into(),
            kind: CompletionKind::Function,
            detail: "(object, fields: array) → object".into(),
            documentation: "Keep only the specified fields from an object.".into(),
            insert_text: "pick(${1:obj}, [${2:\"field1\", \"field2\"}])".into(),
            is_snippet: true,
        },
        CompletionItem {
            label: "omit".into(),
            kind: CompletionKind::Function,
            detail: "(object, fields: array) → object".into(),
            documentation: "Remove the specified fields from an object.".into(),
            insert_text: "omit(${1:obj}, [${2:\"field1\", \"field2\"}])".into(),
            is_snippet: true,
        },
    ]
}

fn utility_completions() -> Vec<CompletionItem> {
    vec![CompletionItem {
        label: "timestamp_ms".into(),
        kind: CompletionKind::Function,
        detail: "() → int".into(),
        documentation: "Returns the current time in milliseconds since the Unix epoch.".into(),
        insert_text: "timestamp_ms()".into(),
        is_snippet: false,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completions_returns_all_mstream_functions() {
        let items = completions();
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

        assert!(labels.contains(&"result"));
        assert!(labels.contains(&"mask_email"));
        assert!(labels.contains(&"mask_phone"));
        assert!(labels.contains(&"mask_year_only"));
        assert!(labels.contains(&"hash_sha256"));
        assert!(labels.contains(&"timestamp_ms"));
        assert!(labels.contains(&"sum"));
        assert!(labels.contains(&"avg"));
        assert!(labels.contains(&"min"));
        assert!(labels.contains(&"max"));
        assert!(labels.contains(&"group_by"));
        assert!(labels.contains(&"count_by"));
        assert!(labels.contains(&"unique"));
        assert!(labels.contains(&"flatten"));
        assert!(labels.contains(&"pluck"));
        assert!(labels.contains(&"pick"));
        assert!(labels.contains(&"omit"));
        assert!(labels.contains(&"transform"));
    }

    #[test]
    fn all_completions_have_required_fields() {
        for item in completions() {
            assert!(!item.label.is_empty(), "empty label");
            assert!(!item.detail.is_empty(), "empty detail for {}", item.label);
            assert!(
                !item.documentation.is_empty(),
                "empty docs for {}",
                item.label
            );
            assert!(
                !item.insert_text.is_empty(),
                "empty insert_text for {}",
                item.label
            );
        }
    }
}
