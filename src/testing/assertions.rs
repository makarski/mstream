//! Assertion checking for test case validation.
//!
//! This module provides functions to verify that actual output values
//! match expected values using various assertion types.

use serde_json::Value as JsonValue;
use std::collections::HashSet;

use crate::api::types::{AssertType, AssertionFailure, TestAssertion};

// =============================================================================
// Public API
// =============================================================================

/// Check all assertions against the actual output value.
pub fn check_assertions(actual: &JsonValue, assertions: &[TestAssertion]) -> Vec<AssertionFailure> {
    assertions
        .iter()
        .filter_map(|assertion| check_single_assertion(actual, assertion))
        .collect()
}

/// Check for extra fields in strict mode (fields not covered by any assertion).
pub fn check_extra_fields(
    actual: &JsonValue,
    assertions: &[TestAssertion],
) -> Vec<AssertionFailure> {
    let covered_paths: HashSet<&str> = assertions.iter().map(|a| a.path.as_str()).collect();
    let mut failures = Vec::new();
    collect_extra_fields(actual, "", &covered_paths, &mut failures);
    failures
}

// =============================================================================
// Single Assertion Check
// =============================================================================

fn check_single_assertion(
    actual: &JsonValue,
    assertion: &TestAssertion,
) -> Option<AssertionFailure> {
    let actual_value = get_value_at_path(actual, &assertion.path);

    let passed = match assertion.assert_type {
        AssertType::Equals => actual_value == Some(&assertion.expected_value),
        AssertType::Contains => check_contains(actual_value, &assertion.expected_value),
        AssertType::TypeMatches => check_type_matches(actual_value, &assertion.expected_value),
        AssertType::Gt => {
            check_numeric_comparison(actual_value, &assertion.expected_value, |a, b| a > b)
        }
        AssertType::Gte => {
            check_numeric_comparison(actual_value, &assertion.expected_value, |a, b| a >= b)
        }
        AssertType::Lt => {
            check_numeric_comparison(actual_value, &assertion.expected_value, |a, b| a < b)
        }
        AssertType::Lte => {
            check_numeric_comparison(actual_value, &assertion.expected_value, |a, b| a <= b)
        }
    };

    if passed {
        None
    } else {
        let actual_val = actual_value.cloned().unwrap_or(JsonValue::Null);
        Some(AssertionFailure {
            path: assertion.path.clone(),
            assert_type: assertion.assert_type.clone(),
            expected: assertion.expected_value.clone(),
            actual: actual_val.clone(),
            message: format_failure_message(
                &assertion.path,
                &assertion.assert_type,
                &assertion.expected_value,
                &actual_val,
            ),
        })
    }
}

// =============================================================================
// Path Navigation
// =============================================================================

/// Get a value at a dot-separated path, with support for array indexing.
pub fn get_value_at_path<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for segment in path.split('.') {
        current = get_path_segment(current, segment)?;
    }
    Some(current)
}

fn get_path_segment<'a>(value: &'a JsonValue, segment: &str) -> Option<&'a JsonValue> {
    // Check for array index pattern: [N]
    if segment.starts_with('[') && segment.ends_with(']') {
        let index_str = &segment[1..segment.len() - 1];
        let index: usize = index_str.parse().ok()?;
        return value.as_array()?.get(index);
    }
    // Regular field access
    value.get(segment)
}

// =============================================================================
// Assertion Type Implementations
// =============================================================================

fn check_contains(actual: Option<&JsonValue>, expected: &JsonValue) -> bool {
    match (actual, expected) {
        (Some(JsonValue::String(actual_str)), JsonValue::String(expected_str)) => {
            actual_str.contains(expected_str.as_str())
        }
        (Some(JsonValue::Array(actual_arr)), expected_item) => actual_arr.contains(expected_item),
        _ => false,
    }
}

fn check_type_matches(actual: Option<&JsonValue>, expected: &JsonValue) -> bool {
    match actual {
        Some(actual_val) => json_type_name(actual_val) == json_type_name(expected),
        None => expected.is_null(),
    }
}

fn check_numeric_comparison<F>(actual: Option<&JsonValue>, expected: &JsonValue, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (actual, expected) {
        (Some(JsonValue::Number(actual_num)), JsonValue::Number(expected_num)) => {
            match (actual_num.as_f64(), expected_num.as_f64()) {
                (Some(a), Some(b)) => cmp(a, b),
                _ => false,
            }
        }
        _ => false,
    }
}

// =============================================================================
// Extra Fields Detection (Strict Mode)
// =============================================================================

fn collect_extra_fields(
    value: &JsonValue,
    current_path: &str,
    covered_paths: &HashSet<&str>,
    failures: &mut Vec<AssertionFailure>,
) {
    if let JsonValue::Array(arr) = value {
        for (i, item) in arr.iter().enumerate() {
            let path = format!("{}[{}]", current_path, i);
            collect_extra_fields(item, &path, covered_paths, failures);
        }
        return;
    }

    let JsonValue::Object(map) = value else {
        return;
    };

    for (key, val) in map {
        let path = build_path(current_path, key);
        let should_recurse = val.is_object() || val.is_array();

        if is_path_covered(&path, covered_paths) {
            if val.is_object() {
                collect_extra_fields(val, &path, covered_paths, failures);
            }
            continue;
        }

        if should_recurse {
            collect_extra_fields(val, &path, covered_paths, failures);
        } else {
            failures.push(create_extra_field_failure(&path, val));
        }
    }
}

fn is_path_covered(path: &str, covered_paths: &HashSet<&str>) -> bool {
    covered_paths.contains(path)
        || covered_paths
            .iter()
            .any(|p| p.starts_with(&format!("{}.", path)))
}

fn create_extra_field_failure(path: &str, value: &JsonValue) -> AssertionFailure {
    AssertionFailure {
        path: path.to_string(),
        assert_type: AssertType::Equals,
        expected: JsonValue::Null,
        actual: value.clone(),
        message: format!("Unexpected field '{}' in output (strict mode)", path),
    }
}

fn build_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", prefix, key)
    }
}

// =============================================================================
// Formatting Helpers
// =============================================================================

fn format_failure_message(
    path: &str,
    assert_type: &AssertType,
    expected: &JsonValue,
    actual: &JsonValue,
) -> String {
    let expected_str = serde_json::to_string(expected).unwrap_or_else(|_| "?".to_string());
    let actual_str = serde_json::to_string(actual).unwrap_or_else(|_| "?".to_string());

    match assert_type {
        AssertType::Equals => {
            format!(
                "Expected {} to equal {}, got {}",
                path, expected_str, actual_str
            )
        }
        AssertType::Contains => {
            format!(
                "Expected {} to contain {}, got {}",
                path, expected_str, actual_str
            )
        }
        AssertType::TypeMatches => {
            let expected_type = json_type_name(expected);
            let actual_type = json_type_name(actual);
            format!(
                "Expected {} to be type {}, got {} ({})",
                path, expected_type, actual_type, actual_str
            )
        }
        AssertType::Gt => format!("Expected {} > {}, got {}", path, expected_str, actual_str),
        AssertType::Gte => format!("Expected {} >= {}, got {}", path, expected_str, actual_str),
        AssertType::Lt => format!("Expected {} < {}, got {}", path, expected_str, actual_str),
        AssertType::Lte => format!("Expected {} <= {}, got {}", path, expected_str, actual_str),
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // Test helpers
    // =========================================================================

    fn assertion(path: &str, assert_type: AssertType, expected_value: JsonValue) -> TestAssertion {
        TestAssertion {
            path: path.to_string(),
            assert_type,
            expected_value,
        }
    }

    fn assert_passes(actual: &JsonValue, assertions: Vec<TestAssertion>) {
        assert!(check_assertions(actual, &assertions).is_empty());
    }

    fn assert_fails(
        actual: &JsonValue,
        assertions: Vec<TestAssertion>,
        expected_count: usize,
    ) -> Vec<AssertionFailure> {
        let failures = check_assertions(actual, &assertions);
        assert_eq!(failures.len(), expected_count);
        failures
    }

    // =========================================================================
    // get_value_at_path tests
    // =========================================================================

    #[test]
    fn test_get_value_at_path_simple() {
        let value = json!({"name": "John", "age": 30});
        assert_eq!(get_value_at_path(&value, "name"), Some(&json!("John")));
        assert_eq!(get_value_at_path(&value, "age"), Some(&json!(30)));
        assert_eq!(get_value_at_path(&value, "missing"), None);
    }

    #[test]
    fn test_get_value_at_path_nested() {
        let value = json!({"user": {"profile": {"name": "John"}}});
        assert_eq!(
            get_value_at_path(&value, "user.profile.name"),
            Some(&json!("John"))
        );
        assert_eq!(get_value_at_path(&value, "user.profile.missing"), None);
    }

    #[test]
    fn test_get_value_at_path_array_index() {
        let value = json!({"items": ["a", "b", "c"]});
        assert_eq!(get_value_at_path(&value, "items.[0]"), Some(&json!("a")));
        assert_eq!(get_value_at_path(&value, "items.[2]"), Some(&json!("c")));
        assert_eq!(get_value_at_path(&value, "items.[5]"), None);
    }

    #[test]
    fn test_get_value_at_path_empty() {
        let value = json!({"name": "John"});
        assert_eq!(get_value_at_path(&value, ""), Some(&value));
    }

    // =========================================================================
    // check_assertions tests
    // =========================================================================

    #[test]
    fn test_check_assertions_equals_pass() {
        let actual = json!({"name": "John", "age": 30});
        assert_passes(
            &actual,
            vec![
                assertion("name", AssertType::Equals, json!("John")),
                assertion("age", AssertType::Equals, json!(30)),
            ],
        );
    }

    #[test]
    fn test_check_assertions_equals_fail() {
        let actual = json!({"name": "Jane"});
        let failures = assert_fails(
            &actual,
            vec![assertion("name", AssertType::Equals, json!("John"))],
            1,
        );
        assert_eq!(failures[0].path, "name");
        assert_eq!(failures[0].expected, json!("John"));
        assert_eq!(failures[0].actual, json!("Jane"));
    }

    #[test]
    fn test_check_assertions_contains_string() {
        let actual = json!({"email": "john@example.com"});
        assert_passes(
            &actual,
            vec![assertion("email", AssertType::Contains, json!("@example"))],
        );
        assert_fails(
            &actual,
            vec![assertion("email", AssertType::Contains, json!("@other"))],
            1,
        );
    }

    #[test]
    fn test_check_assertions_contains_array() {
        let actual = json!({"tags": ["a", "b", "c"]});
        assert_passes(
            &actual,
            vec![assertion("tags", AssertType::Contains, json!("b"))],
        );
        assert_fails(
            &actual,
            vec![assertion("tags", AssertType::Contains, json!("z"))],
            1,
        );
    }

    #[test]
    fn test_check_assertions_type_matches() {
        let actual = json!({"count": 42, "name": "test"});
        assert_passes(
            &actual,
            vec![
                assertion("count", AssertType::TypeMatches, json!(999)),
                assertion("name", AssertType::TypeMatches, json!("other")),
            ],
        );
        assert_fails(
            &actual,
            vec![assertion("count", AssertType::TypeMatches, json!("string"))],
            1,
        );
    }

    #[test]
    fn test_check_assertions_missing_path() {
        let actual = json!({"name": "John"});
        let failures = assert_fails(
            &actual,
            vec![assertion("missing", AssertType::Equals, json!("value"))],
            1,
        );
        assert_eq!(failures[0].actual, JsonValue::Null);
    }

    #[test]
    fn test_check_assertions_numeric_comparisons() {
        let actual = json!({"age": 25});

        assert_passes(&actual, vec![assertion("age", AssertType::Gt, json!(20))]);
        assert_fails(
            &actual,
            vec![assertion("age", AssertType::Gt, json!(25))],
            1,
        );
        assert_passes(&actual, vec![assertion("age", AssertType::Gte, json!(25))]);
        assert_passes(&actual, vec![assertion("age", AssertType::Lt, json!(30))]);
        assert_passes(&actual, vec![assertion("age", AssertType::Lte, json!(25))]);
    }

    #[test]
    fn test_check_assertions_numeric_on_non_number() {
        let actual = json!({"name": "John"});
        assert_fails(
            &actual,
            vec![assertion("name", AssertType::Gt, json!(10))],
            1,
        );
    }

    // =========================================================================
    // check_extra_fields tests
    // =========================================================================

    #[test]
    fn test_check_extra_fields_strict_mode() {
        let actual = json!({
            "name": "John",
            "age": 30,
            "secret": "password",
            "extra": "field"
        });

        let assertions = vec![
            assertion("name", AssertType::Equals, json!("John")),
            assertion("age", AssertType::Equals, json!(30)),
        ];

        let extra_failures = check_extra_fields(&actual, &assertions);
        assert_eq!(extra_failures.len(), 2);

        let messages: Vec<&str> = extra_failures.iter().map(|f| f.message.as_str()).collect();
        assert!(messages.iter().any(|m| m.contains("secret")));
        assert!(messages.iter().any(|m| m.contains("extra")));
    }

    #[test]
    fn test_check_extra_fields_nested() {
        let actual = json!({
            "user": {
                "name": "John",
                "hidden": "data"
            }
        });

        let assertions = vec![assertion("user.name", AssertType::Equals, json!("John"))];

        let extra_failures = check_extra_fields(&actual, &assertions);
        assert_eq!(extra_failures.len(), 1);
        assert!(extra_failures[0].message.contains("user.hidden"));
    }
}
