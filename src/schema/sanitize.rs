//! Avro name and namespace sanitization.
//!
//! Avro names must match `[A-Za-z_][A-Za-z0-9_]*`. This module provides
//! functions to clean arbitrary strings into valid Avro identifiers.

/// Sanitize a dotted namespace by sanitizing each segment independently.
pub fn sanitize_avro_namespace(ns: &str) -> String {
    ns.split('.')
        .map(|segment| sanitize_avro_name(segment))
        .collect::<Vec<_>>()
        .join(".")
}

/// Sanitize a string to be a valid Avro name.
/// Avro names must match: [A-Za-z_][A-Za-z0-9_]*
pub fn sanitize_avro_name(s: &str) -> String {
    if s.is_empty() {
        return "Field".to_string();
    }

    let mut chars = s.chars();
    let first = chars.next().unwrap();

    let mut result = String::with_capacity(s.len());
    let mut last_was_underscore = sanitize_first_char(first, &mut result);

    for c in chars {
        last_was_underscore = sanitize_subsequent_char(c, &mut result, last_was_underscore);
    }

    finalize_avro_name(result)
}

fn sanitize_first_char(c: char, result: &mut String) -> bool {
    if c.is_ascii_alphabetic() || c == '_' {
        result.push(c);
        c == '_'
    } else if c.is_ascii_digit() {
        result.push('_');
        result.push(c);
        false
    } else {
        result.push('_');
        true
    }
}

fn sanitize_subsequent_char(c: char, result: &mut String, last_was_underscore: bool) -> bool {
    if c.is_ascii_alphanumeric() {
        result.push(c);
        false
    } else if !last_was_underscore {
        result.push('_');
        true
    } else {
        last_was_underscore
    }
}

fn finalize_avro_name(mut result: String) -> String {
    while result.ends_with('_') && result.len() > 1 {
        result.pop();
    }

    if result.is_empty() || result == "_" {
        "Field".to_string()
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_handles_invalid_chars() {
        assert_eq!(sanitize_avro_name("valid_name"), "valid_name");
        assert_eq!(sanitize_avro_name("with-dash"), "with_dash");
        assert_eq!(sanitize_avro_name("with.dot"), "with_dot");
        assert_eq!(sanitize_avro_name("with space"), "with_space");
        assert_eq!(
            sanitize_avro_name("123starts_with_digit"),
            "_123starts_with_digit"
        );
        assert_eq!(sanitize_avro_name(""), "Field");
        assert_eq!(sanitize_avro_name("---"), "Field");
        assert_eq!(sanitize_avro_name("_already_valid"), "_already_valid");
        assert_eq!(sanitize_avro_name("__double"), "_double");
        assert_eq!(sanitize_avro_name("Valid"), "Valid");
        assert_eq!(sanitize_avro_name("trailing_"), "trailing");
    }

    #[test]
    fn namespace_handles_hyphens() {
        assert_eq!(
            sanitize_avro_namespace("com.mstream.valid"),
            "com.mstream.valid"
        );
        assert_eq!(
            sanitize_avro_namespace("com.mstream.etl-test-sinks"),
            "com.mstream.etl_test_sinks"
        );
        assert_eq!(
            sanitize_avro_namespace("my-org.my-project"),
            "my_org.my_project"
        );
        assert_eq!(sanitize_avro_namespace(""), "Field");
    }
}
