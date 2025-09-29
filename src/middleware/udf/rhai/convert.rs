//! # Type Conversion Utilities for Rhai Middleware
//!
//! This module provides conversion utilities between Rust types and Rhai types,
//! with special handling for binary data through base64 encoding/decoding.
//!
//! ## Core Components
//!
//! - [`JsonConverter`]: Bidirectional conversion between `serde_json::Value` and Rhai `Dynamic`
//! - [`RhaiString`]: Handles string/binary data conversion with automatic base64 encoding
//! - [`RhaiMap`]: Converts between Rust `HashMap` and Rhai `Map`
//!
//! ## Binary Data Handling
//!
//! Binary data (non-UTF8 bytes) is automatically encoded as base64 with a `data:base64,`
//! prefix when converting to strings, and decoded back when converting from strings.
//!
//! ## Example
//!
//! ```rust,ignore
//! use mstream::middleware::udf::rhai::convert::{RhaiString, JsonConverter};
//!
//! // Convert binary data to Rhai string
//! let binary = vec![0xFF, 0xFE, 0x00];
//! let rhai_str = RhaiString::from(binary.as_slice());
//! assert!(rhai_str.0.starts_with("data:base64,"));
//!
//! // Convert back to bytes
//! let bytes: Vec<u8> = rhai_str.try_into()?;
//! assert_eq!(bytes, binary);
//! ```

use base64::engine::{general_purpose, Engine as _};
use rhai::{Array, Dynamic, Map};
use serde_json::Value;
use std::{collections::HashMap, convert::TryFrom};

/// Errors that can occur during type conversions
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// Invalid number format in JSON
    #[error("Invalid number format: cannot convert to i64 or f64")]
    InvalidNumber,

    /// Float value cannot be represented in JSON
    #[error("Invalid float value: {0} cannot be represented in JSON")]
    InvalidFloat(f64),

    /// Type cannot be converted to JSON
    #[error("Unsupported type for JSON conversion: {type_name}")]
    UnsupportedType { type_name: String },

    /// Base64 decoding failed
    #[error("Failed to decode base64: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),

    /// Invalid data URI format
    #[error("Invalid data URI format: expected 'data:base64,' prefix")]
    InvalidDataUri,
}

/// Converter between `serde_json::Value` and Rhai `Dynamic`
///
/// This struct provides bidirectional conversion between JSON values and
/// Rhai's dynamic type system, preserving structure and types where possible.
///
/// # Example
///
/// ```rust,ignore
/// use serde_json::json;
/// use rhai::Dynamic;
/// use mstream::middleware::udf::rhai::convert::JsonConverter;
///
/// let json_value = json!({"name": "Alice", "age": 30});
/// let converter = JsonConverter::new(json_value);
/// let dynamic: Dynamic = converter.try_into()?;
/// ```
pub struct JsonConverter(serde_json::Value);

impl TryFrom<JsonConverter> for Dynamic {
    type Error = ConvertError;

    fn try_from(converter: JsonConverter) -> Result<Self, Self::Error> {
        let value = converter.0;

        Ok(match value {
            Value::Null => Dynamic::UNIT,
            Value::Bool(b) => Dynamic::from(b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Dynamic::from(i)
                } else if let Some(f) = n.as_f64() {
                    Dynamic::from(f)
                } else {
                    return Err(ConvertError::InvalidNumber);
                }
            }
            Value::String(s) => Dynamic::from(s),
            Value::Array(arr) => {
                let mut rhai_arr = Array::new();
                for item in arr {
                    rhai_arr.push(Dynamic::try_from(JsonConverter(item))?);
                }
                Dynamic::from(rhai_arr)
            }
            Value::Object(obj) => {
                let mut rhai_map = Map::new();
                for (key, value) in obj {
                    rhai_map.insert(key.into(), Dynamic::try_from(JsonConverter(value))?);
                }
                Dynamic::from(rhai_map)
            }
        })
    }
}

impl TryFrom<Dynamic> for JsonConverter {
    type Error = ConvertError;

    fn try_from(dynamic: Dynamic) -> Result<Self, Self::Error> {
        let value = if dynamic.is_unit() {
            Value::Null
        } else if let Some(b) = dynamic.as_bool().ok() {
            Value::Bool(b)
        } else if let Some(i) = dynamic.as_int().ok() {
            Value::Number(serde_json::Number::from(i))
        } else if let Some(f) = dynamic.as_float().ok() {
            match serde_json::Number::from_f64(f) {
                Some(n) => Value::Number(n),
                None => return Err(ConvertError::InvalidFloat(f)),
            }
        } else if let Some(s) = dynamic.clone().into_immutable_string().ok() {
            Value::String(s.to_string())
        } else if let Some(arr) = dynamic.clone().try_cast::<Array>() {
            let mut json_arr = Vec::new();
            for item in arr {
                let converter = JsonConverter::try_from(item)?;
                json_arr.push(converter.0);
            }
            Value::Array(json_arr)
        } else if let Some(map) = dynamic.clone().try_cast::<Map>() {
            let mut json_obj = serde_json::Map::new();
            for (key, value) in map {
                let key_str = key.as_str();
                let converter = JsonConverter::try_from(value)?;
                json_obj.insert(key_str.to_string(), converter.0);
            }
            Value::Object(json_obj)
        } else {
            return Err(ConvertError::UnsupportedType {
                type_name: dynamic.type_name().to_string(),
            });
        };

        Ok(JsonConverter(value))
    }
}

impl JsonConverter {
    /// Creates a new JsonConverter from a serde_json Value
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    /// Consumes the converter and returns the inner JSON value
    pub fn into_value(self) -> serde_json::Value {
        self.0
    }
}

/// String wrapper that handles binary data through base64 encoding
///
/// When converting from bytes:
/// - UTF-8 strings are preserved as-is
/// - Binary data is encoded as base64 with `data:base64,` prefix
///
/// When converting to bytes:
/// - Strings with `data:base64,` prefix are decoded
/// - Strings that look like base64 are decoded
/// - Regular strings are converted to UTF-8 bytes
///
/// # Example
///
/// ```rust,ignore
/// use mstream::middleware::udf::rhai::convert::RhaiString;
///
/// // UTF-8 string
/// let text = "Hello, World!";
/// let rhai_str = RhaiString::from(text.as_bytes());
/// assert_eq!(rhai_str.0, "Hello, World!");
///
/// // Binary data
/// let binary = vec![0xFF, 0xFE];
/// let rhai_str = RhaiString::from(binary.as_slice());
/// assert!(rhai_str.0.starts_with("data:base64,"));
/// ```
pub struct RhaiString(pub String);

impl From<&[u8]> for RhaiString {
    fn from(bytes: &[u8]) -> Self {
        let str = match std::str::from_utf8(bytes) {
            Ok(s) => s.to_string(),
            Err(_) => format!("data:base64,{}", general_purpose::STANDARD.encode(bytes)),
        };

        RhaiString(str)
    }
}

impl TryInto<Vec<u8>> for RhaiString {
    type Error = ConvertError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let s = &self.0;
        if s.starts_with("data:base64,") {
            // Strip the data:base64, prefix and decode
            let base64_part = &s[12..]; // "data:base64," is 12 characters
            Ok(general_purpose::STANDARD.decode(base64_part)?)
        } else if is_likely_base64(s) {
            Ok(general_purpose::STANDARD.decode(s)?)
        } else {
            Ok(s.as_bytes().to_vec())
        }
    }
}

/// Wrapper for converting between Rust HashMap and Rhai Map
///
/// Provides conversion from optional HashMap references to Rhai maps,
/// with empty map as default for None values.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use mstream::middleware::udf::rhai::convert::RhaiMap;
///
/// let mut map = HashMap::new();
/// map.insert("key".to_string(), "value".to_string());
///
/// let rhai_map = RhaiMap::from(Some(&map));
/// assert_eq!(rhai_map.0.len(), 1);
///
/// let empty_map = RhaiMap::from(None);
/// assert!(empty_map.0.is_empty());
/// ```
pub struct RhaiMap(pub rhai::Map);

impl From<Option<&HashMap<String, String>>> for RhaiMap {
    fn from(opt_map: Option<&HashMap<String, String>>) -> Self {
        match opt_map {
            Some(map) => {
                let mut rhai_map = rhai::Map::new();
                for (k, v) in map {
                    rhai_map.insert(k.into(), v.into());
                }
                RhaiMap(rhai_map)
            }
            None => RhaiMap(rhai::Map::new()),
        }
    }
}

/// Heuristic function to detect if a string is likely base64 encoded
///
/// Uses multiple heuristics to determine if a string is base64:
/// - Character set validation
/// - Padding rules
/// - Length patterns
/// - Common base64 characteristics
///
/// # Arguments
///
/// * `s` - The string to check
///
/// # Returns
///
/// `true` if the string is likely base64 encoded, `false` otherwise
fn is_likely_base64(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Check for valid base64 characters
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
    {
        return false;
    }

    // Check padding rules
    let padding_count = s.chars().filter(|&c| c == '=').count();
    if padding_count > 2 {
        return false;
    }

    // If there's padding, it must be at the end
    if padding_count > 0 {
        let first_padding = s.find('=').unwrap();
        if !s[first_padding..].chars().all(|c| c == '=') {
            return false;
        }
        // Padded base64 must be divisible by 4
        return s.len() % 4 == 0;
    }

    // For unpadded base64, apply stricter heuristics
    let remainder = s.len() % 4;

    // Remainder of 1 is never valid
    if remainder == 1 {
        return false;
    }

    // Short strings with remainder 2 are unlikely to be base64
    // (like "SGVsbG" which is only 6 chars)
    if remainder == 2 && s.len() <= 6 {
        return false;
    }

    // Additional heuristic: very short strings are unlikely to be base64
    // unless they're exactly 4 chars (a complete base64 block)
    if s.len() < 8 && s.len() != 4 {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_rhai_string_valid_utf8() {
        let json_bytes = r#"{"name": "John", "age": 30}"#.as_bytes();
        let result = RhaiString::from(json_bytes).0;

        assert_eq!(result, r#"{"name": "John", "age": 30}"#);
    }

    #[test]
    fn test_bytes_to_rhai_string_invalid_utf8() {
        let invalid_bytes = &[0xFF, 0xFE, 0xFD];
        let result = RhaiString::from(invalid_bytes.as_slice()).0;

        assert!(result.starts_with("data:base64,"));

        let expected_base64 = general_purpose::STANDARD.encode(invalid_bytes);
        assert_eq!(result, format!("data:base64,{}", expected_base64));
    }

    #[test]
    fn test_bytes_to_rhai_string_empty() {
        let empty_bytes = &[];
        let result = RhaiString::from(empty_bytes.as_slice()).0;

        assert_eq!(result, "");
    }

    #[test]
    fn test_rhai_string_to_bytes_regular_string() {
        let input = "Hello, World!";
        let result: Result<Vec<u8>, ConvertError> = RhaiString(input.to_string()).try_into();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), input.as_bytes());
    }

    #[test]
    fn test_rhai_string_to_bytes_json_string() {
        let json = r#"{"name": "John", "age": 30}"#;
        let result: Result<Vec<u8>, ConvertError> = RhaiString(json.to_string()).try_into();

        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes, json.as_bytes());

        let back_to_string = String::from_utf8(bytes).unwrap();
        assert_eq!(back_to_string, json);
    }

    #[test]
    fn test_rhai_string_to_bytes_data_prefix() {
        let original_data = "Hello, World!";
        let base64_encoded = general_purpose::STANDARD.encode(original_data);
        let input = format!("data:base64,{}", base64_encoded);

        let result: Result<Vec<u8>, ConvertError> = RhaiString(input).try_into();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_data.as_bytes());
    }

    #[test]
    fn test_rhai_string_to_bytes_likely_base64() {
        let original_data = "Hello, World! This is a test message.";
        let base64_string = general_purpose::STANDARD.encode(original_data);

        let result: Result<Vec<u8>, ConvertError> = RhaiString(base64_string).try_into();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_data.as_bytes());
    }

    #[test]
    fn test_rhai_string_to_bytes_invalid_base64() {
        let invalid_base64 = "This-is-not-base64!@#$%";
        let result: Result<Vec<u8>, ConvertError> =
            RhaiString(invalid_base64.to_string()).try_into();

        // Should fall back to treating it as regular string
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), invalid_base64.as_bytes());
    }

    #[test]
    fn test_rhai_string_to_bytes_invalid_base64_data_prefix() {
        let input = "data:base64,!!!invalid!!!";
        let result: Result<Vec<u8>, ConvertError> = RhaiString(input.to_string()).try_into();

        // Should return an error for invalid base64 after prefix
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConvertError::Base64DecodeError(_)
        ));
    }

    #[test]
    fn test_is_likely_base64_valid_cases() {
        assert!(is_likely_base64("SGVsbG8gV29ybGQ="));
        assert!(is_likely_base64("SGVsbG8gV29ybGQ"));

        let long_string = general_purpose::STANDARD
            .encode("This is a longer test string to verify base64 detection works correctly");
        assert!(is_likely_base64(&long_string));
    }

    #[test]
    fn test_is_likely_base64_invalid_cases() {
        assert!(!is_likely_base64("SGVsbG"));
        assert!(!is_likely_base64("Hello@World!"));
        assert!(!is_likely_base64(""));
        assert!(!is_likely_base64("Hello World"));
        assert!(!is_likely_base64(r#"{"name": "John"}"#));
        assert!(!is_likely_base64("SGVsbG8==="));
    }

    #[test]
    fn test_is_likely_base64_edge_cases() {
        assert!(is_likely_base64("AAAA"));
        assert!(is_likely_base64("SGk="));
        assert!(is_likely_base64("QQ=="));
        assert!(is_likely_base64("A+/A"));
    }

    #[test]
    fn test_roundtrip_conversion_utf8() {
        let original = r#"{"name": "John Doe", "age": 30, "email": "john@example.com"}"#;
        let bytes = original.as_bytes();

        let rhai_string = RhaiString::from(bytes).0;
        assert_eq!(rhai_string, original);

        let result: Result<Vec<u8>, ConvertError> = RhaiString(rhai_string).try_into();
        assert!(result.is_ok());

        let result_bytes = result.unwrap();
        assert_eq!(result_bytes, bytes);

        let final_string = String::from_utf8(result_bytes).unwrap();
        assert_eq!(final_string, original);
    }

    #[test]
    fn test_roundtrip_conversion_binary() {
        let original_bytes = &[0x00, 0x01, 0xFF, 0xFE, 0x42, 0x21];

        let rhai_string = RhaiString::from(original_bytes.as_slice()).0;
        assert!(rhai_string.starts_with("data:base64,"));

        let result: Result<Vec<u8>, ConvertError> = RhaiString(rhai_string).try_into();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_bytes);
    }

    #[test]
    fn test_json_converter_basic_types() {
        use serde_json::json;

        // Null
        let converter = JsonConverter::new(Value::Null);
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        assert!(dynamic.unwrap().is_unit());

        // Boolean
        let converter = JsonConverter::new(json!(true));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        assert_eq!(dynamic.unwrap().as_bool().unwrap(), true);

        // Integer
        let converter = JsonConverter::new(json!(42));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        assert_eq!(dynamic.unwrap().as_int().unwrap(), 42);

        // Float
        let converter = JsonConverter::new(json!(3.14));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        assert!((dynamic.unwrap().as_float().unwrap() - 3.14).abs() < f64::EPSILON);

        // String
        let converter = JsonConverter::new(json!("hello"));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        assert_eq!(
            dynamic.unwrap().into_immutable_string().unwrap().as_str(),
            "hello"
        );
    }

    #[test]
    fn test_json_converter_complex_types() {
        use serde_json::json;

        // Array
        let converter = JsonConverter::new(json!([1, 2, 3]));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        let arr = dynamic.unwrap().into_array().unwrap();
        assert_eq!(arr.len(), 3);

        // Object
        let converter = JsonConverter::new(json!({"key": "value", "number": 42}));
        let dynamic: Result<Dynamic, ConvertError> = converter.try_into();
        assert!(dynamic.is_ok());
        let map = dynamic.unwrap().try_cast::<Map>().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_rhai_map_conversion() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());

        let rhai_map = RhaiMap::from(Some(&map));
        assert_eq!(rhai_map.0.len(), 2);
        assert!(rhai_map.0.contains_key("key1"));
        assert!(rhai_map.0.contains_key("key2"));

        let empty_map = RhaiMap::from(None);
        assert!(empty_map.0.is_empty());
    }
}
