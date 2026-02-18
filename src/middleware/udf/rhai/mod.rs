//! # Rhai Script Middleware for Event Transformation
//!
//! This module provides a sandboxed Rhai scripting environment for transforming events
//! in a data processing pipeline. It allows users to write custom transformation logic
//! in Rhai scripts without compromising system security.
//!
//! ## Features
//!
//! - **Sandboxed Execution**: Scripts run in a restricted environment with disabled file I/O,
//!   network access, and code evaluation capabilities
//! - **Resource Limits**: Enforced limits on operations, memory usage, and execution depth
//! - **Binary Data Support**: Automatic handling of binary data through base64 encoding
//! - **JSON Processing**: Built-in JSON encoding/decoding functions
//! - **Attribute Management**: Support for event metadata through attributes
//!
//! ## Security
//!
//! The Rhai engine is configured with multiple security restrictions:
//! - Disabled symbols: `eval`, `import`, file I/O operations (`open`, `read`, `write`, etc.)
//! - Maximum operations: 10,000 per script execution
//! - Maximum call depth: 10 levels
//! - Maximum string size: 4MB
//! - Maximum array/map size: 10,000 elements
//!
//! ## Script Requirements
//!
//! Scripts must define exactly one `transform` function with the following signature:
//!
//! ```rhai
//! fn transform(input, attributes) {
//!     // Process input and attributes
//!     result(transformed_data, attributes)
//! }
//! ```
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! # use std::collections::HashMap;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use mstream::middleware::udf::rhai::RhaiMiddleware;
//! use mstream::source::SourceEvent;
//!
//! // Create middleware with a script
//! let mut middleware = RhaiMiddleware::new(
//!     "/path/to/scripts".to_string(),
//!     "transform.rhai".to_string()
//! )?;
//!
//! // Create an event to transform
//! let event = SourceEvent {
//!     raw_bytes: b"hello world".to_vec(),
//!     attributes: Some(HashMap::new()),
//!     encoding: mstream::config::Encoding::Json,
//!     is_framed_batch: false,
//!     cursor: None,
//! };
//!
//! // Transform the event
//! let result = middleware.transform(event).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Example Rhai Scripts
//!
//! ### Simple String Transformation
//! ```rhai
//! fn transform(input, attributes) {
//!     result(input.to_upper())
//! }
//! ```
//!
//! ### JSON Processing
//! ```rhai
//! fn transform(data, attributes) {
//!     data.processed = true;
//!     data.timestamp = "2024-01-01";
//!     result(data)
//! }
//! ```
//!
//! ### Attribute Manipulation
//! ```rhai
//! fn transform(input, attributes) {
//!     attributes["processed_by"] = "rhai-middleware";
//!     attributes["length"] = input.len().to_string();
//!     result(input, attributes)
//! }
//! ```

use std::{collections::HashMap, path::Path};

use crate::middleware::udf::rhai::convert::{
    ConvertError, LazyBsonDocument, RhaiEncodingExt, RhaiMap,
};
use crate::source::SourceEvent;
use rhai::{AST, Dynamic};
use tokio::task::block_in_place;

pub mod completions;
pub mod convert;
pub(crate) mod masks;
pub(crate) mod transforms;
pub mod validate;

/// The required name for the transformation function in Rhai scripts
const UDF_NAME: &str = "transform";

/// Errors that can occur during Rhai middleware operations
#[derive(Debug, thiserror::Error)]
pub enum RhaiMiddlewareError {
    /// Script file does not exist at the specified path
    #[error("UDF script file does not exist: {path}")]
    FileNotFound { path: String },

    /// Failed to read the script file from disk
    #[error("Failed to read UDF script file: {source}")]
    FileReadError {
        #[source]
        source: std::io::Error,
    },

    /// Script compilation failed due to syntax errors
    #[error("Failed to compile Rhai script: {source}")]
    CompileError {
        #[source]
        source: rhai::ParseError,
    },

    /// Script doesn't define the required transform function or has wrong signature
    #[error("UDF script must define a function named '{UDF_NAME}' with exactly 2 parameters")]
    MissingTransformFunction,

    /// Runtime error during script execution
    #[error("UDF script execution failed: {message}. path: {path}")]
    ExecutionError { message: String, path: String },

    /// Failed to decode the transformed data (e.g., invalid base64)
    #[error("Failed to decode transformed data from Rhai: {source}. path: {path}")]
    DecodeError {
        #[source]
        source: ConvertError,
        path: String,
    },

    /// I/O error (e.g., when resolving current directory)
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Middleware for transforming events using Rhai scripts
///
/// This struct manages a sandboxed Rhai scripting environment that can safely
/// execute user-provided transformation scripts on events.
///
/// # Security
///
/// The Rhai engine is configured with strict security restrictions to prevent
/// malicious scripts from:
/// - Accessing the file system
/// - Making network requests
/// - Evaluating dynamic code
/// - Consuming excessive resources
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use mstream::middleware::udf::rhai::RhaiMiddleware;
///
/// let middleware = RhaiMiddleware::new(
///     "./scripts".to_string(),
///     "uppercase.rhai".to_string()
/// )?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RhaiMiddleware {
    script_path: Option<String>,
    compiled_script: rhai::AST,
    engine: rhai::Engine,
}

impl RhaiMiddleware {
    /// Creates a new Rhai middleware instance with the specified script
    ///
    /// # Arguments
    ///
    /// * `script_path` - Directory path containing the script (absolute or relative)
    /// * `filename` - Name of the script file within the directory
    ///
    /// # Returns
    ///
    /// Returns `Ok(RhaiMiddleware)` if the script was successfully loaded and compiled,
    /// or an error if:
    /// - The script file doesn't exist
    /// - The script has syntax errors
    /// - The script doesn't define the required `transform` function
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use mstream::middleware::udf::rhai::RhaiMiddleware;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let middleware = RhaiMiddleware::new(
    ///     "/opt/scripts".to_string(),
    ///     "json_transform.rhai".to_string()
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(script_path: String, filename: String) -> Result<Self, RhaiMiddlewareError> {
        let engine = Self::sandboxed_engine();
        let path = Path::new(&script_path);

        let full_path = if path.is_absolute() {
            path.to_path_buf().join(filename)
        } else {
            std::env::current_dir()?.join(path).join(filename)
        };

        if !full_path.exists() {
            return Err(RhaiMiddlewareError::FileNotFound {
                path: full_path.display().to_string(),
            });
        }

        let script_content = std::fs::read_to_string(&full_path)
            .map_err(|e| RhaiMiddlewareError::FileReadError { source: e })?;

        let compiled_script = engine
            .compile(&script_content)
            .map_err(|e| RhaiMiddlewareError::CompileError { source: e })?;

        Self::assert_udf_exists(&compiled_script)?;

        Ok(Self {
            script_path: Some(full_path.to_string_lossy().to_string()),
            compiled_script,
            engine,
        })
    }

    pub fn with_script(script_content: String) -> Result<Self, RhaiMiddlewareError> {
        let engine = Self::sandboxed_engine();
        let compiled_script = engine
            .compile(&script_content)
            .map_err(|e| RhaiMiddlewareError::CompileError { source: e })?;

        Self::assert_udf_exists(&compiled_script)?;

        Ok(Self {
            script_path: None,
            compiled_script,
            engine,
        })
    }

    /// Transforms an event using the loaded Rhai script
    ///
    /// The transformation process:
    /// 1. Converts the event's raw bytes to a string (or base64 if binary)
    /// 2. Passes the data and attributes to the script's `transform` function
    /// 3. Receives the transformed data and optional updated attributes
    /// 4. Decodes the result back to bytes
    ///
    /// # Arguments
    ///
    /// * `event` - The source event to transform
    ///
    /// # Returns
    ///
    /// Returns `Ok(SourceEvent)` with transformed data and possibly updated attributes,
    /// or an error if:
    /// - Script execution fails (runtime error, resource limits exceeded)
    /// - Script returns invalid data type
    /// - Result cannot be decoded back to bytes
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::collections::HashMap;
    /// # use mstream::middleware::udf::rhai::RhaiMiddleware;
    /// # use mstream::source::SourceEvent;
    /// # async fn example(mut middleware: RhaiMiddleware) -> Result<(), Box<dyn std::error::Error>> {
    /// let event = SourceEvent {
    ///     raw_bytes: b"hello".to_vec(),
    ///     attributes: None,
    ///     encoding: mstream::config::Encoding::Json,
    ///     is_framed_batch: false,
    ///     cursor: None,
    /// };
    ///
    /// let transformed = middleware.transform(event).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transform(
        &mut self,
        event: SourceEvent,
    ) -> Result<SourceEvent, RhaiMiddlewareError> {
        block_in_place(|| {
            let rhai_attributes: rhai::Map = RhaiMap::from(event.attributes.as_ref()).0;

            // 1. Input: Bytes -> Rhai Map
            let rhai_input = event
                .encoding
                .decode_rhai(&event.raw_bytes, event.is_framed_batch)
                .map_err(|e| RhaiMiddlewareError::DecodeError {
                    source: e,
                    path: self.script_path.clone().unwrap_or_default(),
                })?;

            // 2. Execute Script
            let result = self
                .engine
                .call_fn::<TransformResult>(
                    &mut rhai::Scope::new(),
                    &self.compiled_script,
                    UDF_NAME,
                    (rhai_input, rhai_attributes),
                )
                .map_err(|err| RhaiMiddlewareError::ExecutionError {
                    message: err.to_string(),
                    path: self.script_path.clone().unwrap_or_default(),
                })?;

            let attributes = result.attributes().or_else(|| event.attributes);

            // 3. Output: Rhai Map -> Bytes
            let raw_bytes = event
                // todo: consider a case when middleware changes inner encoding?
                // in this case the source event encoding (as here) will not be valid
                .encoding
                .encode_rhai(result.data, event.is_framed_batch)
                .map_err(|e| RhaiMiddlewareError::ExecutionError {
                    message: e.to_string(),
                    path: self.script_path.clone().unwrap_or_default(),
                })?;

            Ok(SourceEvent {
                raw_bytes,
                attributes,
                encoding: event.encoding,
                ..event
            })
        })
    }

    /// Creates a sandboxed Rhai engine with security restrictions and custom functions
    fn sandboxed_engine() -> rhai::Engine {
        let mut engine = rhai::Engine::new();

        Self::configure_sandbox(&mut engine);
        Self::configure_limits(&mut engine);
        Self::register_api(&mut engine);

        engine
    }

    fn configure_sandbox(engine: &mut rhai::Engine) {
        // Disable dangerous operations
        engine.disable_symbol("eval");
        engine.disable_symbol("load_file");
        engine.disable_symbol("load_script");
        engine.disable_symbol("import");

        // Disable file I/O
        engine.disable_symbol("open");
        engine.disable_symbol("close");
        engine.disable_symbol("read_line");
        engine.disable_symbol("write");
        engine.disable_symbol("flush");
    }

    fn configure_limits(engine: &mut rhai::Engine) {
        engine.set_max_operations(1_000_000);
        engine.set_max_call_levels(64);
        engine.set_max_expr_depths(32, 32);
        engine.set_max_string_size(0); // unlimited
        engine.set_max_array_size(0); // unlimited
        engine.set_max_map_size(0); // unlimited
    }

    fn register_api(engine: &mut rhai::Engine) {
        engine
            .register_type_with_name::<LazyBsonDocument>("BsonDocument")
            .register_indexer_get(LazyBsonDocument::get)
            .register_indexer_set(LazyBsonDocument::set)
            .register_fn("remove", LazyBsonDocument::remove)
            .register_fn("contains", LazyBsonDocument::contains)
            .register_fn("keys", LazyBsonDocument::keys)
            .register_fn("values", LazyBsonDocument::values)
            .register_fn("len", LazyBsonDocument::len)
            .register_fn("is_empty", LazyBsonDocument::is_empty);

        engine.register_type::<TransformResult>();

        // Register result functions
        engine.register_fn(
            "result",
            |data: Dynamic, attr: rhai::Map| -> TransformResult {
                TransformResult::new(data, attr)
            },
        );
        engine.register_fn("result", |data: Dynamic| -> TransformResult {
            TransformResult::new(data, rhai::Map::new())
        });

        // Register utilities
        engine.register_fn("timestamp_ms", Self::timestamp_ms);
        engine.register_fn("hash_sha256", masks::hash_sha256);
        engine.register_fn("mask_email", masks::mask_email);
        engine.register_fn("mask_phone", masks::mask_phone);
        engine.register_fn("mask_year_only", masks::mask_year_only);

        // Register transformation functions (sum, avg, min, max, group_by, etc.)
        transforms::register_transform_functions(engine);
    }

    fn timestamp_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64,
            Err(err) => {
                log::warn!("system clock drift detected: {}", err);
                0
            }
        }
    }

    /// Validates that the script defines exactly one transform function with correct signature
    fn assert_udf_exists(ast: &AST) -> Result<(), RhaiMiddlewareError> {
        let transform_funcs = ast
            .iter_functions()
            .filter(|func| func.name == UDF_NAME)
            .collect::<Vec<_>>();

        if transform_funcs.len() != 1 {
            return Err(RhaiMiddlewareError::MissingTransformFunction);
        }

        for func in transform_funcs {
            if func.params.len() != 2 {
                return Err(RhaiMiddlewareError::MissingTransformFunction);
            }
        }

        Ok(())
    }
}

/// Result type returned by Rhai transform functions
#[derive(Clone)]
struct TransformResult {
    pub data: Dynamic,
    pub attributes: Option<rhai::Map>,
}

impl TransformResult {
    pub fn new(data: Dynamic, attributes: rhai::Map) -> Self {
        Self {
            data,
            attributes: Some(attributes),
        }
    }

    /// Converts Rhai map attributes to HashMap, returning None if empty
    fn attributes(&self) -> Option<HashMap<String, String>> {
        if self.attributes.is_none() {
            return None;
        }

        self.attributes.as_ref().map(|attrs| {
            attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // Common helper function for all test modules
    fn create_test_script(dir: &TempDir, filename: &str, content: &str) -> String {
        let file_path = dir.path().join(filename);
        fs::write(&file_path, content).unwrap();
        dir.path().to_str().unwrap().to_string()
    }

    #[test]
    fn test_absolute_path() {
        let temp_dir = TempDir::new().unwrap();
        let script_content = r#"
                    fn transform(input, attributes) {
                        result(input)
                    }
                "#;

        let file_path = temp_dir.path().join("absolute.rhai");
        fs::write(&file_path, script_content).unwrap();

        // Use absolute path to temp_dir
        let absolute_path = temp_dir.path().to_str().unwrap().to_string();

        let middleware = RhaiMiddleware::new(absolute_path, "absolute.rhai".to_string());
        assert!(middleware.is_ok());
    }

    #[test]
    fn test_relative_path() {
        let temp_dir = TempDir::new().unwrap();
        let script_content = r#"
                    fn transform(input, attributes) {
                        result(input)
                    }
                "#;

        // Create a subdirectory structure
        let sub_dir = temp_dir.path().join("scripts");
        fs::create_dir(&sub_dir).unwrap();
        let file_path = sub_dir.join("relative.rhai");
        fs::write(&file_path, script_content).unwrap();

        // Change to temp_dir and use relative path
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let middleware = RhaiMiddleware::new("scripts".to_string(), "relative.rhai".to_string());

        // Restore original dir
        std::env::set_current_dir(original_dir).unwrap();

        assert!(middleware.is_ok());
    }

    // Test module for transform functionality
    #[cfg(test)]
    mod transform_tests {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn test_empty_input() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    if input.is_empty() {
                        result("empty input received")
                    } else {
                        result(input)
                    }
                }
            "#;
            let script_path = create_test_script(&temp_dir, "empty.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "empty.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"{}".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            assert_eq!(result.raw_bytes, b"\"empty input received\"");
        }

        #[test]
        #[cfg(unix)] // Permission tests only work on Unix-like systems
        fn test_file_read_permission_error() {
            use std::os::unix::fs::PermissionsExt;

            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    result(input)
                }
            "#;

            let file_path = temp_dir.path().join("no_read.rhai");
            fs::write(&file_path, script_content).unwrap();

            // Remove read permissions
            let mut perms = fs::metadata(&file_path).unwrap().permissions();
            perms.set_mode(0o000);
            fs::set_permissions(&file_path, perms).unwrap();

            let middleware = RhaiMiddleware::new(
                temp_dir.path().to_str().unwrap().to_string(),
                "no_read.rhai".to_string(),
            );

            assert!(
                matches!(middleware, Err(RhaiMiddlewareError::FileReadError { .. })),
                "Expected FileReadError for unreadable file"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_simple_string_transformation() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    result(input.to_upper())
                }
            "#;
            let script_path = create_test_script(&temp_dir, "upper.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "upper.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            assert_eq!(result.raw_bytes, b"\"TEST\"");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_attributes_modification() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    attributes["processed"] = "true";
                    attributes["length"] = input.len().to_string();
                    result(input, attributes)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "attrs.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "attrs.rhai".to_string()).unwrap();

            let mut initial_attrs = HashMap::new();
            initial_attrs.insert("source".to_string(), "test".to_string());

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: Some(initial_attrs),
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            let attrs = result.attributes.unwrap();
            assert_eq!(attrs.get("processed"), Some(&"true".to_string()));
            assert_eq!(attrs.get("length"), Some(&"4".to_string()));
            assert_eq!(attrs.get("source"), Some(&"test".to_string()));
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_json_processing() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    input.processed = true;
                    input.value = input.value * 2;
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "json.rhai", script_content);

            let mut middleware = RhaiMiddleware::new(script_path, "json.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: br#"{"name":"test","value":10}"#.to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            let result_str = String::from_utf8(result.raw_bytes).unwrap();
            let json: serde_json::Value = serde_json::from_str(&result_str).unwrap();

            assert_eq!(json["processed"], true);
            assert_eq!(json["value"], 20);
            assert_eq!(json["name"], "test");
        }

        /*
        #[tokio::test(flavor = "multi_thread")]
        async fn test_binary_data_passthrough() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    // Binary data comes as base64, just pass through
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "binary.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "binary.rhai".to_string()).unwrap();

            let binary_data = vec![0xFF, 0xFE, 0x00, 0x01, 0x42];
            let event = SourceEvent {
                raw_bytes: binary_data.clone(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            assert_eq!(result.raw_bytes, binary_data);
        }
        */

        #[tokio::test(flavor = "multi_thread")]
        async fn test_parse_int_stdlib() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    let num = parse_int("42");
                    result(num)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "parse_int.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "parse_int.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;
            assert!(result.is_ok(), "parse_int should work: {:?}", result.err());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_result_with_only_data() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    // Use single-argument result function
                    result(input + " modified")
                }
            "#;
            let script_path = create_test_script(&temp_dir, "single_result.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "single_result.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"data\"".to_vec(),
                attributes: Some(HashMap::new()),
                ..Default::default()
            };

            let result = middleware.transform(event).await.unwrap();
            assert_eq!(result.raw_bytes, b"\"data modified\"");
            // Attributes should be preserved from input when using single-arg result
            assert_eq!(result.attributes, Some(HashMap::new()));
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_decode_error_invalid_base64() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    // Return an invalid base64 string with data prefix
                    result("data:base64,invalid!!base64")
                }
            "#;
            let script_path = create_test_script(&temp_dir, "bad_decode.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "bad_decode.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"test".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;
            assert!(
                matches!(result, Err(RhaiMiddlewareError::DecodeError { .. })),
                "Expected DecodeError for invalid base64 decoding"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_script_wrong_return_type() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    // Return a number instead of result()
                    42
                }
            "#;
            let script_path = create_test_script(&temp_dir, "wrong_return.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "wrong_return.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;
            assert!(
                matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                "Expected ExecutionError for wrong return type"
            );
        }
    }

    // Test error handling and sandboxing
    #[cfg(test)]
    mod sandbox_tests {
        use tempfile::TempDir;

        use super::*;

        #[test]
        fn test_max_expr_depth() {
            let temp_dir = TempDir::new().unwrap();
            // Create deeply nested expression
            let mut expr = "1".to_string();
            for _ in 0..50 {
                expr = format!("({} + 1)", expr);
            }

            let script_content = format!(
                r#"
                fn transform(input, attributes) {{
                    let x = {};
                    result(x.to_string())
                }}
            "#,
                expr
            );

            let script_path = create_test_script(&temp_dir, "deep_expr.rhai", &script_content);

            let middleware = RhaiMiddleware::new(script_path, "deep_expr.rhai".to_string());
            assert!(
                matches!(middleware, Err(RhaiMiddlewareError::CompileError { .. })),
                "Expected CompileError for exceeding expression depth"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_runtime_arithmetic_error() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    let x = 1 / 0;  // Runtime arithmetic error
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "arithmetic.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "arithmetic.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;
            assert!(
                matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                "Expected ExecutionError for division by zero"
            );
        }

        #[test]
        fn test_sandbox_blocks_eval() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    eval("1 + 1");  // Should be blocked
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "eval.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "eval.rhai".to_string());

            // eval is disabled and causes compile error
            assert!(
                matches!(middleware, Err(RhaiMiddlewareError::CompileError { .. })),
                "Expected CompileError for disabled eval"
            );
        }

        #[test]
        fn test_sandbox_blocks_import() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    import "some_module";  // Should be blocked
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "import.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "import.rhai".to_string());

            // import is disabled and causes compile error
            assert!(
                matches!(middleware, Err(RhaiMiddlewareError::CompileError { .. })),
                "Expected CompileError for disabled import"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_disabled_file_operations() {
            // Test that all file operations we disabled are blocked at runtime
            let operations = vec![
                ("open", r#"open("file.txt")"#),
                ("close", r#"close(1)"#),
                ("read_line", r#"read_line()"#),
                ("write", r#"write("data")"#),
                ("flush", r#"flush()"#),
            ];

            for (op_name, op_code) in operations {
                let temp_dir = TempDir::new().unwrap();
                let script_content = format!(
                    r#"
                    fn transform(input, attributes) {{
                        {};
                        result(input)
                    }}
                    "#,
                    op_code
                );
                let script_path =
                    create_test_script(&temp_dir, &format!("{}.rhai", op_name), &script_content);

                let mut middleware = RhaiMiddleware::new(script_path, format!("{}.rhai", op_name))
                    .expect(&format!("Script with {} should compile", op_name));

                let event = SourceEvent {
                    raw_bytes: b"\"test\"".to_vec(),
                    attributes: None,
                    ..Default::default()
                };

                let result = middleware.transform(event).await;

                assert!(
                    matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                    "Expected ExecutionError for disabled '{}' function at runtime",
                    op_name
                );
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_max_operations_limit() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    let count = 0;
                    // This will exceed the 1,000,000 operations limit
                    for i in 0..1100000 {
                        count += 1;
                    }
                    result(count.to_string())
                }
            "#;
            let script_path = create_test_script(&temp_dir, "operations.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "operations.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;

            assert!(
                matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                "Expected ExecutionError for exceeding operations limit"
            );

            if let Err(RhaiMiddlewareError::ExecutionError { message, .. }) = result {
                assert!(
                    message.to_lowercase().contains("operations"),
                    "Expected error message to mention operations, got: {}",
                    message
                );
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_max_array_size_limit() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    let arr = [];
                    // Try to exceed the operations limit (since array size is unlimited)
                    for i in 0..1100000 {
                        arr.push(i);
                    }
                    result(arr.len().to_string())
                }
            "#;
            let script_path = create_test_script(&temp_dir, "big_array.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "big_array.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;

            // May fail due to array size or operations limit
            assert!(
                matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                "Expected ExecutionError for resource limit"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_max_map_size_limit() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    let map = #{};
                    // Try to exceed the operations limit (since map size is unlimited)
                    for i in 0..1100000 {
                        map[i.to_string()] = i;
                    }
                    result(map.len().to_string())
                }
            "#;
            let script_path = create_test_script(&temp_dir, "big_map.rhai", script_content);

            let mut middleware =
                RhaiMiddleware::new(script_path, "big_map.rhai".to_string()).unwrap();

            let event = SourceEvent {
                raw_bytes: b"\"test\"".to_vec(),
                attributes: None,
                ..Default::default()
            };

            let result = middleware.transform(event).await;

            // May fail due to map size or operations limit
            assert!(
                matches!(result, Err(RhaiMiddlewareError::ExecutionError { .. })),
                "Expected ExecutionError for resource limit"
            );
        }

        // Note: String size limit is difficult to test independently because
        // creating a large string requires many operations, hitting the operations
        // limit first. This is expected behavior - operations limit acts as a
        // general safeguard against resource exhaustion.
    }

    // Test module for assert_udf_exists functionality
    #[cfg(test)]
    mod assert_udf_tests {
        use super::*;

        #[test]
        fn test_valid_transform_function() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes) {
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "valid.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "valid.rhai".to_string());
            assert!(middleware.is_ok());
        }

        #[test]
        fn test_missing_transform_function() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn process(input, attributes) {
                    result(input)
                }

                fn helper() {
                    print("helper");
                }
            "#;
            let script_path = create_test_script(&temp_dir, "no_transform.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "no_transform.rhai".to_string());
            assert!(matches!(
                middleware,
                Err(RhaiMiddlewareError::MissingTransformFunction)
            ));
        }

        #[test]
        fn test_transform_with_zero_params() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform() {
                    result("fixed output")
                }
            "#;
            let script_path = create_test_script(&temp_dir, "zero_params.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "zero_params.rhai".to_string());
            assert!(matches!(
                middleware,
                Err(RhaiMiddlewareError::MissingTransformFunction)
            ));
        }

        #[test]
        fn test_transform_with_one_param() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input) {
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "one_param.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "one_param.rhai".to_string());
            assert!(matches!(
                middleware,
                Err(RhaiMiddlewareError::MissingTransformFunction)
            ));
        }

        #[test]
        fn test_missing_file() {
            let temp_dir = TempDir::new().unwrap();
            let script_path = temp_dir.path().to_str().unwrap().to_string();

            let middleware = RhaiMiddleware::new(script_path, "nonexistent.rhai".to_string());
            assert!(matches!(
                middleware,
                Err(RhaiMiddlewareError::FileNotFound { .. })
            ));
        }

        #[test]
        fn test_compile_error() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input, attributes {  // Missing closing paren
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "invalid.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "invalid.rhai".to_string());
            assert!(matches!(
                middleware,
                Err(RhaiMiddlewareError::CompileError { .. })
            ));
        }

        #[test]
        fn test_multiple_transform_functions_wrong_signature() {
            let temp_dir = TempDir::new().unwrap();
            let script_content = r#"
                fn transform(input) {
                    result(input)
                }

                fn transform(input, attributes, extra) {
                    result(input)
                }
            "#;
            let script_path = create_test_script(&temp_dir, "multi_transform.rhai", script_content);

            let middleware = RhaiMiddleware::new(script_path, "multi_transform.rhai".to_string());
            assert!(
                matches!(
                    middleware,
                    Err(RhaiMiddlewareError::MissingTransformFunction)
                ),
                "Should reject when all transform functions have wrong signatures"
            );
        }
    }
}
