//! User-Defined Function (UDF) middleware for event transformation.
//!
//! Allows users to transform events using custom scripts written in supported
//! scripting languages. Currently supports Rhai scripting with sandboxed execution.

use crate::middleware::udf::rhai::RhaiMiddleware;

pub mod rhai;

/// UDF middleware implementations.
///
/// Provides a unified interface for different scripting engines to transform events.
pub enum UdfMiddleware {
    /// Rhai scripting engine for safe, sandboxed transformations.
    Rhai(RhaiMiddleware),
}
