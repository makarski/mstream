//! Testing module for transform scripts.
//!
//! This module provides functionality to:
//! - Run test cases against transform scripts
//! - Verify assertions on transform output
//! - Derive schemas and assertions from sample data
//!
//! # Structure
//!
//! - `assertions`: Assertion checking and verification
//! - `runner`: Test case execution
//! - `schema`: Schema and assertion derivation

pub mod assertions;
pub mod runner;
pub mod schema;

// Re-export commonly used items
pub use assertions::{check_assertions, check_extra_fields, get_value_at_path};
pub use runner::{run_test_case, run_test_cases};
pub use schema::{derive_assertions, derive_schema_from_value, infer_type_schema};
