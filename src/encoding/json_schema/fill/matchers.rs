//! Field name pattern matching infrastructure and matcher registries.
//!
//! This module provides the pattern matching system used to generate realistic
//! synthetic data based on field names.

// =============================================================================
// Pattern Matching Infrastructure
// =============================================================================

/// Pattern for matching field names
#[derive(Clone, Copy)]
#[allow(dead_code)] // Variants used in static matcher tables
pub enum FieldPattern {
    /// Field name ends with this suffix
    EndsWith(&'static str),
    /// Field name contains this substring
    Contains(&'static str),
    /// Field name matches any of these exactly
    ExactAny(&'static [&'static str]),
    /// Field name contains any of these substrings
    ContainsAny(&'static [&'static str]),
}

impl FieldPattern {
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::EndsWith(s) => name.ends_with(s),
            Self::Contains(s) => name.contains(s),
            Self::ExactAny(patterns) => patterns.contains(&name),
            Self::ContainsAny(patterns) => patterns.iter().any(|p| name.contains(p)),
        }
    }
}

// =============================================================================
// Value Kind Enums
// =============================================================================

/// Types of string values that can be generated
#[derive(Clone, Copy)]
#[allow(dead_code)] // Variants used in static matcher tables
pub enum StringKind {
    ObjectId,
    Email,
    FirstName,
    LastName,
    FullName,
    Phone,
    Street,
    City,
    State,
    Country,
    PostalCode,
    FullAddress,
    Ssn,
    CreditCard,
    Cvv,
}

// =============================================================================
// Matcher Structs
// =============================================================================

/// Matcher entry for string fields
pub struct StringMatcher {
    pub pattern: FieldPattern,
    pub kind: StringKind,
}

/// Integer range definition
pub struct IntRange {
    pub min: i64,
    pub max: i64,
}

impl IntRange {
    pub const fn new(min: i64, max: i64) -> Self {
        Self { min, max }
    }
}

/// Matcher entry for integer fields
pub struct IntegerMatcher {
    pub pattern: FieldPattern,
    pub range: IntRange,
}

/// Float range definition
pub struct FloatRange {
    pub min: f64,
    pub max: f64,
}

impl FloatRange {
    pub const fn new(min: f64, max: f64) -> Self {
        Self { min, max }
    }
}

/// Matcher entry for number fields
pub struct NumberMatcher {
    pub pattern: FieldPattern,
    pub range: FloatRange,
}

// =============================================================================
// Static Matcher Registries
// =============================================================================

/// String field matchers in priority order
pub static STRING_MATCHERS: &[StringMatcher] = &[
    // IDs - highest priority
    StringMatcher {
        pattern: FieldPattern::ExactAny(&["_id", "id"]),
        kind: StringKind::ObjectId,
    },
    StringMatcher {
        pattern: FieldPattern::EndsWith("_id"),
        kind: StringKind::ObjectId,
    },
    // Contact info
    StringMatcher {
        pattern: FieldPattern::Contains("email"),
        kind: StringKind::Email,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["phone", "mobile"]),
        kind: StringKind::Phone,
    },
    // Names - specific before generic
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["firstname", "first_name"]),
        kind: StringKind::FirstName,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["lastname", "last_name"]),
        kind: StringKind::LastName,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["full_name", "fullname"]),
        kind: StringKind::FullName,
    },
    // Address components
    StringMatcher {
        pattern: FieldPattern::Contains("street"),
        kind: StringKind::Street,
    },
    StringMatcher {
        pattern: FieldPattern::Contains("city"),
        kind: StringKind::City,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["state", "province"]),
        kind: StringKind::State,
    },
    StringMatcher {
        pattern: FieldPattern::Contains("country"),
        kind: StringKind::Country,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["zip", "postal", "postcode"]),
        kind: StringKind::PostalCode,
    },
    StringMatcher {
        pattern: FieldPattern::Contains("address"),
        kind: StringKind::FullAddress,
    },
    // Financial
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["ssn", "social"]),
        kind: StringKind::Ssn,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&[
            "creditcard",
            "credit_card",
            "cardnumber",
            "card_number",
        ]),
        kind: StringKind::CreditCard,
    },
    StringMatcher {
        pattern: FieldPattern::ContainsAny(&["cvv", "cvc"]),
        kind: StringKind::Cvv,
    },
];

/// Integer field matchers in priority order
pub static INTEGER_MATCHERS: &[IntegerMatcher] = &[
    IntegerMatcher {
        pattern: FieldPattern::Contains("credits"),
        range: IntRange::new(0, 180),
    },
    IntegerMatcher {
        pattern: FieldPattern::ExactAny(&[
            "year_of_study",
            "study_year",
            "academic_year",
            "year_level",
            "school_year",
        ]),
        range: IntRange::new(1, 8),
    },
    IntegerMatcher {
        pattern: FieldPattern::Contains("year_of_study"),
        range: IntRange::new(1, 8),
    },
    IntegerMatcher {
        pattern: FieldPattern::Contains("year"),
        range: IntRange::new(2020, 2025),
    },
    IntegerMatcher {
        pattern: FieldPattern::Contains("salary"),
        range: IntRange::new(30000, 200000),
    },
    IntegerMatcher {
        pattern: FieldPattern::Contains("age"),
        range: IntRange::new(18, 80),
    },
    IntegerMatcher {
        pattern: FieldPattern::ContainsAny(&["count", "total"]),
        range: IntRange::new(0, 100),
    },
    IntegerMatcher {
        pattern: FieldPattern::Contains("h_index"),
        range: IntRange::new(0, 100),
    },
];

/// Number (float) field matchers in priority order
pub static NUMBER_MATCHERS: &[NumberMatcher] = &[
    NumberMatcher {
        pattern: FieldPattern::ExactAny(&["lat", "latitude"]),
        range: FloatRange::new(-90.0, 90.0),
    },
    NumberMatcher {
        pattern: FieldPattern::Contains("latitude"),
        range: FloatRange::new(-90.0, 90.0),
    },
    NumberMatcher {
        pattern: FieldPattern::ExactAny(&["lng", "lon"]),
        range: FloatRange::new(-180.0, 180.0),
    },
    NumberMatcher {
        pattern: FieldPattern::Contains("longitude"),
        range: FloatRange::new(-180.0, 180.0),
    },
    NumberMatcher {
        pattern: FieldPattern::Contains("gpa"),
        range: FloatRange::new(0.0, 4.0),
    },
    NumberMatcher {
        pattern: FieldPattern::ContainsAny(&["percent", "ratio"]),
        range: FloatRange::new(0.0, 100.0),
    },
    NumberMatcher {
        pattern: FieldPattern::ContainsAny(&["price", "cost"]),
        range: FloatRange::new(1.0, 1000.0),
    },
    NumberMatcher {
        pattern: FieldPattern::Contains("salary"),
        range: FloatRange::new(30000.0, 200000.0),
    },
    NumberMatcher {
        pattern: FieldPattern::Contains("h_index"),
        range: FloatRange::new(0.0, 100.0),
    },
];

/// Patterns that indicate non-person name fields
pub static NON_PERSON_NAME_PATTERNS: &[&str] = &[
    "filename",
    "file_name",
    "hostname",
    "host_name",
    "pathname",
    "path_name",
    "dirname",
    "dir_name",
    "typename",
    "type_name",
    "classname",
    "class_name",
    "tablename",
    "table_name",
    "colname",
    "col_name",
    "column_name",
    "fieldname",
    "field_name",
    "varname",
    "var_name",
    "keyname",
    "key_name",
    "tagname",
    "tag_name",
    "nickname",
    "codename",
    "code_name",
    "servicename",
    "service_name",
    "dbname",
    "db_name",
    "schemaname",
    "schema_name",
];

/// Field names that are clearly person names
pub static PERSON_NAME_EXACT_MATCHES: &[&str] = &[
    "name",
    "person_name",
    "contact_name",
    "author_name",
    "customer_name",
    "employee_name",
    "student_name",
    "user_name",
    "display_name",
    "legal_name",
];

/// Date field suffixes
pub static DATE_SUFFIXES: &[&str] = &["_at", "_date", "_time", "_on"];

/// Date field prefixes
pub static DATE_PREFIXES: &[&str] = &["date_of_", "time_of_", "date_"];

/// Keywords that indicate a date field
pub static DATE_KEYWORDS: &[&str] = &[
    "created",
    "updated",
    "modified",
    "deleted",
    "timestamp",
    "datetime",
    "birthdate",
    "birthday",
    "dob",
    "enrolled",
    "started",
    "finished",
    "completed",
    "expired",
    "expiry",
    "login",
    "last_seen",
    "registered",
];

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_exact_single() {
        assert!(FieldPattern::ExactAny(&["test"]).matches("test"));
        assert!(!FieldPattern::ExactAny(&["test"]).matches("testing"));
    }

    #[test]
    fn pattern_ends_with() {
        assert!(FieldPattern::EndsWith("_id").matches("user_id"));
        assert!(!FieldPattern::EndsWith("_id").matches("identity"));
    }

    #[test]
    fn pattern_contains() {
        assert!(FieldPattern::Contains("email").matches("user_email"));
        assert!(FieldPattern::Contains("email").matches("email_address"));
    }

    #[test]
    fn pattern_exact_any() {
        let pattern = FieldPattern::ExactAny(&["a", "b", "c"]);
        assert!(pattern.matches("a"));
        assert!(pattern.matches("b"));
        assert!(!pattern.matches("ab"));
    }

    #[test]
    fn pattern_contains_any() {
        let pattern = FieldPattern::ContainsAny(&["foo", "bar"]);
        assert!(pattern.matches("prefix_foo_suffix"));
        assert!(pattern.matches("bar"));
        assert!(!pattern.matches("baz"));
    }
}
