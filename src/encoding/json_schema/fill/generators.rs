//! Value generators for schema filling.
//!
//! This module contains functions for generating synthetic values based on
//! schema formats and field name heuristics.

use fake::Fake;
use fake::faker::address::en::{CityName, CountryName, PostCode, StateName, StreetName};
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::Word;
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::rand::rngs::StdRng;
use rand::Rng;
use uuid::Uuid;

use super::matchers::{
    DATE_KEYWORDS, DATE_PREFIXES, DATE_SUFFIXES, INTEGER_MATCHERS, NON_PERSON_NAME_PATTERNS,
    NUMBER_MATCHERS, PERSON_NAME_EXACT_MATCHES, STRING_MATCHERS, StringKind,
};

// =============================================================================
// Format-based Generation
// =============================================================================

/// Generate a string value based on the JSON Schema format.
pub fn generate_by_format(rng: &mut StdRng, format: &str) -> String {
    match format {
        "date-time" => gen_datetime(rng),
        "email" => SafeEmail().fake_with_rng(rng),
        "objectid" => gen_object_id(rng),
        "date" => gen_date(rng),
        "time" => gen_time(rng),
        "uri" | "url" => gen_url(rng),
        "uuid" => gen_uuid(rng),
        "ipv4" => gen_ipv4(rng),
        "ipv6" => gen_ipv6(rng),
        "hostname" => gen_hostname(rng),
        _ => Word().fake_with_rng(rng),
    }
}

pub fn gen_datetime(rng: &mut StdRng) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        rng.random_range(2020..=2025),
        rng.random_range(1..=12),
        rng.random_range(1..=28),
        rng.random_range(0..=23),
        rng.random_range(0..=59),
        rng.random_range(0..=59),
        rng.random_range(0..=999),
    )
}

pub fn gen_date(rng: &mut StdRng) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        rng.random_range(2020..=2025),
        rng.random_range(1..=12),
        rng.random_range(1..=28),
    )
}

pub fn gen_time(rng: &mut StdRng) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        rng.random_range(0..=23),
        rng.random_range(0..=59),
        rng.random_range(0..=59),
    )
}

pub fn gen_object_id(rng: &mut StdRng) -> String {
    let bytes: [u8; 12] = rng.random();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn gen_uuid(rng: &mut StdRng) -> String {
    Uuid::from_bytes(rng.random()).to_string()
}

pub fn gen_url(rng: &mut StdRng) -> String {
    format!(
        "https://example.com/{}",
        Word().fake_with_rng::<String, _>(rng)
    )
}

pub fn gen_ipv4(rng: &mut StdRng) -> String {
    format!(
        "{}.{}.{}.{}",
        rng.random_range(1..255),
        rng.random_range(0..255),
        rng.random_range(0..255),
        rng.random_range(1..255),
    )
}

pub fn gen_ipv6(rng: &mut StdRng) -> String {
    (0..8)
        .map(|_| format!("{:x}", rng.random_range(0..65535u16)))
        .collect::<Vec<_>>()
        .join(":")
}

pub fn gen_hostname(rng: &mut StdRng) -> String {
    format!("{}.example.com", Word().fake_with_rng::<String, _>(rng))
}

// =============================================================================
// Field Name Heuristics
// =============================================================================

/// Generate a string value based on field name heuristics.
pub fn generate_string_by_name(rng: &mut StdRng, name: &str) -> String {
    // Check registered matchers first
    for matcher in STRING_MATCHERS {
        if matcher.pattern.matches(name) {
            return generate_string_kind(rng, matcher.kind);
        }
    }

    // Check person name (complex logic)
    if is_person_name_field(name) {
        return Name().fake_with_rng(rng);
    }

    // Check date fields
    if is_date_field(name) {
        return gen_datetime(rng);
    }

    // Default
    Word().fake_with_rng(rng)
}

/// Generate a string value for a specific StringKind.
pub fn generate_string_kind(rng: &mut StdRng, kind: StringKind) -> String {
    match kind {
        StringKind::ObjectId => gen_object_id(rng),
        StringKind::Email => SafeEmail().fake_with_rng(rng),
        StringKind::FirstName => FirstName().fake_with_rng(rng),
        StringKind::LastName => LastName().fake_with_rng(rng),
        StringKind::FullName => Name().fake_with_rng(rng),
        StringKind::Phone => PhoneNumber().fake_with_rng(rng),
        StringKind::Street => StreetName().fake_with_rng(rng),
        StringKind::City => CityName().fake_with_rng(rng),
        StringKind::State => StateName().fake_with_rng(rng),
        StringKind::Country => CountryName().fake_with_rng(rng),
        StringKind::PostalCode => PostCode().fake_with_rng(rng),
        StringKind::FullAddress => format!(
            "{} {}",
            rng.random_range(1..=9999),
            StreetName().fake_with_rng::<String, _>(rng)
        ),
        StringKind::Ssn => format!(
            "{:03}-{:02}-{:04}",
            rng.random_range(100..=999),
            rng.random_range(10..=99),
            rng.random_range(1000..=9999),
        ),
        StringKind::CreditCard => String::from("4111111111111111"),
        StringKind::Cvv => format!("{:03}", rng.random_range(100..=999)),
    }
}

/// Generate an integer value based on field name heuristics.
pub fn generate_integer_by_name(rng: &mut StdRng, name: &str) -> i64 {
    for matcher in INTEGER_MATCHERS {
        if matcher.pattern.matches(name) {
            return rng.random_range(matcher.range.min..=matcher.range.max);
        }
    }
    rng.random_range(1..=1000)
}

/// Generate a number (float) value based on field name heuristics.
pub fn generate_number_by_name(rng: &mut StdRng, name: &str) -> f64 {
    for matcher in NUMBER_MATCHERS {
        if matcher.pattern.matches(name) {
            let value = rng.random_range(matcher.range.min..=matcher.range.max);
            return (value * 100.0).round() / 100.0;
        }
    }
    rng.random_range(1.0..=1000.0)
}

// =============================================================================
// Field Classification Helpers
// =============================================================================

/// Check if a field name represents a person's name.
pub fn is_person_name_field(name: &str) -> bool {
    if PERSON_NAME_EXACT_MATCHES.contains(&name) {
        return true;
    }

    if name.contains("name") {
        return !NON_PERSON_NAME_PATTERNS.iter().any(|p| name.contains(p));
    }

    false
}

/// Check if a field name represents a date/time value.
pub fn is_date_field(name: &str) -> bool {
    DATE_SUFFIXES.iter().any(|s| name.ends_with(s))
        || DATE_PREFIXES.iter().any(|p| name.starts_with(p))
        || DATE_KEYWORDS.iter().any(|k| name.contains(k))
}
