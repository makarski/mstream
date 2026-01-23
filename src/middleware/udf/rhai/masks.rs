use chrono::{DateTime, Datelike, TimeZone, Utc};
use rhai::{Dynamic, Map};

pub(super) fn hash_sha256(input: Dynamic) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.to_string());
    format!("{:x}", hasher.finalize())
}

pub(super) fn mask_email(email: &str) -> String {
    let Some((local, domain)) = email.split_once('@') else {
        return email.to_string();
    };

    if local.len() <= 1 {
        format!("*@{}", domain)
    } else {
        let first_char = local.chars().next().unwrap();
        format!("{}***@{}", first_char, domain)
    }
}

pub(super) fn mask_phone(phone: &str) -> String {
    if phone.len() <= 4 {
        return phone.to_string();
    }

    let visible = &phone[phone.len() - 4..];
    let masked = "*".repeat(phone.len() - 4);
    format!("{}{}", masked, visible)
}

pub(super) fn mask_year_only(input: Dynamic) -> Dynamic {
    mask_string_date(&input)
        .or_else(|| mask_ejson_date(&input))
        .unwrap_or(input)
}

// =============================================================================
// Date Masking Helpers
// =============================================================================

/// Mask a DateTime to January 1st of the same year
fn mask_to_year_start(dt: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0).unwrap()
}

/// Try to parse and mask a string date (RFC3339 or YYYY-MM-DD)
fn mask_string_date(input: &Dynamic) -> Option<Dynamic> {
    let date_str = input.clone().into_string().ok()?;

    // Try RFC3339 / ISO8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(&date_str) {
        let masked = mask_to_year_start(dt.with_timezone(&Utc));
        return Some(masked.to_rfc3339().into());
    }

    // Fallback to simple YYYY-MM-DD
    if let Ok(dt) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
        return Some(format!("{}-01-01", dt.format("%Y")).into());
    }

    None
}

/// Try to parse and mask a MongoDB EJSON date: {"$date": {"$numberLong": "..."}}
fn mask_ejson_date(input: &Dynamic) -> Option<Dynamic> {
    let map = input.clone().try_cast::<Map>()?;
    let millis = extract_ejson_millis(&map)?;
    let dt = Utc.timestamp_millis_opt(millis).single()?;

    let masked = mask_to_year_start(dt);
    Some(build_ejson_date(masked.timestamp_millis()))
}

/// Extract milliseconds from EJSON date structure
fn extract_ejson_millis(map: &Map) -> Option<i64> {
    let inner = map.get("$date")?.clone().try_cast::<Map>()?;
    let millis_str = inner.get("$numberLong")?.clone().into_string().ok()?;
    millis_str.parse().ok()
}

/// Build an EJSON date structure from milliseconds
fn build_ejson_date(millis: i64) -> Dynamic {
    let mut inner = Map::new();
    inner.insert("$numberLong".into(), millis.to_string().into());

    let mut outer = Map::new();
    outer.insert("$date".into(), inner.into());

    outer.into()
}
