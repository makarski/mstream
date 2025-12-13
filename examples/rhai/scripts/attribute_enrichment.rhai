// Enrich event attributes with metadata and derived information
fn transform(data, attributes) {
    // Calculate content metrics
    // Note: 'data' is already decoded (e.g. a Map for JSON/BSON)
    let field_count = 0;

    if type_of(data) == "map" {
        field_count = data.len();
    }

    // Add metrics to attributes
    attributes["field_count"] = field_count.to_string();
    attributes["data_type"] = type_of(data);

    // Add processing timestamp
    attributes["processed_at"] = "2024-01-01T00:00:00Z";

    // Add data quality indicators
    if field_count == 0 {
        attributes["quality"] = "empty";
    } else if field_count < 5 {
        attributes["quality"] = "minimal";
    } else if field_count < 20 {
        attributes["quality"] = "standard";
    } else {
        attributes["quality"] = "complex";
    }

    // Pass through the data unchanged but with enriched attributes
    result(data, attributes)
}
