// Enrich event attributes with metadata and derived information
fn transform(input, attributes) {
    // Calculate content metrics
    let input_length = input.len();
    let line_count = input.split("\n").len();
    let word_count = input.split(" ").len();

    // Detect content type
    let content_type = if input.starts_with("{") || input.starts_with("[") {
        "json"
    } else if input.starts_with("<?xml") {
        "xml"
    } else if input.starts_with("data:base64,") {
        "binary_base64"
    } else if input.contains("\t") {
        "tsv"
    } else if input.contains(",") && line_count > 1 {
        "csv"
    } else {
        "text"
    };

    // Add metrics to attributes
    attributes["content_type"] = content_type;
    attributes["byte_count"] = input_length.to_string();
    attributes["line_count"] = line_count.to_string();
    attributes["word_count"] = word_count.to_string();

    // Add processing timestamp
    attributes["processed_at"] = "2024-01-01T00:00:00Z";

    // Add data quality indicators
    if input_length == 0 {
        attributes["quality"] = "empty";
    } else if input_length < 10 {
        attributes["quality"] = "minimal";
    } else if input_length < 100 {
        attributes["quality"] = "small";
    } else if input_length < 1000 {
        attributes["quality"] = "medium";
    } else {
        attributes["quality"] = "large";
    }

    // Pass through the input unchanged but with enriched attributes
    result(input, attributes)
}
