// Simple anonymization script for order data
// Masks customer names and emails while preserving order structure

fn transform(data, attributes) {
    // When batching is enabled, data is an array of documents
    // We need to create a new array with anonymized documents
    
    let anonymized_orders = [];
    
    for order in data {
        // Anonymize customer information
        order.customer_name = "ANONYMIZED";
        order.customer_email = "anonymized@example.com";
        
        // Add processing metadata
        order.anonymized = true;
        order.anonymized_at = timestamp();
        order.processed_by = "rhai-anonymizer";
        
        // Add to result array
        anonymized_orders.push(order);
    }
    
    // Update attributes with processing info
    attributes["anonymized"] = "true";
    attributes["processor"] = "anonymize_orders";
    attributes["batch_size"] = anonymized_orders.len().to_string();
    
    // Return the anonymized array
    return result(anonymized_orders, attributes);
}
