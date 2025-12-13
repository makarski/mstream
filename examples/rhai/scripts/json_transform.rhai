// Transform user events by enriching with calculated fields
fn transform(data, attributes) {
    // Add processing metadata
    data.processed_at = "2024-01-01T00:00:00Z";
    data.version = "1.0";

    // Calculate loyalty tier based on points collected
    if "loyalty_points" in data {
        if data.loyalty_points >= 10000 {
            data.tier = "platinum";
            data.discount_percentage = 20;
        } else if data.loyalty_points >= 5000 {
            data.tier = "gold";
            data.discount_percentage = 15;
        } else if data.loyalty_points >= 1000 {
            data.tier = "silver";
            data.discount_percentage = 10;
        } else if data.loyalty_points >= 100 {
            data.tier = "bronze";
            data.discount_percentage = 5;
        } else {
            data.tier = "member";
            data.discount_percentage = 0;
        }

        // Calculate perks based on tier
        data.free_shipping = data.loyalty_points >= 1000;
        data.priority_support = data.loyalty_points >= 5000;
        data.exclusive_access = data.loyalty_points >= 10000;
    }

    // Calculate full name if first and last names exist
    if "first_name" in data && "last_name" in data {
        data.full_name = data.first_name + " " + data.last_name;
    }

    // Normalize email to lowercase
    if "email" in data {
        data.email = data.email.to_lower();
    }

    // Remove sensitive fields
    data.remove("password");
    data.remove("ssn");
    data.remove("credit_card");

    // Update attributes with processing info
    attributes["processor"] = "json_transform";
    if "tier" in data {
        attributes["loyalty_tier"] = data.tier;
    }

    // Return transformed data and attributes
    result(data, attributes)
}
