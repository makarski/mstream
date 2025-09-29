// Filter and reshape product events based on business rules
fn transform(input, attributes) {
    let product = json_decode(input);

    // Filter logic - only process active products with valid prices
    if !product.active || product.price <= 0 {
        // Mark as filtered in attributes
        attributes["filtered"] = "true";
        attributes["filter_reason"] = if !product.active {
            "inactive_product"
        } else {
            "invalid_price"
        };

        // Return empty object for filtered items
        result(json_encode(#{}), attributes)
    } else {
        // Reshape product data
        let output = #{};

        // Core fields
        output.id = product.id;
        output.name = product.name;
        output.price = product.price;

        // Calculate pricing tiers
        if product.price < 10 {
            output.price_tier = "budget";
        } else if product.price < 50 {
            output.price_tier = "standard";
        } else if product.price < 200 {
            output.price_tier = "premium";
        } else {
            output.price_tier = "luxury";
        }

        // Add discount if applicable
        if "discount_percentage" in product && product.discount_percentage > 0 {
            output.discounted_price = product.price * (1.0 - product.discount_percentage / 100.0);
            output.savings = product.price - output.discounted_price;
        }

        // Category mapping
        if "category" in product {
            output.category = product.category.to_upper();
            output.department = switch product.category {
                "electronics" | "computers" | "phones" => "technology",
                "clothing" | "shoes" | "accessories" => "fashion",
                "food" | "beverages" | "snacks" => "grocery",
                "furniture" | "decor" | "garden" => "home",
                _ => "general"
            };
        }

        // Stock status
        if "quantity" in product {
            output.in_stock = product.quantity > 0;
            output.stock_level = if product.quantity == 0 {
                "out_of_stock"
            } else if product.quantity < 10 {
                "low"
            } else if product.quantity < 50 {
                "medium"
            } else {
                "high"
            };
        }

        // Update attributes
        attributes["filtered"] = "false";
        attributes["price_tier"] = output.price_tier;
        attributes["department"] = output.department;

        result(json_encode(output), attributes)
    }
}
