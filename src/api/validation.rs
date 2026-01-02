use serde::Serialize;

/// Validation error for a specific field
#[derive(Debug, Clone, Serialize)]
pub struct ValidationError {
    pub field: String,
    pub reason: String,
}

impl ValidationError {
    pub fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

/// Trait for types that can be validated
pub trait Validate {
    fn validate(&self) -> Result<(), Vec<ValidationError>>;
}

/// Validate a resource name (job name, service name, etc.)
pub fn validate_resource_name(name: &str, resource_type: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError::new(
            "name",
            format!("{} name cannot be empty", resource_type),
        ));
    }

    if name.len() > 64 {
        return Err(ValidationError::new(
            "name",
            format!("{} name cannot exceed 64 characters", resource_type),
        ));
    }

    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ValidationError::new(
            "name",
            format!(
                "{} name can only contain alphanumeric characters, hyphens, and underscores",
                resource_type
            ),
        ));
    }

    Ok(())
}
