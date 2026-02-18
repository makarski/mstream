use serde::Serialize;

use super::{RhaiMiddleware, RhaiMiddlewareError};

#[derive(Debug, Serialize)]
pub struct Diagnostic {
    pub line: usize,
    pub column: usize,
    pub end_line: usize,
    pub end_column: usize,
    pub message: String,
    pub severity: DiagnosticSeverity,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

pub fn validate_script(script: &str) -> Vec<Diagnostic> {
    match RhaiMiddleware::with_script(script.to_string()) {
        Ok(_) => Vec::new(),
        Err(err) => error_to_diagnostics(&err),
    }
}

fn error_to_diagnostics(err: &RhaiMiddlewareError) -> Vec<Diagnostic> {
    match err {
        RhaiMiddlewareError::CompileError { source } => {
            let position = source.position();
            let line = position.line().unwrap_or(1);
            let column = position.position().unwrap_or(1);
            let message = format!("{}", source.err_type());

            vec![Diagnostic {
                line,
                column,
                end_line: line,
                end_column: column,
                message,
                severity: DiagnosticSeverity::Error,
            }]
        }
        RhaiMiddlewareError::MissingTransformFunction => {
            vec![Diagnostic {
                line: 1,
                column: 1,
                end_line: 1,
                end_column: 1,
                message: "Script must define a 'transform(data, attributes)' function \
                          with exactly 2 parameters"
                    .to_string(),
                severity: DiagnosticSeverity::Error,
            }]
        }
        other => {
            vec![Diagnostic {
                line: 1,
                column: 1,
                end_line: 1,
                end_column: 1,
                message: other.to_string(),
                severity: DiagnosticSeverity::Error,
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_script_returns_no_diagnostics() {
        let script = r#"fn transform(data, attributes) { result(data, attributes) }"#;
        let diagnostics = validate_script(script);
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn syntax_error_returns_diagnostic_with_position() {
        let script = r#"fn transform(data, attributes { result(data, attributes) }"#;
        let diagnostics = validate_script(script);
        assert_eq!(diagnostics.len(), 1);
        assert!(matches!(diagnostics[0].severity, DiagnosticSeverity::Error));
        assert!(diagnostics[0].line >= 1);
        assert!(diagnostics[0].column >= 1);
    }

    #[test]
    fn missing_transform_function_returns_diagnostic() {
        let script = r#"fn process(data, attributes) { result(data, attributes) }"#;
        let diagnostics = validate_script(script);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("transform"));
    }

    #[test]
    fn wrong_parameter_count_returns_diagnostic() {
        let script = r#"fn transform(data) { result(data) }"#;
        let diagnostics = validate_script(script);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("transform"));
    }

    #[test]
    fn empty_script_returns_diagnostic() {
        let diagnostics = validate_script("");
        assert_eq!(diagnostics.len(), 1);
    }

    #[test]
    fn script_with_custom_functions_is_valid() {
        let script = r#"
            fn transform(data, attributes) {
                data.email = mask_email(data.email);
                data.phone = mask_phone(data.phone);
                data.hash = hash_sha256(data.ssn);
                data.year = mask_year_only(data.dob);
                data.ts = timestamp_ms();
                let total = sum([1, 2, 3]);
                let average = avg([1, 2, 3]);
                let items = unique([1, 2, 2]);
                let flat = flatten([[1], [2]]);
                result(data, attributes)
            }
        "#;
        let diagnostics = validate_script(script);
        assert!(
            diagnostics.is_empty(),
            "unexpected diagnostics: {:?}",
            diagnostics
        );
    }

    #[test]
    fn multiline_syntax_error_reports_correct_line() {
        let script = "fn transform(data, attributes) {\n    let x = 1;\n    let y = ;\n    result(data, attributes)\n}";
        let diagnostics = validate_script(script);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].line, 3);
    }
}
