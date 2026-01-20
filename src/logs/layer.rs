use std::fmt;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use super::{LogBuffer, LogEntry, LogLevel};

/// A tracing Layer that captures log events to a LogBuffer
#[derive(Clone)]
pub struct LogBufferLayer {
    buffer: LogBuffer,
}

impl LogBufferLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self { buffer }
    }
}

thread_local! {
    static RECURSION_GUARD: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

impl<S> Layer<S> for LogBufferLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Prevent infinite recursion if internal components log
        let is_recursive = RECURSION_GUARD.with(|guard| {
            if guard.get() {
                true
            } else {
                guard.set(true);
                false
            }
        });

        if is_recursive {
            return;
        }

        // Use a scope guard to ensure we reset the flag even if we panic (though we shouldn't)
        struct GuardReset;
        impl Drop for GuardReset {
            fn drop(&mut self) {
                RECURSION_GUARD.with(|guard| guard.set(false));
            }
        }
        let _reset_guard = GuardReset;

        let metadata = event.metadata();
        let level = LogLevel::from(metadata.level());
        let target = metadata.target().to_string();

        // Extract fields from the event
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        // Try to get job_name from span context if not in event fields
        let job_name = visitor.job_name.or_else(|| {
            ctx.event_scope(event).and_then(|scope| {
                for span in scope {
                    let extensions = span.extensions();
                    if let Some(fields) = extensions.get::<SpanFields>() {
                        if let Some(ref name) = fields.job_name {
                            return Some(name.clone());
                        }
                    }
                }
                None
            })
        });

        let entry = LogEntry {
            timestamp: chrono::Utc::now(),
            level,
            message: visitor.message,
            job_name,
            target,
        };

        self.buffer.push(entry);
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span not found");

        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);

        if visitor.job_name.is_some() {
            let mut extensions = span.extensions_mut();
            extensions.insert(SpanFields {
                job_name: visitor.job_name,
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span not found");

        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);

        if visitor.job_name.is_some() {
            let mut extensions = span.extensions_mut();
            if let Some(fields) = extensions.get_mut::<SpanFields>() {
                if visitor.job_name.is_some() {
                    fields.job_name = visitor.job_name;
                }
            } else {
                extensions.insert(SpanFields {
                    job_name: visitor.job_name,
                });
            }
        }
    }
}

/// Storage for span fields we care about
#[derive(Debug)]
struct SpanFields {
    job_name: Option<String>,
}

/// Visitor to extract fields from events and spans
#[derive(Default)]
struct FieldVisitor {
    message: String,
    job_name: Option<String>,
}

/// Check if string is surrounded by double quotes
fn is_quoted(s: &str) -> bool {
    s.len() >= 2 && s.starts_with('"') && s.ends_with('"')
}

/// Strip surrounding quotes from Debug-formatted strings
fn strip_debug_quotes(mut s: String) -> String {
    if is_quoted(&s) {
        s.pop();
        s.remove(0);
    }
    s
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // Only process fields we care about - skip others to avoid allocation
        match field.name() {
            "message" => self.message = strip_debug_quotes(format!("{value:?}")),
            "job_name" => self.job_name = Some(strip_debug_quotes(format!("{value:?}"))),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_string(),
            "job_name" => self.job_name = Some(value.to_string()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    #[test]
    fn test_layer_captures_basic_event() {
        let buffer = LogBuffer::new(100);
        let layer = LogBufferLayer::new(buffer.clone());

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("test message");
        });

        let entries = buffer.get_entries(&super::super::LogFilter::new());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].level, LogLevel::Info);
        assert!(entries[0].message.contains("test message"));
    }

    #[test]
    fn test_layer_captures_job_name_field() {
        let buffer = LogBuffer::new(100);
        let layer = LogBufferLayer::new(buffer.clone());

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(job_name = "my-job", "processing");
        });

        let entries = buffer.get_entries(&super::super::LogFilter::new());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].job_name, Some("my-job".to_string()));
    }

    #[test]
    fn test_layer_captures_job_name_from_span() {
        let buffer = LogBuffer::new(100);
        let layer = LogBufferLayer::new(buffer.clone());

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("job_span", job_name = "span-job");
            let _guard = span.enter();
            tracing::info!("inside span");
        });

        let entries = buffer.get_entries(&super::super::LogFilter::new());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].job_name, Some("span-job".to_string()));
    }

    #[test]
    fn test_strip_debug_quotes() {
        assert_eq!(strip_debug_quotes(r#""test""#.to_string()), "test");
        assert_eq!(strip_debug_quotes(r#""test"#.to_string()), "\"test");
        assert_eq!(strip_debug_quotes(r#"test""#.to_string()), "test\"");
        assert_eq!(strip_debug_quotes(r#""""#.to_string()), "");
        assert_eq!(strip_debug_quotes(r#""#.to_string()), "");
        assert_eq!(strip_debug_quotes(r#"a"#.to_string()), "a");
        assert_eq!(strip_debug_quotes(r#"ab"#.to_string()), "ab");
        // Ensure unicode safety - slicing at index 1 is only safe if first char is 1 byte '"'
        // But the check s.starts_with('"') guarantees first byte is valid ascii quote.
        assert_eq!(strip_debug_quotes(r#""🔥""#.to_string()), "🔥");
    }
}
