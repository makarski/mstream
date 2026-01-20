use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::broadcast;

mod layer;

pub use layer::LogBufferLayer;

/// Log level for entries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<&tracing::Level> for LogLevel {
    fn from(level: &tracing::Level) -> Self {
        match *level {
            tracing::Level::TRACE => LogLevel::Trace,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::ERROR => LogLevel::Error,
        }
    }
}

impl LogLevel {
    /// Returns true if self is at or above the given minimum level
    pub fn meets_minimum(&self, min: LogLevel) -> bool {
        self.severity() >= min.severity()
    }

    fn severity(&self) -> u8 {
        match self {
            LogLevel::Trace => 0,
            LogLevel::Debug => 1,
            LogLevel::Info => 2,
            LogLevel::Warn => 3,
            LogLevel::Error => 4,
        }
    }

    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "trace" => Some(LogLevel::Trace),
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "warn" | "warning" => Some(LogLevel::Warn),
            "error" => Some(LogLevel::Error),
            _ => None,
        }
    }
}

/// A single log entry
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    pub target: String,
}

/// Shared log buffer with ring buffer storage and broadcast for live subscribers
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<LogBufferInner>,
}

struct LogBufferInner {
    buffer: RwLock<VecDeque<Arc<LogEntry>>>,
    capacity: usize,
    broadcast_tx: broadcast::Sender<Arc<LogEntry>>,
}

impl LogBuffer {
    /// Create a new log buffer with the given capacity
    pub fn new(capacity: usize) -> Self {
        // Broadcast channel capacity - if subscribers lag, they'll miss events
        let (broadcast_tx, _) = broadcast::channel(1024);

        Self {
            inner: Arc::new(LogBufferInner {
                buffer: RwLock::new(VecDeque::with_capacity(capacity)),
                capacity,
                broadcast_tx,
            }),
        }
    }

    /// Push a new log entry, evicting oldest if at capacity
    pub fn push(&self, entry: LogEntry) {
        let entry = Arc::new(entry);
        // Send to broadcast channel (ignore if no receivers)
        let _ = self.inner.broadcast_tx.send(entry.clone());

        // Add to ring buffer
        let mut buffer = self.inner.buffer.write();
        if buffer.len() >= self.inner.capacity {
            buffer.pop_front();
        }
        buffer.push_back(entry);
    }

    /// Get recent entries, optionally filtered
    pub fn get_entries(&self, filter: &LogFilter) -> Vec<Arc<LogEntry>> {
        let buffer = self.inner.buffer.read();

        buffer
            .iter()
            .filter(|entry| filter.matches(entry))
            .cloned()
            .collect()
    }

    /// Get the last N entries, optionally filtered
    pub fn get_recent(&self, limit: usize, filter: &LogFilter) -> Vec<Arc<LogEntry>> {
        let buffer = self.inner.buffer.read();

        let mut entries: Vec<Arc<LogEntry>> = buffer
            .iter()
            .rev()
            .filter(|entry| filter.matches(entry))
            .take(limit)
            .cloned()
            .collect();

        entries.reverse();
        entries
    }

    /// Subscribe to live log entries
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LogEntry>> {
        self.inner.broadcast_tx.subscribe()
    }

    /// Get current buffer size
    pub fn len(&self) -> usize {
        self.inner.buffer.read().len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Filter for log queries
#[derive(Debug, Default)]
pub struct LogFilter {
    pub job_name: Option<String>,
    pub min_level: Option<LogLevel>,
    pub since: Option<DateTime<Utc>>,
}

impl LogFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_job_name(mut self, job_name: impl Into<String>) -> Self {
        self.job_name = Some(job_name.into());
        self
    }

    pub fn with_min_level(mut self, level: LogLevel) -> Self {
        self.min_level = Some(level);
        self
    }

    pub fn with_since(mut self, since: DateTime<Utc>) -> Self {
        self.since = Some(since);
        self
    }

    /// Check if an entry matches this filter
    pub fn matches(&self, entry: &LogEntry) -> bool {
        let job_matches = self
            .job_name
            .as_ref()
            .map_or(true, |name| entry.job_name.as_ref() == Some(name));

        let level_matches = self
            .min_level
            .map_or(true, |min| entry.level.meets_minimum(min));

        let since_matches = self.since.map_or(true, |since| entry.timestamp >= since);

        job_matches && level_matches && since_matches
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(level: LogLevel, job_name: Option<&str>, message: &str) -> LogEntry {
        LogEntry {
            timestamp: Utc::now(),
            level,
            message: message.to_string(),
            job_name: job_name.map(|s| s.to_string()),
            target: "test".to_string(),
        }
    }

    #[test]
    fn test_log_buffer_push_and_get() {
        let buffer = LogBuffer::new(100);

        buffer.push(make_entry(LogLevel::Info, None, "test message"));
        buffer.push(make_entry(LogLevel::Warn, Some("job1"), "warning"));

        let entries = buffer.get_entries(&LogFilter::new());
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_log_buffer_capacity() {
        let buffer = LogBuffer::new(3);

        buffer.push(make_entry(LogLevel::Info, None, "msg1"));
        buffer.push(make_entry(LogLevel::Info, None, "msg2"));
        buffer.push(make_entry(LogLevel::Info, None, "msg3"));
        buffer.push(make_entry(LogLevel::Info, None, "msg4"));

        let entries = buffer.get_entries(&LogFilter::new());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].message, "msg2"); // msg1 was evicted
    }

    #[test]
    fn test_filter_by_job_name() {
        let buffer = LogBuffer::new(100);

        buffer.push(make_entry(LogLevel::Info, Some("job1"), "job1 msg"));
        buffer.push(make_entry(LogLevel::Info, Some("job2"), "job2 msg"));
        buffer.push(make_entry(LogLevel::Info, None, "no job msg"));

        let filter = LogFilter::new().with_job_name("job1");
        let entries = buffer.get_entries(&filter);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].job_name, Some("job1".to_string()));
    }

    #[test]
    fn test_filter_by_level() {
        let buffer = LogBuffer::new(100);

        buffer.push(make_entry(LogLevel::Debug, None, "debug"));
        buffer.push(make_entry(LogLevel::Info, None, "info"));
        buffer.push(make_entry(LogLevel::Warn, None, "warn"));
        buffer.push(make_entry(LogLevel::Error, None, "error"));

        let filter = LogFilter::new().with_min_level(LogLevel::Warn);
        let entries = buffer.get_entries(&filter);

        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .all(|e| e.level.meets_minimum(LogLevel::Warn))
        );
    }

    #[test]
    fn test_get_recent_with_limit() {
        let buffer = LogBuffer::new(100);

        for i in 0..10 {
            buffer.push(make_entry(LogLevel::Info, None, &format!("msg{}", i)));
        }

        let entries = buffer.get_recent(3, &LogFilter::new());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].message, "msg7");
        assert_eq!(entries[1].message, "msg8");
        assert_eq!(entries[2].message, "msg9");
    }

    #[test]
    fn test_level_severity() {
        assert!(LogLevel::Error.meets_minimum(LogLevel::Trace));
        assert!(LogLevel::Error.meets_minimum(LogLevel::Error));
        assert!(!LogLevel::Info.meets_minimum(LogLevel::Warn));
        assert!(LogLevel::Info.meets_minimum(LogLevel::Debug));
    }
}
