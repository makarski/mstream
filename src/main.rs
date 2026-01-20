use tracing::info;
use tracing_subscriber::prelude::*;

use mstream::logs::{LogBuffer, LogBufferLayer};

const CONFIG_FILE: &str = "mstream-config.toml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "pprof")]
    let profiler_guard = profiler::start();

    // Parse command-line arguments for config file path (need this early for config)
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .iter()
        .position(|arg| arg == "--config")
        .and_then(|pos| args.get(pos + 1).cloned())
        .unwrap_or_else(|| CONFIG_FILE.to_string());

    // Load config early to get logs settings
    let config = mstream::config::Config::load(&config_path)?;
    let logs_config = config
        .system
        .as_ref()
        .and_then(|s| s.logs.clone())
        .unwrap_or_default();

    // Create log buffer with configured capacity
    let log_buffer = LogBuffer::new(logs_config.buffer_capacity);

    // Set up layered tracing subscriber
    let (non_blocking_logger, _log_guard) = tracing_appender::non_blocking(std::io::stdout());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_logger)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    let buffer_layer = LogBufferLayer::new(log_buffer.clone())
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(buffer_layer)
        .init();

    info!("starting mstream...");

    let app = mstream::run_app_with_log_buffer(config, log_buffer, logs_config);
    app.await?;

    #[cfg(feature = "pprof")]
    profiler::flush(profiler_guard);

    Ok(())
}

#[cfg(feature = "pprof")]
mod profiler {
    use pprof::{ProfilerGuard, ProfilerGuardBuilder, protos::Message};
    use std::fs::File;

    pub fn start() -> Option<ProfilerGuard<'static>> {
        ProfilerGuardBuilder::default().frequency(100).build().ok()
    }

    pub fn flush(guard: Option<ProfilerGuard<'static>>) {
        let Some(guard) = guard else { return };
        let Ok(report) = guard.report().build() else {
            return;
        };

        if let Ok(mut file) = File::create("profile.svg") {
            let _ = report.flamegraph(&mut file);
        }

        if let Ok(profile) = report.pprof() {
            if let Ok(mut file) = File::create("profile.pb") {
                let _ = profile.write_to_writer(&mut file);
            }
        }
    }
}
