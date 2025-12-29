use log::info;

const CONFIG_FILE: &str = "mstream-config.toml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "pprof")]
    let profiler_guard = profiler::start();

    let (non_blocking_logger, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(non_blocking_logger)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("starting mstream...");
    
    // Parse command-line arguments for config file path
    let config_path = std::env::args()
        .skip(1)
        .position(|arg| arg == "--config")
        .and_then(|pos| std::env::args().nth(pos + 2))
        .unwrap_or_else(|| CONFIG_FILE.to_string());
    
    let app = mstream::run_app(&config_path);
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
        if let Some(guard) = guard {
            if let Ok(report) = guard.report().build() {
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
    }
}
