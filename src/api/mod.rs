use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post};

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;

use crate::config::system::LogsConfig;
use crate::job_manager::JobManager;
use crate::logs::LogBuffer;
use crate::testing::DynTestSuiteStore;

pub(crate) mod error;
pub(crate) mod handler;
pub(crate) mod logs;
pub(crate) mod types;

use handler::{
    create_service, create_start_job, delete_schema, delete_test_suite, fill_schema,
    get_one_service, get_resource_schema, get_schema, get_test_suite, list_checkpoints, list_jobs,
    list_schemas, list_service_resources, list_services, list_test_suites, remove_service,
    restart_job, save_schema, save_test_suite, schema_convert, stop_job, transform_run,
    transform_test_generate, transform_test_run,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) job_manager: Arc<Mutex<JobManager>>,
    pub(crate) log_buffer: LogBuffer,
    pub(crate) logs_config: LogsConfig,
    pub(crate) test_suite_store: DynTestSuiteStore,
}

impl AppState {
    pub fn new(
        jb: Arc<Mutex<JobManager>>,
        log_buffer: LogBuffer,
        logs_config: LogsConfig,
        test_suite_store: DynTestSuiteStore,
    ) -> Self {
        Self {
            job_manager: jb,
            log_buffer,
            logs_config,
            test_suite_store,
        }
    }
}

pub async fn start_server(state: AppState, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .merge(crate::ui::ui_routes())
        .route("/jobs", get(list_jobs))
        .route("/jobs", post(create_start_job))
        .route("/jobs/{name}/stop", post(stop_job))
        .route("/jobs/{name}/restart", post(restart_job))
        .route("/services", get(list_services))
        .route("/services/{name}", get(get_one_service))
        .route("/services/{name}/resources", get(list_service_resources))
        .route(
            "/services/{name}/schema/introspect",
            get(get_resource_schema),
        )
        .route("/schema/fill", post(fill_schema))
        .route("/schema/convert", post(schema_convert))
        .route("/services", post(create_service))
        .route("/services/{name}", delete(remove_service))
        .route("/jobs/{name}/checkpoints", get(list_checkpoints))
        .route("/transform/run", post(transform_run))
        .route("/transform/test/generate", post(transform_test_generate))
        .route("/transform/test/run", post(transform_test_run))
        .route("/test-suites", get(list_test_suites))
        .route("/test-suites", post(save_test_suite))
        .route("/test-suites/{id}", get(get_test_suite))
        .route("/test-suites/{id}", delete(delete_test_suite))
        .route("/services/{name}/schemas", get(list_schemas))
        .route("/services/{name}/schemas", post(save_schema))
        .route("/services/{name}/schemas/{id}", get(get_schema))
        .route("/services/{name}/schemas/{id}", delete(delete_schema))
        .route("/logs", get(logs::get_logs))
        .route("/logs/stream", get(logs::stream_logs))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("web server listening on: {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
