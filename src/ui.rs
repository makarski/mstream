use crate::api::AppState;
use crate::config::service_config::UdfConfig;
use crate::config::{BatchConfig, Connector, Service};
use crate::job_manager::{JobMetadata, JobState as JState, ServiceStatus};
use axum::{
    Router,
    extract::{Form, Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{delete, get, post},
};
use serde::Deserialize;

pub fn ui_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(get_root))
        .route("/ui/jobs", get(get_jobs_table))
        .route("/ui/jobs", post(create_job_ui))
        .route("/ui/jobs/{name}", get(get_job_details))
        .route("/ui/jobs/{name}/stop", post(stop_job_ui))
        .route("/ui/jobs/{name}/restart", post(restart_job_ui))
        .route("/ui/services", get(get_services_table))
        .route("/ui/services", post(create_service_ui))
        .route("/ui/services/{name}", get(get_service_details))
        .route("/ui/services/{name}", delete(remove_service_ui))
}

fn layout(title: &str, content: &str) -> Html<String> {
    Html(format!(
        r##"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{} - MStream</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
    <script src="https://unpkg.com/htmx.org@2.0.0"></script>
    <style>
        .pipeline-viz {{ display: flex; align-items: center; gap: 1rem; overflow-x: auto; padding: 1rem 0; }}
        .stage {{ min-width: 200px; text-align: center; }}
        .stage .box {{ height: 100%; display: flex; flex-direction: column; justify-content: center; }}
        .arrow {{ font-size: 1.5rem; color: #b5b5b5; }}
        .resource-name {{ font-family: monospace; font-size: 0.85rem; word-break: break-all; color: #666; }}
        pre.config {{ background-color: #f5f5f5; padding: 1.25rem; border-radius: 4px; }}
        .navbar {{ border-bottom: 1px solid #f5f5f5; margin-bottom: 2rem; }}

        /* Custom summary marker removal and styling */
        details > summary {{ list-style: none; }}
        details > summary::-webkit-details-marker {{ display: none; }}

        /* HTMX Loading State for Bulma Buttons */
        .button.htmx-request {{
            color: transparent !important;
            pointer-events: none;
            position: relative;
        }}
        .button.htmx-request::after {{
            animation: spinAround 500ms infinite linear;
            border: 2px solid #dbdbdb;
            border-radius: 9999px;
            border-right-color: transparent;
            border-top-color: transparent;
            content: "";
            display: block;
            height: 1em;
            position: absolute;
            width: 1em;
            left: calc(50% - 0.5em);
            top: calc(50% - 0.5em);
        }}
        @keyframes spinAround {{
            from {{ transform: rotate(0deg); }}
            to {{ transform: rotate(359deg); }}
        }}
    </style>
</head>
<body>
    <nav class="navbar" role="navigation" aria-label="main navigation">
        <div class="container">
            <div class="navbar-brand">
                <a class="navbar-item has-text-weight-bold is-size-4" href="/">
                    MStream
                </a>
            </div>
        </div>
    </nav>

    <div class="container">
        {}
    </div>
</body>
</html>
"##,
        title, content
    ))
}

async fn get_root() -> Html<String> {
    let content = r##"
    <div class="columns">
        <div class="column">
            <h2 class="title is-4">Jobs</h2>
            <div class="box" style="padding: 0; overflow: hidden;">
                <details>
                    <summary class="button is-primary is-light is-fullwidth is-radiusless" style="border: none; height: 3.5em;">
                        <span class="has-text-weight-bold">+ Create New Job</span>
                    </summary>
                    <div class="p-5">
                        <form hx-post="/ui/jobs" hx-target="#jobs-container" hx-swap="innerHTML" hx-disabled-elt="button[type='submit']">
                            <div class="field">
                                <label class="label">Configuration (JSON)</label>
                                <div class="control">
                                    <textarea class="textarea is-family-monospace" name="config_json" placeholder='{{ "name": "my-job", "source": { ... } }}' rows="6"></textarea>
                                </div>
                            </div>
                            <div class="control">
                                <button class="button is-primary" type="submit">Start Job</button>
                            </div>
                        </form>
                    </div>
                </details>
            </div>
            <div id="jobs-container" hx-get="/ui/jobs" hx-trigger="load">
                <progress class="progress is-small is-primary" max="100">15%</progress>
            </div>
        </div>

        <div class="column">
            <h2 class="title is-4">Services</h2>
            <div class="box" style="padding: 0; overflow: hidden;">
                <details>
                    <summary class="button is-info is-light is-fullwidth is-radiusless" style="border: none; height: 3.5em;">
                        <span class="has-text-weight-bold">+ Create New Service</span>
                    </summary>
                    <div class="p-5">
                        <form hx-post="/ui/services" hx-target="#services-container" hx-swap="innerHTML" hx-disabled-elt="button[type='submit']">
                            <div class="field">
                                <label class="label">Configuration (JSON)</label>
                                <div class="control">
                                    <textarea class="textarea is-family-monospace" name="config_json" placeholder='{{ "provider": "kafka", "name": "my-kafka", ... }}' rows="6"></textarea>
                                </div>
                            </div>
                            <div class="control">
                                <button class="button is-info" type="submit">Create Service</button>
                            </div>
                        </form>
                    </div>
                </details>
            </div>
            <div id="services-container" hx-get="/ui/services" hx-trigger="load, services-update from:body">
                <progress class="progress is-small is-info" max="100">15%</progress>
            </div>
        </div>
    </div>
    "##;

    layout("Home", content)
}

// --- JOBS ---

async fn get_jobs_table(State(state): State<AppState>) -> Html<String> {
    let jm = state.job_manager.lock().await;
    match jm.list_jobs().await {
        Ok(jobs) => render_jobs_table(&jobs, None),
        Err(e) => Html(format!(
            "<div class='notification is-danger'>Error loading jobs: {}</div>",
            e
        )),
    }
}

async fn get_job_details(State(state): State<AppState>, Path(name): Path<String>) -> Html<String> {
    let jm = state.job_manager.lock().await;
    let job = match jm.get_job(&name).await {
        Ok(Some(j)) => j,
        Ok(None) => {
            return layout(
                "Not Found",
                "<div class='notification is-warning'>Job not found</div>",
            );
        }
        Err(e) => {
            return layout(
                "Error",
                &format!("<div class='notification is-danger'>Error: {}</div>", e),
            );
        }
    };

    let status_tag = match job.state {
        JState::Running => "is-success",
        JState::Stopped => "is-light",
        JState::Failed => "is-danger",
    };

    let duration = if job.stopped_at.is_none() {
        let d = chrono::Utc::now().signed_duration_since(job.started_at);
        format_duration(d)
    } else {
        "Stopped".to_string()
    };

    let batch_info = job
        .pipeline
        .as_ref()
        .and_then(|p| p.batch.as_ref())
        .map(|b| match b {
            BatchConfig::Count { size } => format!("Count: {}", size),
        })
        .unwrap_or_else(|| "None".to_string());

    let pipeline_json = job
        .pipeline
        .as_ref()
        .map(|p| {
            serde_json::to_string_pretty(p).unwrap_or_else(|_| "Error serializing config".into())
        })
        .unwrap_or_else(|| "No pipeline configuration".into());

    let pipeline_viz = job
        .pipeline
        .as_ref()
        .map(|p| render_pipeline_viz(p))
        .unwrap_or_default();

    let content = format!(
        r##"
        <nav class="breadcrumb" aria-label="breadcrumbs">
          <ul>
            <li><a href="/">Dashboard</a></li>
            <li class="is-active"><a href="#" aria-current="page">Job: {}</a></li>
          </ul>
        </nav>

        <div class="level">
            <div class="level-left">
                <div class="level-item">
                    <h1 class="title is-2">{}</h1>
                </div>
                <div class="level-item">
                    <span class="tag is-medium {}">{}</span>
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    <div class="buttons">
                        <button class="button is-warning is-light" hx-post="/ui/jobs/{}/stop" hx-swap="none" hx-disabled-elt="this" hx-headers='{{"ui-context": "details"}}'>Stop</button>
                        <button class="button is-info is-light" hx-post="/ui/jobs/{}/restart" hx-swap="none" hx-disabled-elt="this" hx-headers='{{"ui-context": "details"}}'>Restart</button>
                    </div>
                </div>
            </div>
        </div>

        <div class="box">
            <div class="columns">
                <div class="column">
                    <p class="heading">Started At</p>
                    <p class="title is-5">{}</p>
                </div>
                <div class="column">
                    <p class="heading">Running Time</p>
                    <p class="title is-5">{}</p>
                </div>
                <div class="column">
                    <p class="heading">Batching</p>
                    <p class="title is-5">{}</p>
                </div>
                <div class="column">
                    <p class="heading">Dependencies</p>
                    <p class="title is-5">{}</p>
                </div>
            </div>
        </div>

        <h2 class="title is-4 mt-6">Pipeline Flow</h2>
        <div class="box">
            {}
        </div>

        <h2 class="title is-4 mt-6">Configuration</h2>
        <pre class="config">{}</pre>
        "##,
        job.name,
        job.name,
        status_tag,
        job.state,
        job.name,
        job.name, // buttons
        job.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
        duration,
        batch_info,
        if job.service_deps.is_empty() {
            "None".to_string()
        } else {
            job.service_deps.join(", ")
        },
        pipeline_viz,
        pipeline_json
    );

    layout(&format!("Job {}", job.name), &content)
}

fn render_pipeline_viz(connector: &Connector) -> String {
    let mut html = String::from(r#"<div class="pipeline-viz">"#);
    let schemas = connector.schemas.as_deref().unwrap_or(&[]);

    let get_schemas_html = |svc_name: &str, resource: &str| -> String {
        let matches: Vec<_> = schemas
            .iter()
            .filter(|s| s.service_name == svc_name && s.resource == resource)
            .map(|s| s.id.as_str())
            .collect();

        if matches.is_empty() {
            String::new()
        } else {
            let tags = matches
                .iter()
                .map(|id| {
                    format!(
                        "<span class='tag is-info is-light is-small mt-1'>Schema: {}</span>",
                        id
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            format!("<div class='mt-2'>{}</div>", tags)
        }
    };

    // Source
    html.push_str(&format!(
        r#"
        <div class="stage">
            <div class="box has-background-success-light" style="border-top: 4px solid #48c774;">
                <strong class="has-text-success-dark">Source</strong>
                <div class="is-size-6 has-text-weight-medium"><a href="/ui/services/{}">{}</a></div>
                <div class="resource-name mt-1">{}</div>
                {}
            </div>
        </div>
        <div class="arrow">&rarr;</div>
        "#,
        connector.source.service_name,
        connector.source.service_name,
        connector.source.resource,
        get_schemas_html(&connector.source.service_name, &connector.source.resource)
    ));

    // Middlewares
    if let Some(middlewares) = &connector.middlewares {
        for mw in middlewares {
            html.push_str(&format!(
                r#"
                <div class="stage">
                    <div class="box has-background-warning-light" style="border-top: 4px solid #ffdd57;">
                        <strong class="has-text-warning-dark">Transform</strong>
                        <div class="is-size-6 has-text-weight-medium"><a href="/ui/services/{}">{}</a></div>
                        <div class="resource-name mt-1">{}</div>
                        {}
                    </div>
                </div>
                <div class="arrow">&rarr;</div>
                "#,
                mw.service_name,
                mw.service_name,
                mw.resource,
                get_schemas_html(&mw.service_name, &mw.resource)
            ));
        }
    }

    // Sinks
    html.push_str(r#"<div style="display: flex; flex-direction: column; gap: 1rem;">"#);
    for sink in &connector.sinks {
        html.push_str(&format!(
            r#"
            <div class="stage">
                <div class="box has-background-info-light" style="border-top: 4px solid #3298dc;">
                    <strong class="has-text-info-dark">Sink</strong>
                    <div class="is-size-6 has-text-weight-medium"><a href="/ui/services/{}">{}</a></div>
                    <div class="resource-name mt-1">{}</div>
                    {}
                </div>
            </div>
            "#,
            sink.service_name,
            sink.service_name,
            sink.resource,
            get_schemas_html(&sink.service_name, &sink.resource)
        ));
    }
    html.push_str("</div>");

    html.push_str("</div>");
    html
}

fn format_duration(d: chrono::Duration) -> String {
    let seconds = d.num_seconds();
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else {
        format!("{}m {}s", minutes, seconds)
    }
}

#[derive(Deserialize)]
struct CreateConfigForm {
    config_json: String,
}

async fn create_job_ui(
    State(state): State<AppState>,
    Form(form): Form<CreateConfigForm>,
) -> impl IntoResponse {
    let connector: Connector = match serde_json::from_str(&form.config_json) {
        Ok(c) => c,
        Err(e) => {
            let jm = state.job_manager.lock().await;
            let jobs = jm.list_jobs().await.unwrap_or_default();
            return (
                [("HX-Trigger", "services-update")],
                render_jobs_table(&jobs, Some(&format!("Invalid JSON: {}", e))),
            );
        }
    };

    let mut jm = state.job_manager.lock().await;
    let error = match jm.start_job(connector).await {
        Ok(_) => None,
        Err(e) => Some(format!("Failed to start job: {}", e)),
    };

    let jobs = jm.list_jobs().await.unwrap_or_default();
    (
        [("HX-Trigger", "services-update")],
        render_jobs_table(&jobs, error.as_deref()),
    )
}

fn render_jobs_table(jobs: &[JobMetadata], error: Option<&str>) -> Html<String> {
    let mut rows = String::new();
    for job in jobs {
        rows.push_str(&render_job_row(job));
    }

    let error_html = if let Some(err) = error {
        format!("<div class='notification is-danger is-light'>{}</div>", err)
    } else {
        String::new()
    };

    Html(format!(
        r#"
        {}
        <div class="table-container">
            <table class="table is-fullwidth is-striped is-hoverable">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>State</th>
                        <th>Started At</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {}
                </tbody>
            </table>
        </div>
        "#,
        error_html, rows
    ))
}

fn render_job_row(job: &JobMetadata) -> String {
    let status_tag = match job.state {
        JState::Running => "is-success",
        JState::Stopped => "is-light",
        JState::Failed => "is-danger",
    };

    let actions = format!(
        r#"
        <div class="buttons are-small">
            <button class="button is-warning is-light" hx-post="/ui/jobs/{}/stop" hx-target="closest tr" hx-swap="outerHTML" hx-disabled-elt="this">Stop</button>
            <button class="button is-info is-light" hx-post="/ui/jobs/{}/restart" hx-target="closest tr" hx-swap="outerHTML" hx-disabled-elt="this">Restart</button>
        </div>
        "#,
        job.name, job.name
    );

    format!(
        r#"
        <tr>
            <td><a href="/ui/jobs/{}" class="has-text-weight-medium">{}</a></td>
            <td><span class="tag {}">{}</span></td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
        job.name,
        job.name,
        status_tag,
        job.state,
        job.started_at.format("%Y-%m-%d %H:%M:%S"),
        actions
    )
}

async fn stop_job_ui(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    {
        let mut jm = state.job_manager.lock().await;
        let _ = jm.stop_job(&name).await;
    }

    // Yield to allow main loop to process exit and update state
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // If called from details page, redirect to reload
    if let Some(ctx) = headers.get("ui-context") {
        if ctx == "details" {
            return (
                StatusCode::OK,
                [("HX-Redirect", format!("/ui/jobs/{}", name))],
                Html(String::new()),
            )
                .into_response();
        }
    }

    let jm = state.job_manager.lock().await;
    if let Ok(Some(job)) = jm.get_job(&name).await {
        return (
            [("HX-Trigger", "services-update")],
            Html(render_job_row(&job)),
        )
            .into_response();
    }

    Html(format!(
        "<tr><td colspan='4' class='has-text-danger'>Error stopping job {}</td></tr>",
        name
    ))
    .into_response()
}

async fn restart_job_ui(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut jm = state.job_manager.lock().await;
    let res = jm.restart_job(&name).await;

    // If called from details page, redirect to reload
    if let Some(ctx) = headers.get("ui-context") {
        if ctx == "details" {
            return (
                StatusCode::OK,
                [("HX-Redirect", format!("/ui/jobs/{}", name))],
                Html(String::new()),
            )
                .into_response();
        }
    }

    match res {
        Ok(job) => (
            [("HX-Trigger", "services-update")],
            Html(render_job_row(&job)),
        )
            .into_response(),
        Err(_) => Html(format!(
            "<tr><td colspan='4' class='has-text-danger'>Error restarting job {}</td></tr>",
            name
        ))
        .into_response(),
    }
}

// --- SERVICES ---

async fn get_services_table(State(state): State<AppState>) -> Html<String> {
    let jm = state.job_manager.lock().await;
    match jm.list_services().await {
        Ok(services) => render_services_table(&services, None),
        Err(e) => Html(format!(
            "<div class='notification is-danger'>Error loading services: {}</div>",
            e
        )),
    }
}

async fn get_service_details(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Html<String> {
    let jm = state.job_manager.lock().await;
    let status = match jm.get_service(&name).await {
        Ok(s) => s,
        Err(e) => {
            return layout(
                "Error",
                &format!("<div class='notification is-danger'>Error: {}</div>", e),
            );
        }
    };

    let provider = match status.service {
        Service::PubSub(_) => "PubSub",
        Service::Kafka(_) => "Kafka",
        Service::MongoDb(_) => "MongoDB",
        Service::Http(_) => "HTTP",
        Service::Udf(_) => "UDF",
    };

    let used_by_links = if status.used_by_jobs.is_empty() {
        "<span class='has-text-grey'>None</span>".to_string()
    } else {
        status
            .used_by_jobs
            .iter()
            .map(|job_name| format!(r#"<a href="/ui/jobs/{}">{}</a>"#, job_name, job_name))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let config_json = serde_json::to_string_pretty(&status.service)
        .unwrap_or_else(|_| "Error serializing config".into());

    let extra_content = match &status.service {
        Service::Udf(cfg) => load_udf_scripts(&cfg).await,
        _ => String::new(),
    };

    let content = format!(
        r##"
        <nav class="breadcrumb" aria-label="breadcrumbs">
          <ul>
            <li><a href="/">Dashboard</a></li>
            <li class="is-active"><a href="#" aria-current="page">Service: {}</a></li>
          </ul>
        </nav>

        <div class="level">
            <div class="level-left">
                <div class="level-item">
                    <h1 class="title is-2">{}</h1>
                </div>
            </div>
            <div class="level-right">
                <div class="level-item">
                    {}
                </div>
            </div>
        </div>

        <div class="box">
            <div class="columns">
                <div class="column">
                    <p class="heading">Provider</p>
                    <p class="title is-5">{}</p>
                </div>
                <div class="column">
                    <p class="heading">Used By Jobs</p>
                    <p class="title is-5">{}</p>
                </div>
            </div>
        </div>

        <h2 class="title is-4 mt-6">Configuration</h2>
        <pre class="config">{}</pre>

        {}
        "##,
        status.service.name(),
        status.service.name(),
        if status.used_by_jobs.is_empty() {
            format!(
                r##"<button class="button is-danger is-light" hx-delete="/ui/services/{}" hx-confirm="Are you sure? This will delete the service." hx-target="body" hx-disabled-elt="this">Delete Service</button>"##,
                status.service.name()
            )
        } else {
            r#"<button class="button is-danger is-light" disabled title="Cannot delete service while in use">Delete Service</button>"#.to_string()
        },
        provider,
        used_by_links,
        config_json,
        extra_content
    );

    layout(&format!("Service {}", status.service.name()), &content)
}

async fn load_udf_scripts(config: &UdfConfig) -> String {
    let mut html = String::from("<h2 class='title is-4 mt-6'>Scripts</h2>");

    if let Some(sources) = &config.sources {
        for script in sources {
            html.push_str(&render_script(&script.filename, &script.content));
        }
        return html;
    }

    let path = std::path::Path::new(&config.script_path);
    if path.is_dir() {
        if let Ok(mut entries) = tokio::fs::read_dir(path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(content) = tokio::fs::read_to_string(&path).await {
                        let filename = path.file_name().unwrap_or_default().to_string_lossy();
                        html.push_str(&render_script(&filename, &content));
                    }
                }
            }
        }
    } else if path.is_file() {
        if let Ok(content) = tokio::fs::read_to_string(&path).await {
            let filename = path.file_name().unwrap_or_default().to_string_lossy();
            html.push_str(&render_script(&filename, &content));
        }
    }

    html
}

fn render_script(filename: &str, content: &str) -> String {
    let escaped = content
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;");

    format!(
        r##"
        <div class="message is-info is-small mb-4">
            <div class="message-header">
                <p>{}</p>
            </div>
            <div class="message-body" style="padding: 0;">
                <pre style="background: transparent; border: none; border-radius: 0;"><code>{}</code></pre>
            </div>
        </div>
        "##,
        filename, escaped
    )
}

async fn create_service_ui(
    State(state): State<AppState>,
    Form(form): Form<CreateConfigForm>,
) -> Html<String> {
    let service: Service = match serde_json::from_str(&form.config_json) {
        Ok(s) => s,
        Err(e) => {
            let jm = state.job_manager.lock().await;
            let services = jm.list_services().await.unwrap_or_default();
            return render_services_table(&services, Some(&format!("Invalid JSON: {}", e)));
        }
    };

    let jm = state.job_manager.lock().await;
    let error = match jm.create_service(service).await {
        Ok(_) => None,
        Err(e) => Some(format!("Failed to create service: {}", e)),
    };

    let services = jm.list_services().await.unwrap_or_default();
    render_services_table(&services, error.as_deref())
}

async fn remove_service_ui(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Html<String> {
    let jm = state.job_manager.lock().await;
    let error = match jm.remove_service(&name).await {
        Ok(_) => None,
        Err(e) => Some(format!("Failed to remove service: {}", e)),
    };

    let services = jm.list_services().await.unwrap_or_default();
    render_services_table(&services, error.as_deref())
}

fn render_services_table(services: &[ServiceStatus], error: Option<&str>) -> Html<String> {
    let mut rows = String::new();
    for svc in services {
        rows.push_str(&render_service_row(svc));
    }

    let error_html = if let Some(err) = error {
        format!("<div class='notification is-danger is-light'>{}</div>", err)
    } else {
        String::new()
    };

    Html(format!(
        r#"
        {}
        <div class="table-container">
            <table class="table is-fullwidth is-striped is-hoverable">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Provider</th>
                        <th>Used By</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {}
                </tbody>
            </table>
        </div>
        "#,
        error_html, rows
    ))
}

fn render_service_row(status: &ServiceStatus) -> String {
    let name = status.service.name();
    let provider = match status.service {
        Service::PubSub(_) => "PubSub",
        Service::Kafka(_) => "Kafka",
        Service::MongoDb(_) => "MongoDB",
        Service::Http(_) => "HTTP",
        Service::Udf(_) => "UDF",
    };

    let used_by = if status.used_by_jobs.is_empty() {
        "<span class='has-text-grey'>-</span>".to_string()
    } else {
        status.used_by_jobs.join(", ")
    };

    let delete_btn = if status.used_by_jobs.is_empty() {
        format!(
            r##"<button class="button is-small is-danger is-light" hx-delete="/ui/services/{}" hx-target="#services-container" hx-confirm="Are you sure?">Delete</button>"##,
            name
        )
    } else {
        r#"<button class="button is-small is-danger is-light" disabled>Delete</button>"#.to_string()
    };

    format!(
        r#"
        <tr>
            <td><a href="/ui/services/{}" class="has-text-weight-medium">{}</a></td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
        name, name, provider, used_by, delete_btn
    )
}
