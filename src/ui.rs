use crate::api::AppState;
use crate::config::service_config::UdfConfig;
use crate::config::{BatchConfig, Connector, Masked, Service};
use crate::job_manager::{JobMetadata, JobState as JState, ServiceStatus};
use crate::kafka::KafkaOffset;
use axum::{
    Router,
    extract::{Form, Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{delete, get, post},
};
use serde::Deserialize;

/// Escapes HTML special characters to prevent XSS attacks.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

const LAYOUT_CSS: &str = r#"
    .pipeline-viz { display: flex; align-items: center; gap: 1rem; overflow-x: auto; padding: 1rem 0; }
    .stage { min-width: 200px; text-align: center; }
    .stage .box { height: 100%; display: flex; flex-direction: column; justify-content: center; }
    .arrow { font-size: 1.5rem; color: #b5b5b5; }
    .resource-name { font-family: monospace; font-size: 0.85rem; word-break: break-all; color: #666; }
    .navbar { border-bottom: 1px solid #f5f5f5; margin-bottom: 2rem; }
    pre.config { padding: 1.25rem; background-color: #f6f8fa; border-radius: 6px; }
    .string { color: #0a3069; }
    .number { color: #0550ae; }
    .boolean { color: #0550ae; }
    .null { color: #0550ae; }
    .key { color: #cf222e; }
    .code-container { position: relative; }
    .copy-btn {
        position: absolute;
        top: 0.5rem;
        right: 0.5rem;
        opacity: 0;
        transition: opacity 0.2s;
    }
    .code-container:hover .copy-btn { opacity: 1; }
    details > summary { list-style: none; }
    details > summary::-webkit-details-marker { display: none; }
    .button.htmx-request {
        color: transparent !important;
        pointer-events: none;
        position: relative;
    }
    .button.htmx-request::after {
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
    }
    @keyframes spinAround {
        from { transform: rotate(0deg); }
        to { transform: rotate(359deg); }
    }
"#;

const LAYOUT_JS: &str = r#"
    function highlightAll() {
        document.querySelectorAll('pre.json-content').forEach((block) => {
            if (block.dataset.highlighted) return;
            const content = block.textContent;
            block.innerHTML = syntaxHighlight(content);
            block.dataset.highlighted = "true";
        });
    }

    function syntaxHighlight(json) {
        json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            var cls = 'number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'key';
                } else {
                    cls = 'string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'boolean';
            } else if (/null/.test(match)) {
                cls = 'null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
        });
    }

    function copyToClipboard(btn) {
        const container = btn.closest('.code-container');
        const pre = container.querySelector('pre');
        const code = pre.textContent;
        navigator.clipboard.writeText(code).then(() => {
            const originalText = btn.innerText;
            btn.innerText = 'Copied!';
            btn.classList.add('is-success');
            setTimeout(() => {
                btn.innerText = originalText;
                btn.classList.remove('is-success');
            }, 2000);
        });
    }

    highlightAll();
    document.body.addEventListener('htmx:afterSwap', highlightAll);
"#;

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
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - MStream</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
    <script src="https://unpkg.com/htmx.org@2.0.0"></script>
    <style>{css}</style>
</head>
<body>
    <nav class="navbar" role="navigation" aria-label="main navigation">
        <div class="container">
            <div class="navbar-brand">
                <a class="navbar-item has-text-weight-bold is-size-4" href="/">MStream</a>
            </div>
        </div>
    </nav>
    <div class="container">{content}</div>
    <script>{js}</script>
</body>
</html>"##,
        title = title,
        css = LAYOUT_CSS,
        content = content,
        js = LAYOUT_JS
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

    let checkpoint_cfg = job.pipeline.as_ref().and_then(|p| p.checkpoint.clone());
    let latest_checkpoint = jm.load_checkpoint(&name, &checkpoint_cfg).await;

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

    let checkpoint_enabled = checkpoint_cfg.as_ref().map_or(false, |c| c.enabled);

    let checkpoint_info = if checkpoint_enabled {
        match &latest_checkpoint {
            Some(cp) => {
                let ts = chrono::DateTime::from_timestamp_millis(cp.updated_at)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());

                // Try to decode Kafka offset for display
                let cursor_info = mongodb::bson::from_slice::<KafkaOffset>(&cp.cursor)
                    .ok()
                    .map(|k| format!("offset: {}", k.offset))
                    .unwrap_or_default();

                let cursor_html = if cursor_info.is_empty() {
                    String::new()
                } else {
                    format!(r#"<p class="is-size-7 has-text-grey">{}</p>"#, cursor_info)
                };

                format!(
                    r#"<span class="tag is-link">Enabled</span>
                    <p class="is-size-7 mt-1">Last: {}</p>
                    {}"#,
                    ts, cursor_html
                )
            }
            None => r#"<span class="tag is-link">Enabled</span>
                <p class="is-size-7 mt-1 has-text-grey">No checkpoint yet</p>"#
                .to_string(),
        }
    } else {
        r#"<span class="tag is-light">Disabled</span>"#.to_string()
    };

    let pipeline_json = job
        .pipeline
        .as_ref()
        .map(|p| {
            escape_html(
                &serde_json::to_string_pretty(p)
                    .unwrap_or_else(|_| "Error serializing config".into()),
            )
        })
        .unwrap_or_else(|| "No pipeline configuration".into());

    let pipeline_viz = job
        .pipeline
        .as_ref()
        .map(|p| render_pipeline_viz(p))
        .unwrap_or_default();

    let job_name = escape_html(&job.name);
    let service_deps = if job.service_deps.is_empty() {
        "None".to_string()
    } else {
        job.service_deps
            .iter()
            .map(|s| escape_html(s))
            .collect::<Vec<_>>()
            .join(", ")
    };

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
                    <p class="heading">Checkpoint</p>
                    <div>{}</div>
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
        <div class="code-container">
            <button class="button is-small is-white copy-btn" onclick="copyToClipboard(this)">Copy</button>
            <pre class="config json-content">{}</pre>
        </div>
        "##,
        job_name,
        job_name,
        status_tag,
        job.state,
        job_name,
        job_name, // buttons
        job.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
        duration,
        batch_info,
        checkpoint_info,
        service_deps,
        pipeline_viz,
        pipeline_json
    );

    layout(&format!("Job {}", job_name), &content)
}

/// Style configuration for a pipeline stage.
struct StageStyle {
    label: &'static str,
    text_class: &'static str,
    bg_class: &'static str,
    border_color: &'static str,
}

const SOURCE_STYLE: StageStyle = StageStyle {
    label: "Source",
    text_class: "has-text-success-dark",
    bg_class: "has-background-success-light",
    border_color: "#48c774",
};

const TRANSFORM_STYLE: StageStyle = StageStyle {
    label: "Transform",
    text_class: "has-text-warning-dark",
    bg_class: "has-background-warning-light",
    border_color: "#ffdd57",
};

const SINK_STYLE: StageStyle = StageStyle {
    label: "Sink",
    text_class: "has-text-info-dark",
    bg_class: "has-background-info-light",
    border_color: "#3298dc",
};

/// Renders a single pipeline stage (source, transform, or sink).
fn render_stage(
    style: &StageStyle,
    service_name: &str,
    resource: &str,
    schemas_html: &str,
) -> String {
    format!(
        r#"
        <div class="stage">
            <div class="box {}" style="border-top: 4px solid {};">
                <strong class="{}">{}</strong>
                <div class="is-size-6 has-text-weight-medium"><a href="/ui/services/{}">{}</a></div>
                <div class="resource-name mt-1">{}</div>
                {}
            </div>
        </div>
        "#,
        style.bg_class,
        style.border_color,
        style.text_class,
        style.label,
        service_name,
        service_name,
        resource,
        schemas_html
    )
}

fn render_pipeline_viz(connector: &Connector) -> String {
    let mut html = String::from(r#"<div class="pipeline-viz">"#);
    let schemas = connector.schemas.as_deref().unwrap_or(&[]);

    let get_schemas_html = |svc_name: &str, resource: &str| -> String {
        let matches: Vec<_> = schemas
            .iter()
            .filter(|s| s.service_name == svc_name && s.resource == resource)
            .map(|s| escape_html(&s.id))
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
    let source_svc = escape_html(&connector.source.service_name);
    let source_resource = escape_html(&connector.source.resource);
    html.push_str(&render_stage(
        &SOURCE_STYLE,
        &source_svc,
        &source_resource,
        &get_schemas_html(&connector.source.service_name, &connector.source.resource),
    ));
    html.push_str(r#"<div class="arrow">&rarr;</div>"#);

    // Middlewares
    if let Some(middlewares) = &connector.middlewares {
        for mw in middlewares {
            let mw_svc = escape_html(&mw.service_name);
            let mw_resource = escape_html(&mw.resource);
            html.push_str(&render_stage(
                &TRANSFORM_STYLE,
                &mw_svc,
                &mw_resource,
                &get_schemas_html(&mw.service_name, &mw.resource),
            ));
            html.push_str(r#"<div class="arrow">&rarr;</div>"#);
        }
    }

    // Sinks
    html.push_str(r#"<div style="display: flex; flex-direction: column; gap: 1rem;">"#);
    for sink in &connector.sinks {
        let sink_svc = escape_html(&sink.service_name);
        let sink_resource = escape_html(&sink.resource);
        html.push_str(&render_stage(
            &SINK_STYLE,
            &sink_svc,
            &sink_resource,
            &get_schemas_html(&sink.service_name, &sink.resource),
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

/// Renders a table with optional error message.
fn render_table(headers: &[&str], rows: &str, error: Option<&str>) -> Html<String> {
    let error_html = error
        .map(|err| {
            format!(
                "<div class='notification is-danger is-light'>{}</div>",
                escape_html(err)
            )
        })
        .unwrap_or_default();

    let header_cells: String = headers.iter().map(|h| format!("<th>{}</th>", h)).collect();

    Html(format!(
        r#"
        {}
        <div class="table-container">
            <table class="table is-fullwidth is-striped is-hoverable">
                <thead>
                    <tr>
                        {}
                    </tr>
                </thead>
                <tbody>
                    {}
                </tbody>
            </table>
        </div>
        "#,
        error_html, header_cells, rows
    ))
}

fn render_jobs_table(jobs: &[JobMetadata], error: Option<&str>) -> Html<String> {
    let rows: String = jobs.iter().map(render_job_row).collect();
    render_table(&["Name", "State", "Started At", "Actions"], &rows, error)
}

fn render_job_row(job: &JobMetadata) -> String {
    let status_tag = match job.state {
        JState::Running => "is-success",
        JState::Stopped => "is-light",
        JState::Failed => "is-danger",
    };

    let job_name = escape_html(&job.name);

    let actions = format!(
        r#"
        <div class="buttons are-small">
            <button class="button is-warning is-light" hx-post="/ui/jobs/{}/stop" hx-target="closest tr" hx-swap="outerHTML" hx-disabled-elt="this">Stop</button>
            <button class="button is-info is-light" hx-post="/ui/jobs/{}/restart" hx-target="closest tr" hx-swap="outerHTML" hx-disabled-elt="this">Restart</button>
        </div>
        "#,
        job_name, job_name
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
        job_name,
        job_name,
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
                [("HX-Redirect", format!("/ui/jobs/{}", escape_html(&name)))],
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
        escape_html(&name)
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
                [("HX-Redirect", format!("/ui/jobs/{}", escape_html(&name)))],
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
            escape_html(&name)
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
            .map(|job_name| {
                let escaped = escape_html(job_name);
                format!(r#"<a href="/ui/jobs/{}">{}</a>"#, escaped, escaped)
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let config_json = escape_html(
        &serde_json::to_string_pretty(&status.service.masked())
            .unwrap_or_else(|_| "Error serializing config".into()),
    );

    let extra_content = match &status.service {
        Service::Udf(cfg) => load_udf_scripts(&cfg).await,
        _ => String::new(),
    };

    let service_name = escape_html(status.service.name());

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
        <div class="code-container">
            <button class="button is-small is-white copy-btn" onclick="copyToClipboard(this)">Copy</button>
            <pre class="config json-content">{}</pre>
        </div>

        {}
        "##,
        service_name,
        service_name,
        if status.used_by_jobs.is_empty() {
            format!(
                r##"<button class="button is-danger is-light" hx-delete="/ui/services/{}" hx-confirm="Are you sure? This will delete the service." hx-target="body" hx-disabled-elt="this">Delete Service</button>"##,
                service_name
            )
        } else {
            r#"<button class="button is-danger is-light" disabled title="Cannot delete service while in use">Delete Service</button>"#.to_string()
        },
        provider,
        used_by_links,
        config_json,
        extra_content
    );

    layout(&format!("Service {}", service_name), &content)
}

/// Reads a single script file and returns (filename, content) or None on error.
async fn read_script_file(path: &std::path::Path) -> Option<(String, String)> {
    let content = tokio::fs::read_to_string(path).await.ok()?;
    let filename = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    Some((filename, content))
}

/// Reads script files from a path (file or directory) and returns (filename, content) pairs.
async fn read_scripts_from_path(path: &std::path::Path) -> Vec<(String, String)> {
    if path.is_file() {
        return read_script_file(path).await.into_iter().collect();
    }

    if !path.is_dir() {
        return Vec::new();
    }

    let Ok(mut entries) = tokio::fs::read_dir(path).await else {
        return Vec::new();
    };

    let mut scripts = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let entry_path = entry.path();
        if !entry_path.is_file() {
            continue;
        }
        if let Some(script) = read_script_file(&entry_path).await {
            scripts.push(script);
        }
    }
    scripts
}

async fn load_udf_scripts(config: &UdfConfig) -> String {
    let mut html = String::from("<h2 class='title is-4 mt-6'>Scripts</h2>");

    let scripts: Vec<(String, String)> = if let Some(sources) = &config.sources {
        sources
            .iter()
            .map(|s| (s.filename.clone(), s.content.clone()))
            .collect()
    } else {
        read_scripts_from_path(std::path::Path::new(&config.script_path)).await
    };

    for (filename, content) in scripts {
        html.push_str(&render_script(&filename, &content));
    }

    html
}

fn render_script(filename: &str, content: &str) -> String {
    let escaped_content = escape_html(content);
    let escaped_filename = escape_html(filename);

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
        escaped_filename, escaped_content
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
    let rows: String = services.iter().map(render_service_row).collect();
    render_table(&["Name", "Provider", "Used By", "Actions"], &rows, error)
}

fn render_service_row(status: &ServiceStatus) -> String {
    let name = escape_html(status.service.name());
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
        status
            .used_by_jobs
            .iter()
            .map(|j| escape_html(j))
            .collect::<Vec<_>>()
            .join(", ")
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

#[cfg(test)]
mod tests {
    use super::*;

    mod escape_html_tests {
        use super::*;

        #[test]
        fn escapes_ampersand() {
            assert_eq!(escape_html("foo & bar"), "foo &amp; bar");
        }

        #[test]
        fn escapes_less_than() {
            assert_eq!(escape_html("<script>"), "&lt;script&gt;");
        }

        #[test]
        fn escapes_greater_than() {
            assert_eq!(escape_html("a > b"), "a &gt; b");
        }

        #[test]
        fn escapes_double_quotes() {
            assert_eq!(escape_html(r#"say "hello""#), "say &quot;hello&quot;");
        }

        #[test]
        fn escapes_single_quotes() {
            assert_eq!(escape_html("it's"), "it&#x27;s");
        }

        #[test]
        fn escapes_xss_payload() {
            let payload = "<script>alert('XSS')</script>";
            let escaped = escape_html(payload);
            assert_eq!(
                escaped,
                "&lt;script&gt;alert(&#x27;XSS&#x27;)&lt;/script&gt;"
            );
            assert!(!escaped.contains('<'));
            assert!(!escaped.contains('>'));
        }

        #[test]
        fn leaves_safe_strings_unchanged() {
            assert_eq!(escape_html("hello world"), "hello world");
            assert_eq!(escape_html("my-job-123"), "my-job-123");
            assert_eq!(escape_html("kafka.topic.name"), "kafka.topic.name");
        }

        #[test]
        fn handles_empty_string() {
            assert_eq!(escape_html(""), "");
        }

        #[test]
        fn escapes_all_special_chars_together() {
            let input = r#"<a href="test?a=1&b=2">it's a 'link'</a>"#;
            let escaped = escape_html(input);
            assert!(!escaped.contains('<'));
            assert!(!escaped.contains('>'));
            assert!(!escaped.contains('"'));
            assert!(!escaped.contains('\''));
            // & should only appear as part of escape sequences
            for part in escaped.split('&') {
                if !part.is_empty() {
                    assert!(
                        part.starts_with("amp;")
                            || part.starts_with("lt;")
                            || part.starts_with("gt;")
                            || part.starts_with("quot;")
                            || part.starts_with("#x27;")
                            || escaped.starts_with(part), // first part before any &
                        "unexpected ampersand usage in: {}",
                        escaped
                    );
                }
            }
        }
    }

    mod format_duration_tests {
        use super::*;

        #[test]
        fn formats_seconds_only() {
            let d = chrono::Duration::seconds(45);
            assert_eq!(format_duration(d), "0m 45s");
        }

        #[test]
        fn formats_minutes_and_seconds() {
            let d = chrono::Duration::seconds(125); // 2m 5s
            assert_eq!(format_duration(d), "2m 5s");
        }

        #[test]
        fn formats_hours_minutes_seconds() {
            let d = chrono::Duration::seconds(3661); // 1h 1m 1s
            assert_eq!(format_duration(d), "1h 1m 1s");
        }

        #[test]
        fn formats_days_hours_minutes() {
            let d = chrono::Duration::seconds(90061); // 1d 1h 1m 1s
            assert_eq!(format_duration(d), "1d 1h 1m");
        }

        #[test]
        fn formats_zero_duration() {
            let d = chrono::Duration::seconds(0);
            assert_eq!(format_duration(d), "0m 0s");
        }

        #[test]
        fn formats_exactly_one_hour() {
            let d = chrono::Duration::seconds(3600);
            assert_eq!(format_duration(d), "1h 0m 0s");
        }

        #[test]
        fn formats_exactly_one_day() {
            let d = chrono::Duration::seconds(86400);
            assert_eq!(format_duration(d), "1d 0h 0m");
        }
    }
}
