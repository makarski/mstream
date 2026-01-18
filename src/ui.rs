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
    /* Base & Typography */
    :root {
        --primary: #6366f1;
        --primary-light: #818cf8;
        --primary-dark: #4f46e5;
        --success: #10b981;
        --success-light: #34d399;
        --warning: #f59e0b;
        --danger: #ef4444;
        --gray-50: #f9fafb;
        --gray-100: #f3f4f6;
        --gray-200: #e5e7eb;
        --gray-300: #d1d5db;
        --gray-400: #9ca3af;
        --gray-500: #6b7280;
        --gray-600: #4b5563;
        --gray-700: #374151;
        --gray-800: #1f2937;
        --gray-900: #111827;
        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
        --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1);
        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
        --radius: 12px;
        --radius-sm: 8px;
    }
    body {
        background: linear-gradient(135deg, var(--gray-50) 0%, #eef2ff 100%);
        min-height: 100vh;
        display: flex;
        flex-direction: column;
    }
    .section { flex: 1; }

    /* Modern Navbar */
    .navbar {
        background: rgba(255,255,255,0.8);
        backdrop-filter: blur(10px);
        border-bottom: 1px solid var(--gray-200);
        box-shadow: var(--shadow-sm);
    }
    .navbar-item {
        border-radius: var(--radius-sm);
        transition: all 0.2s ease;
    }
    .navbar-item:hover { background: var(--gray-100); }

    /* Cards */
    .box {
        border-radius: var(--radius);
        box-shadow: var(--shadow-md);
        border: 1px solid var(--gray-200);
        transition: all 0.2s ease;
    }
    .box:hover { box-shadow: var(--shadow-lg); }

    /* Pipeline visualization */
    .pipeline-viz { display: flex; align-items: center; gap: 1rem; overflow-x: auto; padding: 1rem 0; }
    .stage { min-width: 200px; text-align: center; }
    .stage .box {
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        background: linear-gradient(135deg, #fff 0%, var(--gray-50) 100%);
    }
    .arrow { font-size: 1.5rem; color: var(--gray-400); }
    .resource-name { font-family: 'SF Mono', Monaco, monospace; font-size: 0.8rem; word-break: break-all; color: var(--gray-500); }

    /* Code blocks */
    pre.config {
        padding: 1.25rem;
        background: var(--gray-800);
        border-radius: var(--radius-sm);
        color: #e5e7eb;
    }
    .string { color: #a5d6ff; }
    .number { color: #79c0ff; }
    .boolean { color: #ff7b72; }
    .null { color: #ff7b72; }
    .key { color: #7ee787; }
    .code-container { position: relative; }
    .copy-btn {
        position: absolute;
        top: 0.5rem;
        right: 0.5rem;
        opacity: 0;
        transition: all 0.2s ease;
        background: var(--gray-700);
        color: white;
        border: none;
        border-radius: var(--radius-sm);
    }
    .copy-btn:hover { background: var(--gray-600); }
    .code-container:hover .copy-btn { opacity: 1; }

    /* Modern Tables */
    .table {
        background: white;
        border-radius: var(--radius);
        overflow: hidden;
        box-shadow: var(--shadow);
    }
    .table thead { background: var(--gray-50); }
    .table th {
        color: var(--gray-600);
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
        border-bottom: 2px solid var(--gray-200) !important;
    }
    .table td {
        vertical-align: middle;
        border-color: var(--gray-100) !important;
        transition: background 0.15s ease;
    }
    .table tr:hover td { background: var(--gray-50); }

    /* Status indicators with pulse */
    .status-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
    }
    .status-dot.is-running {
        background: var(--success);
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        animation: pulse 2s infinite;
    }
    .status-dot.is-stopped { background: var(--gray-400); }
    .status-dot.is-failed { background: var(--danger); }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Modern Tags */
    .tag {
        font-weight: 500;
        border-radius: 6px;
        padding: 0.25em 0.75em;
    }
    .tag.is-success { background: #dcfce7; color: #166534; }
    .tag.is-light { background: var(--gray-100); color: var(--gray-600); }
    .tag.is-danger { background: #fee2e2; color: #991b1b; }
    .tag.is-link { background: #e0e7ff; color: #3730a3; }
    .tag.is-info { background: #dbeafe; color: #1e40af; }

    /* Buttons */
    .button {
        border-radius: var(--radius-sm);
        font-weight: 500;
        transition: all 0.2s ease;
        border: none;
        box-shadow: var(--shadow-sm);
    }
    .button:hover { transform: translateY(-1px); box-shadow: var(--shadow); }
    .button:active { transform: translateY(0); }
    .button.is-primary { background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%); }
    .button.is-primary:hover { background: linear-gradient(135deg, var(--primary-light) 0%, var(--primary) 100%); }
    .button.is-info { background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); color: white; }
    .button.is-warning { background: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%); }
    .button.is-danger { background: linear-gradient(135deg, #f87171 0%, #ef4444 100%); color: white; }
    .button.is-light { background: white; border: 1px solid var(--gray-200); }
    .button.is-light:hover { background: var(--gray-50); }

    /* Live indicator */
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
        color: #991b1b;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .live-badge::before {
        content: '';
        width: 8px;
        height: 8px;
        background: #ef4444;
        border-radius: 50%;
        animation: pulse-red 1.5s infinite;
    }
    @keyframes pulse-red {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    @media (prefers-reduced-motion: reduce) {
        *, ::before, ::after {
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
            scroll-behavior: auto !important;
        }
    }
    details > summary { list-style: none; cursor: pointer; }
    details > summary::-webkit-details-marker { display: none; }
    details[open] > summary { border-radius: var(--radius) var(--radius) 0 0; }

    /* Form inputs */
    .textarea, .input {
        border-radius: var(--radius-sm);
        border: 1px solid var(--gray-300);
        transition: all 0.2s ease;
    }
    .textarea:focus, .input:focus {
        border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
    }
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

    /* Section styling */
    .section-card {
        background: white;
        border-radius: var(--radius);
        box-shadow: var(--shadow-md);
        padding: 0;
        overflow: hidden;
    }
    .section-card-header {
        padding: 1.25rem 1.5rem;
        border-bottom: 1px solid var(--gray-100);
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: linear-gradient(135deg, #fff 0%, var(--gray-50) 100%);
    }
    .section-card-body { padding: 0; }

    /* Toast notifications */
    .toast-container {
        position: fixed;
        top: 1rem;
        right: 1rem;
        z-index: 1000;
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }
    .toast {
        padding: 1rem 1.5rem;
        border-radius: var(--radius-sm);
        box-shadow: var(--shadow-lg);
        animation: slideIn 0.3s ease-out;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .toast.is-success { background: linear-gradient(135deg, var(--success) 0%, #059669 100%); color: white; }
    .toast.is-danger { background: linear-gradient(135deg, var(--danger) 0%, #dc2626 100%); color: white; }
    .toast.is-warning { background: linear-gradient(135deg, var(--warning) 0%, #d97706 100%); color: rgba(0,0,0,0.8); }
    .toast-close {
        background: none;
        border: none;
        color: inherit;
        cursor: pointer;
        opacity: 0.7;
        font-size: 1.2rem;
        padding: 0;
        margin-left: 0.5rem;
    }
    .toast-close:hover { opacity: 1; }
    @keyframes slideIn {
        from { transform: translateX(100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(100%); opacity: 0; }
    }
    /* Confirmation modal */
    .modal-card {
        max-width: 400px;
        border-radius: var(--radius);
        overflow: hidden;
        box-shadow: var(--shadow-lg);
    }
    .modal-card-head {
        border: none;
        background: white;
        padding: 1.5rem;
    }
    .modal-card-foot {
        border: none;
        background: var(--gray-50);
        padding: 1rem 1.5rem;
    }
    .modal-background { background: rgba(17, 24, 39, 0.6); backdrop-filter: blur(4px); }
    /* Empty state */
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        color: var(--gray-500);
    }
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }
    .empty-state p { margin-bottom: 0.5rem; }
    .empty-state .is-size-5 { color: var(--gray-700); font-weight: 500; }
    /* Section header with actions */
    .section-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .section-header .title { margin-bottom: 0; }
    /* Footer */
    .footer {
        margin-top: auto;
        padding: 2rem;
        background: white;
        border-top: 1px solid var(--gray-200);
    }
    .footer-content {
        display: flex;
        justify-content: space-between;
        align-items: center;
        color: var(--gray-500);
        font-size: 0.875rem;
    }
    .footer-links a {
        color: var(--gray-500);
        margin-left: 1.5rem;
        transition: color 0.2s ease;
    }
    .footer-links a:hover { color: var(--primary); }

    /* Stats cards in job details */
    .stat-card {
        background: white;
        border-radius: var(--radius);
        padding: 1.25rem;
        box-shadow: var(--shadow);
        border: 1px solid var(--gray-100);
    }
    .stat-card .heading {
        color: var(--gray-500);
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.5rem;
    }
    .stat-card .title { color: var(--gray-800); font-weight: 600; }

    /* Breadcrumb */
    .breadcrumb {
        background: white;
        padding: 0.75rem 1rem;
        border-radius: var(--radius-sm);
        box-shadow: var(--shadow-sm);
        margin-bottom: 1.5rem;
    }
    .breadcrumb a { color: var(--gray-500); }
    .breadcrumb a:hover { color: var(--primary); }
    .breadcrumb .is-active a { color: var(--gray-800); font-weight: 500; }
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

    // Toast notifications
    function showToast(message, type = 'is-success', duration = 4000) {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <span>${message}</span>
            <button class="toast-close">&times;</button>
        `;
        toast.querySelector('.toast-close').addEventListener('click', function() { closeToast(this); });
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-in forwards';
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }

    function closeToast(btn) {
        const toast = btn.closest('.toast');
        toast.style.animation = 'slideOut 0.3s ease-in forwards';
        setTimeout(() => toast.remove(), 300);
    }

    // Confirmation dialog
    function showConfirm(title, message, onConfirm) {
        const modal = document.getElementById('confirm-modal');
        document.getElementById('confirm-title').textContent = title;
        document.getElementById('confirm-message').textContent = message;
        modal.classList.add('is-active');

        const confirmBtn = document.getElementById('confirm-btn');
        const newConfirmBtn = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
        newConfirmBtn.id = 'confirm-btn';
        newConfirmBtn.addEventListener('click', () => {
            closeModal();
            onConfirm();
        });
    }

    function closeModal() {
        document.getElementById('confirm-modal').classList.remove('is-active');
    }

    // Wrap dangerous actions with confirmation
    document.body.addEventListener('click', function(e) {
        const btn = e.target.closest('[data-confirm]');
        if (btn && !btn.dataset.confirmPending) {
            e.preventDefault();
            e.stopPropagation();
            const message = btn.dataset.confirm;
            const title = btn.dataset.confirmTitle || 'Confirm Action';
            showConfirm(title, message, () => {
                // Mark as pending to avoid re-triggering confirmation
                btn.dataset.confirmPending = 'true';
                // Trigger the htmx request manually
                htmx.trigger(btn, 'confirmed');
                // Clear pending flag after a short delay
                setTimeout(() => delete btn.dataset.confirmPending, 100);
            });
        }
    });

    // Track request start time for minimum loading display
    document.body.addEventListener('htmx:beforeRequest', function(e) {
        e.detail.elt.dataset.htmxRequestStart = Date.now();
    });

    // Listen for htmx events to show toasts
    document.body.addEventListener('htmx:afterRequest', function(e) {
        const xhr = e.detail.xhr;
        if (!xhr) return;

        if (xhr.status >= 200 && xhr.status < 300) {
            // Check the triggering element and its ancestors for success message
            let el = e.detail.elt;
            while (el && !el.dataset.successMsg) {
                el = el.parentElement;
            }
            if (el && el.dataset.successMsg) {
                showToast(el.dataset.successMsg, 'is-success');
            }
        } else if (xhr.status >= 400) {
            // Handle error responses
            let message = 'An error occurred';
            try {
                const resp = JSON.parse(xhr.responseText);
                message = resp.message || message;
            } catch {}
            showToast(message, 'is-danger', 6000);

            // Re-enable the triggering element on error (with minimum loading time)
            const elt = e.detail.elt;
            if (elt) {
                const minLoadingTime = 300;
                const startTime = elt.dataset.htmxRequestStart;
                const elapsed = startTime ? Date.now() - parseInt(startTime) : minLoadingTime;
                const delay = Math.max(0, minLoadingTime - elapsed);
                setTimeout(() => {
                    elt.classList.remove('htmx-request');
                    elt.disabled = false;
                }, delay);
            }
        }
    });

    // Hamburger menu toggle
    document.addEventListener('DOMContentLoaded', () => {
        // Modal close handlers
        document.querySelectorAll('[data-close-modal]').forEach(el => {
            el.addEventListener('click', closeModal);
        });

        // Copy button handlers
        document.querySelectorAll('.copy-btn').forEach(btn => {
            btn.addEventListener('click', () => copyToClipboard(btn));
        });

        const burgers = document.querySelectorAll('.navbar-burger');
        burgers.forEach(burger => {
            burger.addEventListener('click', () => {
                const target = document.getElementById(burger.dataset.target);
                burger.classList.toggle('is-active');
                if (target) {
                    target.classList.toggle('is-active');
                }
            });
        });

        // Live running time counter
        updateRunningTime();
        setInterval(updateRunningTime, 1000);
    });

    function updateRunningTime() {
        const el = document.getElementById('running-time');
        if (!el || !el.dataset.startedAt) return;

        const startedAt = new Date(el.dataset.startedAt);
        const now = new Date();
        const diff = Math.floor((now - startedAt) / 1000);

        const days = Math.floor(diff / 86400);
        const hours = Math.floor((diff % 86400) / 3600);
        const minutes = Math.floor((diff % 3600) / 60);
        const seconds = diff % 60;

        let parts = [];
        if (days > 0) parts.push(days + 'd');
        if (hours > 0 || days > 0) parts.push(hours + 'h');
        if (minutes > 0 || hours > 0 || days > 0) parts.push(minutes + 'm');
        parts.push(seconds + 's');

        el.textContent = parts.join(' ');
    }

    highlightAll();
    document.body.addEventListener('htmx:afterSwap', highlightAll);
    document.body.addEventListener('htmx:afterSwap', () => {
        updateRunningTime();
    });
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

const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    <div id="toast-container" class="toast-container"></div>

    <!-- Confirmation Modal -->
    <div id="confirm-modal" class="modal">
        <div class="modal-background" data-close-modal></div>
        <div class="modal-card">
            <header class="modal-card-head">
                <p class="modal-card-title" id="confirm-title">Confirm</p>
                <button class="delete" aria-label="close" data-close-modal></button>
            </header>
            <section class="modal-card-body">
                <p id="confirm-message"></p>
            </section>
            <footer class="modal-card-foot">
                <button class="button" data-close-modal>Cancel</button>
                <button class="button is-danger" id="confirm-btn">Confirm</button>
            </footer>
        </div>
    </div>

    <nav class="navbar" role="navigation" aria-label="main navigation">
        <div class="container">
            <div class="navbar-brand">
                <a class="navbar-item has-text-weight-bold is-size-4" href="/">
                    <span style="display: inline-flex; align-items: center; justify-content: center; width: 32px; height: 28px; background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%); color: white; border-radius: 6px; font-size: 14px; font-weight: 700; margin-right: 0.5rem;">MS</span> mstream
                </a>
                <a role="button" class="navbar-burger" aria-label="menu" aria-expanded="false" data-target="navMenu">
                    <span aria-hidden="true"></span>
                    <span aria-hidden="true"></span>
                    <span aria-hidden="true"></span>
                </a>
            </div>
            <div id="navMenu" class="navbar-menu">
            </div>
        </div>
    </nav>

    <section class="section" style="padding-top: 2rem;">
        <div class="container">{content}</div>
    </section>

    <footer class="footer">
        <div class="container">
            <div class="footer-content">
                <span>mstream v{version}</span>
                <div class="footer-links">
                    <a href="https://github.com/makarski/mstream" target="_blank" rel="noopener noreferrer">GitHub</a>
                </div>
            </div>
        </div>
    </footer>

    <script>{js}</script>
</body>
</html>"##,
        title = title,
        css = LAYOUT_CSS,
        content = content,
        js = LAYOUT_JS,
        version = VERSION
    ))
}

fn render_dashboard_header() -> &'static str {
    r#"<div class="level mb-5">
        <div class="level-left">
            <div class="level-item">
                <div>
                    <h1 class="title is-3 mb-1">Dashboard</h1>
                    <p class="has-text-grey">Monitor and manage your data pipelines</p>
                </div>
            </div>
        </div>
    </div>"#
}

fn render_jobs_panel() -> &'static str {
    r##"<div class="section-card">
        <div class="section-card-header">
            <h2 class="title is-5 mb-0">Jobs</h2>
            <div class="buttons are-small mb-0">
                <button class="button is-light" hx-get="/ui/jobs" hx-target="#jobs-container" hx-swap="innerHTML" title="Refresh">
                    ↻
                </button>
            </div>
        </div>
        <div class="section-card-body">
            <details>
                <summary class="button is-primary is-light is-fullwidth is-radiusless" style="border: none; height: 3rem; border-radius: 0;">
                    <span class="has-text-weight-semibold">+ Create New Job</span>
                </summary>
                <div class="p-5" style="background: var(--gray-50); border-bottom: 1px solid var(--gray-200);">
                    <form hx-post="/ui/jobs" hx-target="#jobs-container" hx-swap="innerHTML" hx-disabled-elt="button[type='submit']" data-success-msg="Job created successfully">
                        <div class="field">
                            <label class="label is-small">Configuration (JSON)</label>
                            <div class="control">
                                <textarea class="textarea is-family-monospace is-small" name="config_json" placeholder='{{ "name": "my-job", "source": {{ ... }} }}' rows="5"></textarea>
                            </div>
                        </div>
                        <div class="control">
                            <button class="button is-primary" type="submit">Start Job</button>
                        </div>
                    </form>
                </div>
            </details>
            <div id="jobs-container" hx-get="/ui/jobs" hx-trigger="load, every 30s">
                <div class="p-5 has-text-centered">
                    <span class="has-text-grey">Loading...</span>
                </div>
            </div>
        </div>
    </div>"##
}

fn render_services_panel() -> &'static str {
    r##"<div class="section-card">
        <div class="section-card-header">
            <h2 class="title is-5 mb-0">Services</h2>
            <div class="buttons are-small mb-0">
                <button class="button is-light" hx-get="/ui/services" hx-target="#services-container" hx-swap="innerHTML" title="Refresh">
                    ↻
                </button>
            </div>
        </div>
        <div class="section-card-body">
            <details>
                <summary class="button is-info is-light is-fullwidth is-radiusless" style="border: none; height: 3rem; border-radius: 0;">
                    <span class="has-text-weight-semibold">+ Create New Service</span>
                </summary>
                <div class="p-5" style="background: var(--gray-50); border-bottom: 1px solid var(--gray-200);">
                    <form hx-post="/ui/services" hx-target="#services-container" hx-swap="innerHTML" hx-disabled-elt="button[type='submit']" data-success-msg="Service created successfully">
                        <div class="field">
                            <label class="label is-small">Configuration (JSON)</label>
                            <div class="control">
                                <textarea class="textarea is-family-monospace is-small" name="config_json" placeholder='{{ "provider": "kafka", "name": "my-kafka", ... }}' rows="5"></textarea>
                            </div>
                        </div>
                        <div class="control">
                            <button class="button is-info" type="submit">Create Service</button>
                        </div>
                    </form>
                </div>
            </details>
            <div id="services-container" hx-get="/ui/services" hx-trigger="load, services-update from:body">
                <div class="p-5 has-text-centered">
                    <span class="has-text-grey">Loading...</span>
                </div>
            </div>
        </div>
    </div>"##
}

async fn get_root() -> Html<String> {
    let content = format!(
        r#"
    {}
    <div class="columns">
        <div class="column is-7">
            {}
        </div>
        <div class="column is-5">
            {}
        </div>
    </div>
    "#,
        render_dashboard_header(),
        render_jobs_panel(),
        render_services_panel()
    );

    layout("Dashboard", &content)
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
        let started_at_iso = job.started_at.to_rfc3339();
        format!(
            r#"<span id="running-time" data-started-at="{}">{}</span>"#,
            started_at_iso,
            format_duration(d)
        )
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
            <button class="button is-small is-white copy-btn">Copy</button>
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

/// Configuration for rendering a table with empty state fallback.
struct TableConfig {
    headers: &'static [&'static str],
    empty_icon: &'static str,
    empty_title: &'static str,
    empty_description: &'static str,
}

fn render_table_or_empty<T, F>(
    items: &[T],
    config: TableConfig,
    render_row: F,
    error: Option<&str>,
) -> Html<String>
where
    F: Fn(&T) -> String,
{
    if items.is_empty() && error.is_none() {
        return Html(format!(
            r#"<div class="empty-state">
                <div class="empty-state-icon">{}</div>
                <p class="is-size-5">{}</p>
                <p>{}</p>
            </div>"#,
            config.empty_icon, config.empty_title, config.empty_description
        ));
    }
    let rows: String = items.iter().map(render_row).collect();
    render_table(config.headers, &rows, error)
}

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
    render_table_or_empty(
        jobs,
        TableConfig {
            headers: &["Name", "State", "Checkpoint", "Started At", ""],
            empty_icon: "📋",
            empty_title: "No jobs running",
            empty_description: "Create a job to start streaming data",
        },
        render_job_row,
        error,
    )
}

fn render_job_row(job: &JobMetadata) -> String {
    let (status_tag, status_dot) = match job.state {
        JState::Running => ("is-success", "is-running"),
        JState::Stopped => ("is-light", "is-stopped"),
        JState::Failed => ("is-danger", "is-failed"),
    };

    let job_name = escape_html(&job.name);

    let checkpoint_enabled = job
        .pipeline
        .as_ref()
        .and_then(|p| p.checkpoint.as_ref())
        .map_or(false, |c| c.enabled);

    let checkpoint_badge = if checkpoint_enabled {
        r#"<span class="tag is-link is-light" title="Checkpoint enabled">✓</span>"#
    } else {
        r#"<span class="tag is-light" title="Checkpoint disabled">—</span>"#
    };

    let actions = format!(
        r#"
        <div class="buttons are-small">
            <button class="button is-warning is-light"
                hx-post="/ui/jobs/{}/stop"
                hx-target="closest tr"
                hx-swap="outerHTML"
                hx-trigger="confirmed"
                hx-disabled-elt="this"
                data-confirm="Are you sure you want to stop job '{}'?"
                data-confirm-title="Stop Job"
                data-success-msg="Job stopped">Stop</button>
            <button class="button is-info is-light"
                hx-post="/ui/jobs/{}/restart"
                hx-target="closest tr"
                hx-swap="outerHTML"
                hx-disabled-elt="this"
                data-success-msg="Job restarted">Restart</button>
        </div>
        "#,
        job_name, job_name, job_name
    );

    format!(
        r#"
        <tr>
            <td>
                <a href="/ui/jobs/{}" class="has-text-weight-medium" style="color: var(--gray-800);">{}</a>
            </td>
            <td>
                <span class="status-dot {}"></span>
                <span class="tag {}">{}</span>
            </td>
            <td>{}</td>
            <td style="color: var(--gray-500); font-size: 0.875rem;">{}</td>
            <td>{}</td>
        </tr>
        "#,
        job_name,
        job_name,
        status_dot,
        status_tag,
        job.state,
        checkpoint_badge,
        job.started_at.format("%Y-%m-%d %H:%M:%S"),
        actions
    )
}

async fn stop_job_ui(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let stop_result = {
        let mut jm = state.job_manager.lock().await;
        jm.stop_job(&name).await
    };

    if let Err(e) = &stop_result {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(serde_json::json!({ "message": e.to_string() }).to_string()),
        )
            .into_response();
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
            StatusCode::OK,
            [("HX-Trigger", "services-update")],
            Html(render_job_row(&job)),
        )
            .into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(
            serde_json::json!({ "message": format!("Error stopping job {}", escape_html(&name)) })
                .to_string(),
        ),
    )
        .into_response()
}

async fn restart_job_ui(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut jm = state.job_manager.lock().await;
    let res = jm.restart_job(&name).await;

    // If called from details page, redirect on success or return error
    if let Some(ctx) = headers.get("ui-context") {
        if ctx == "details" {
            return match res {
                Ok(_) => (
                    StatusCode::OK,
                    [("HX-Redirect", format!("/ui/jobs/{}", escape_html(&name)))],
                    Html(String::new()),
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(serde_json::json!({ "message": e.to_string() }).to_string()),
                )
                    .into_response(),
            };
        }
    }

    match res {
        Ok(job) => (
            StatusCode::OK,
            [("HX-Trigger", "services-update")],
            Html(render_job_row(&job)),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(serde_json::json!({ "message": e.to_string() }).to_string()),
        )
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
            <button class="button is-small is-white copy-btn">Copy</button>
            <pre class="config json-content">{}</pre>
        </div>

        {}
        "##,
        service_name,
        service_name,
        if status.is_system {
            r#"<span class="tag is-warning is-light mr-2">🔒 System Service</span><button class="button is-danger is-light" disabled title="Cannot delete system service">Delete Service</button>"#.to_string()
        } else if status.used_by_jobs.is_empty() {
            format!(
                r##"<button class="button is-danger is-light"
                    hx-delete="/ui/services/{}"
                    hx-target="body"
                    hx-trigger="confirmed"
                    hx-disabled-elt="this"
                    data-confirm="Are you sure you want to delete service '{}'?"
                    data-confirm-title="Delete Service"
                    data-success-msg="Service deleted">Delete Service</button>"##,
                service_name, service_name
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
    render_table_or_empty(
        services,
        TableConfig {
            headers: &["Name", "Provider", "Used By", ""],
            empty_icon: "🔌",
            empty_title: "No services configured",
            empty_description: "Create a service to connect to MongoDB, Kafka, or PubSub",
        },
        render_service_row,
        error,
    )
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

    let system_badge = if status.is_system {
        r#" <span class="tag is-warning is-light">🔒 System</span>"#
    } else {
        ""
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

    let delete_btn = if status.is_system {
        r#"<button class="button is-small is-danger is-light" disabled title="Cannot delete system service">Delete</button>"#.to_string()
    } else if status.used_by_jobs.is_empty() {
        format!(
            r##"<button class="button is-small is-danger is-light"
                hx-delete="/ui/services/{}"
                hx-target="#services-container"
                hx-trigger="confirmed"
                data-confirm="Are you sure you want to delete service '{}'?"
                data-confirm-title="Delete Service"
                data-success-msg="Service deleted">Delete</button>"##,
            name, name
        )
    } else {
        r#"<button class="button is-small is-danger is-light" disabled title="Cannot delete service while in use">Delete</button>"#.to_string()
    };

    format!(
        r#"
        <tr>
            <td><a href="/ui/services/{}" class="has-text-weight-medium">{}</a>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
        name, name, system_badge, provider, used_by, delete_btn
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
