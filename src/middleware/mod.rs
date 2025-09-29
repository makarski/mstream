//! Middleware layer for event transformation.
//!
//! Provides a unified interface for transforming events through different middleware
//! implementations including HTTP endpoints and user-defined functions.

use crate::http::middleware::HttpMiddleware;
use crate::middleware::udf::UdfMiddleware;
use crate::source::SourceEvent;

pub mod udf;

/// Middleware provider for event transformation.
///
/// Encapsulates different middleware implementations and provides
/// a common interface for transforming events.
pub enum MiddlewareProvider {
    /// HTTP-based middleware for remote transformations.
    Http(HttpMiddleware),
    /// User-defined function middleware for script-based transformations.
    Udf(UdfMiddleware),
}

impl MiddlewareProvider {
    /// Transforms an event using the configured middleware.
    ///
    /// Routes the event to the appropriate middleware implementation
    /// based on the provider type.
    pub async fn transform(&mut self, event: SourceEvent) -> anyhow::Result<SourceEvent> {
        match self {
            MiddlewareProvider::Http(middleware) => middleware.transform(event).await,
            MiddlewareProvider::Udf(udf) => match udf {
                UdfMiddleware::Rhai(middleware) => Ok(middleware.transform(event).await?),
            },
        }
    }
}
