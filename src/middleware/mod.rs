use crate::http::middleware::HttpMiddleware;
use crate::source::SourceEvent;

pub enum MiddlewareProvider {
    Http(HttpMiddleware),
}

impl MiddlewareProvider {
    pub async fn transform(&mut self, event: SourceEvent) -> anyhow::Result<SourceEvent> {
        match self {
            MiddlewareProvider::Http(middleware) => middleware.transform(event).await,
        }
    }
}
