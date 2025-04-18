use anyhow::bail;

use crate::config::Encoding;
use crate::source::SourceEvent;

use super::HttpService;

pub struct HttpMiddleware {
    resource: String,
    encoding: Encoding,
    service: HttpService,
}

impl HttpMiddleware {
    pub fn new(resource: String, encoding: Encoding, service: HttpService) -> Self {
        HttpMiddleware {
            resource,
            encoding,
            service,
        }
    }

    pub async fn transform(&mut self, event: SourceEvent) -> anyhow::Result<SourceEvent> {
        let payload = match event.raw_bytes {
            Some(bytes) => bytes,
            None => bail!("raw_bytes is missing for http middleware"),
        };

        let response = self
            .service
            .post(
                &self.resource,
                payload,
                event.encoding,
                event.attributes.clone(),
            )
            .await?;

        Ok(SourceEvent {
            raw_bytes: Some(response.as_bytes().to_vec()),
            document: None,
            attributes: event.attributes,
            encoding: self.encoding.clone(),
        })
    }
}
