use crate::config::Encoding;
use crate::source::SourceEvent;

use super::HttpService;

pub struct HttpMiddleware {
    resource: String,
    output_encoding: Encoding,
    service: HttpService,
}

impl HttpMiddleware {
    pub fn new(resource: String, output_encoding: Encoding, service: HttpService) -> Self {
        HttpMiddleware {
            resource,
            output_encoding,
            service,
        }
    }

    pub async fn transform(&mut self, event: SourceEvent) -> anyhow::Result<SourceEvent> {
        let response = self
            .service
            .post(
                &self.resource,
                event.raw_bytes,
                event.encoding,
                event.attributes.clone(),
                event.is_framed_batch,
            )
            .await?;

        Ok(SourceEvent {
            raw_bytes: response.as_bytes().to_vec(),
            attributes: event.attributes,
            encoding: self.output_encoding.clone(),
            is_framed_batch: event.is_framed_batch,
            cursor: event.cursor,
        })
    }
}
