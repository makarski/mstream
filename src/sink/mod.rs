use std::collections::HashMap;

use async_trait::async_trait;

#[async_trait]
pub trait EventSink {
    async fn publish(
        &mut self,
        topic: String,
        b: Vec<u8>,
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<String>;
}
