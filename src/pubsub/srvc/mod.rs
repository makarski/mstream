use std::collections::HashMap;

use anyhow::{anyhow, Ok};
use apache_avro::Schema;
use async_trait::async_trait;
use tonic::service::Interceptor;

use super::{tls_transport, Channel, InterceptedService};
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse};
use crate::pubsub::api::{PublishRequest, PubsubMessage};
use crate::schema::SchemaProvider;
use crate::sink::EventSink;

pub struct PubSubPublisher<I> {
    client: PublisherClient<InterceptedService<Channel, I>>,
}

impl<I: Interceptor> PubSubPublisher<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport().await?;
        Ok(Self {
            client: PublisherClient::with_interceptor(channel, interceptor),
        })
    }
}

#[async_trait]
impl<I: Interceptor + Send> EventSink for PubSubPublisher<I> {
    async fn publish(
        &mut self,
        topic: String,
        b: Vec<u8>,
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<String> {
        let req = PublishRequest {
            topic: topic.clone(),
            messages: vec![PubsubMessage {
                data: b,
                attributes,
                ..Default::default()
            }],
        };

        let msg = self
            .client
            .publish(req)
            .await
            .map_err(|err| anyhow!("{}. topic: {}", err.message(), &topic))?;

        let msg = msg.into_inner().message_ids[0].clone();
        Ok(msg)
    }
}

pub struct SchemaService<I> {
    client: SchemaServiceClient<InterceptedService<Channel, I>>,
    cache: HashMap<String, Schema>,
}

impl<I: Interceptor> SchemaService<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport().await?;
        let client = SchemaServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            client,
            cache: HashMap::new(),
        })
    }

    pub async fn list_schemas(&mut self, parent: String) -> anyhow::Result<ListSchemasResponse> {
        let schema_list_response = self
            .client
            .list_schemas(ListSchemasRequest {
                parent,
                ..Default::default()
            })
            .await?;

        Ok(schema_list_response.into_inner())
    }
}

#[async_trait]
impl<I: Interceptor + Send> SchemaProvider for SchemaService<I> {
    async fn get_schema(&mut self, id: String) -> anyhow::Result<Schema> {
        if !self.cache.contains_key(&id) {
            let schema_response = self.client.get_schema(GetSchemaRequest {
                name: id.clone(),
                ..Default::default()
            });

            let pubsub_schema = schema_response.await?.into_inner();
            let avro_schema = Schema::parse_str(&pubsub_schema.definition)?;
            self.cache.insert(id.clone(), avro_schema);

            log::info!("schema {} added to cache", id);
        } else {
            log::info!("schema {} found in cache", id);
        }

        self.cache
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("schema not found"))
    }
}
