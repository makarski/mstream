use std::collections::HashMap;

use super::{tls_transport, Channel, InterceptedService};
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse, Schema};
use anyhow::anyhow;
use tonic::service::Interceptor;

pub type PublisherService<I> = PublisherClient<InterceptedService<Channel, I>>;

pub async fn publisher<I: Interceptor>(interceptor: I) -> anyhow::Result<PublisherService<I>> {
    let channel = tls_transport().await?;
    Ok(PublisherClient::with_interceptor(channel, interceptor))
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

    pub async fn get_schema(&mut self, name: String) -> anyhow::Result<&Schema> {
        if !self.cache.contains_key(&name) {
            let schema_response = self.client.get_schema(GetSchemaRequest {
                name: name.clone(),
                ..Default::default()
            });

            self.cache
                .insert(name.clone(), schema_response.await?.into_inner());

            log::info!("schema {} added to cache", name);
        } else {
            log::info!("schema {} found in cache", name);
        }

        self.cache
            .get(&name)
            .ok_or_else(|| anyhow!("schema not found"))
    }
}
