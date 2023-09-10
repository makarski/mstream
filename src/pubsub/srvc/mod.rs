use super::{tls_transport, Channel, InterceptedService};
use crate::pubsub::api::publisher_client::PublisherClient;
use crate::pubsub::api::schema_service_client::SchemaServiceClient;
use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse, Schema};
use tonic::service::Interceptor;

pub type PublisherService<I> = PublisherClient<InterceptedService<Channel, I>>;

pub async fn publisher<I: Interceptor>(interceptor: I) -> anyhow::Result<PublisherService<I>> {
    let channel = tls_transport().await?;
    Ok(PublisherClient::with_interceptor(channel, interceptor))
}

pub struct SchemaService<I> {
    client: SchemaServiceClient<InterceptedService<Channel, I>>,
}

impl<I: Interceptor> SchemaService<I> {
    pub async fn with_interceptor(interceptor: I) -> anyhow::Result<Self> {
        let channel = tls_transport().await?;
        let client = SchemaServiceClient::with_interceptor(channel, interceptor);
        Ok(Self { client })
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

    pub async fn get_schema(&mut self, name: String) -> anyhow::Result<Schema> {
        let get_schema_response = self
            .client
            .get_schema(GetSchemaRequest {
                name,
                ..Default::default()
            })
            .await?;

        Ok(get_schema_response.into_inner())
    }
}
