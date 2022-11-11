use crate::pubsub::api::{GetSchemaRequest, ListSchemasRequest, ListSchemasResponse, Schema};
use crate::pubsub::schema::{schema_service, SchemaService};

pub struct Registry {
    schema_service: SchemaService,
}

impl Registry {
    pub async fn with_token(access_token: &str) -> anyhow::Result<Self> {
        let schema_service = schema_service(access_token).await?;

        Ok(Registry {
            schema_service: schema_service,
        })
    }

    pub async fn list_schemas(&mut self, parent: String) -> anyhow::Result<ListSchemasResponse> {
        let schema_list_response = self
            .schema_service
            .list_schemas(ListSchemasRequest {
                parent: parent,
                ..Default::default()
            })
            .await?;

        Ok(schema_list_response.into_inner())
    }

    pub async fn get_schema(&mut self, name: String) -> anyhow::Result<Schema> {
        let get_schema_response = self
            .schema_service
            .get_schema(GetSchemaRequest {
                name: name,
                ..Default::default()
            })
            .await?;

        Ok(get_schema_response.into_inner())
    }
}
