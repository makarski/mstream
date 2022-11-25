use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};

pub mod api {
    include!("api/google.pubsub.v1.rs");
}

const ENDPOINT: &str = "https://pubsub.googleapis.com";

pub struct AuthInterceptor(String);

impl AuthInterceptor {
    fn access_token(&self) -> String {
        format!("Bearer {}", self.0)
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request.metadata_mut().insert(
            "authorization",
            self.access_token().parse().map_err(|err| {
                Status::new(
                    Code::InvalidArgument,
                    format!("failed to parse access token: {}", err),
                )
            })?,
        );

        Ok(request)
    }
}

async fn tls_transport() -> anyhow::Result<Channel> {
    let tls_config = ClientTlsConfig::new();

    let channel = Channel::from_static(ENDPOINT)
        .tls_config(tls_config)?
        .connect()
        .await?;

    Ok(channel)
}

pub mod schema {
    use super::{tls_transport, AuthInterceptor, Channel, InterceptedService};
    use crate::pubsub::api::schema_service_client::SchemaServiceClient;

    pub type SchemaService = SchemaServiceClient<InterceptedService<Channel, AuthInterceptor>>;

    pub async fn schema_service(access_token: &str) -> anyhow::Result<SchemaService> {
        let auth = AuthInterceptor(access_token.to_string());
        let channel = tls_transport().await?;

        Ok(SchemaServiceClient::with_interceptor(channel, auth))
    }
}

pub mod publ {
    use super::{tls_transport, AuthInterceptor, Channel, InterceptedService};
    use crate::pubsub::api::publisher_client::PublisherClient;

    pub type PublisherService = PublisherClient<InterceptedService<Channel, AuthInterceptor>>;

    pub async fn publisher(access_token: &str) -> anyhow::Result<PublisherService> {
        let auth = AuthInterceptor(access_token.to_string());
        let channel = tls_transport().await?;

        Ok(PublisherClient::with_interceptor(channel, auth))
    }
}

pub mod sub {
    use super::{tls_transport, AuthInterceptor, Channel, InterceptedService};
    use crate::pubsub::api::subscriber_client::SubscriberClient;

    pub type SubscriberService = SubscriberClient<InterceptedService<Channel, AuthInterceptor>>;

    pub async fn subscriber(access_token: &str) -> anyhow::Result<SubscriberService> {
        let auth = AuthInterceptor(access_token.to_string());
        let channel = tls_transport().await?;

        Ok(SubscriberClient::with_interceptor(channel, auth))
    }
}
