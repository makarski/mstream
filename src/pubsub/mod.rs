use anyhow::anyhow;
use gauth::serv_account::ServiceAccount;
use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};

pub mod api {
    include!("api/google.pubsub.v1.rs");
}
pub mod srvc;

const ENDPOINT: &str = "https://pubsub.googleapis.com";
pub const SCOPES: [&str; 1] = ["https://www.googleapis.com/auth/pubsub"];

#[derive(Clone, Debug)]
pub struct ServiceAccountAuth<P: GCPTokenProvider + Clone>(P);

impl<P: GCPTokenProvider + Clone> ServiceAccountAuth<P> {
    pub fn new(token_provider: P) -> Self {
        Self(token_provider)
    }
}

pub trait GCPTokenProvider {
    fn access_token(&mut self) -> anyhow::Result<String>;
}

impl GCPTokenProvider for ServiceAccount {
    fn access_token(&mut self) -> anyhow::Result<String> {
        Ok(self.access_token()?)
    }
}

impl<P: GCPTokenProvider + Clone> Interceptor for ServiceAccountAuth<P> {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let access_token = self.0.access_token().map_err(|err| {
            Status::new(
                Code::InvalidArgument,
                format!("failed to retrieve access token: {}", err),
            )
        })?;

        request.metadata_mut().insert(
            "authorization",
            access_token.parse().map_err(|err| {
                Status::new(
                    Code::InvalidArgument,
                    format!("failed to parse access token: {}", err),
                )
            })?,
        );

        Ok(request)
    }
}

pub async fn tls_transport() -> anyhow::Result<Channel> {
    let tls_config = ClientTlsConfig::new();

    let channel = Channel::from_static(ENDPOINT)
        .tls_config(tls_config)?
        .connect()
        .await
        .map_err(|err| anyhow!("failed to initiate tls_transport: {}", err))?;

    Ok(channel)
}
