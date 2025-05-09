use std::error::Error;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use gauth::token_provider::{AsyncTokenProvider, Watcher};
use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};

pub mod api {
    include!("api/google.pubsub.v1.rs");
}
pub mod srvc;

const ENDPOINT: &str = "https://pubsub.googleapis.com:443";
pub const SCOPES: [&str; 1] = ["https://www.googleapis.com/auth/pubsub"];

#[derive(Clone)]
pub struct ServiceAccountAuth(Arc<RwLock<dyn GCPTokenProvider + Send + Sync>>);

impl ServiceAccountAuth {
    pub fn new<TP: GCPTokenProvider + Send + Sync + 'static>(token_provider: TP) -> Self {
        Self(Arc::new(RwLock::new(token_provider)))
    }
}

pub trait GCPTokenProvider {
    fn gcp_token(&mut self) -> anyhow::Result<String>;
}

impl<T: Watcher + Clone + Send + 'static> GCPTokenProvider for AsyncTokenProvider<T> {
    fn gcp_token(&mut self) -> anyhow::Result<String> {
        Ok(self.access_token()?)
    }
}

impl Interceptor for ServiceAccountAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let access_token = self
            .0
            .clone()
            .write()
            .map_err(|_| {
                Status::new(
                    Code::Internal,
                    "failed to acquire write lock on token provider".to_string(),
                )
            })?
            .gcp_token()
            .map_err(|err| {
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

#[derive(Clone, Debug)]
pub struct StaticAccessToken(pub String);

impl GCPTokenProvider for StaticAccessToken {
    fn gcp_token(&mut self) -> anyhow::Result<String> {
        Ok(format!("Bearer {}", &self.0))
    }
}

pub async fn tls_transport() -> anyhow::Result<Channel> {
    let tls_config = ClientTlsConfig::new()
        .with_enabled_roots()
        .domain_name("pubsub.googleapis.com");

    match Channel::from_static(ENDPOINT)
        .tls_config(tls_config)?
        .connect()
        .await
    {
        Ok(channel) => Ok(channel),
        Err(err) => {
            // Check if this is a transport error and extract more details
            if let Some(source) = err.source() {
                log::error!("Error source: {:?}", source);
            }

            return Err(anyhow!("failed to initiate tls_transport: {}", err));
        }
    }
}
