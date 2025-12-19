use std::{collections::HashMap, mem::take, sync::Arc};

use anyhow::{anyhow, bail, Context};
use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use mongodb::Client;
use tokio::sync::RwLock;

use crate::{
    config::{
        service_config::{
            GcpAuthConfig, HttpConfig, MongoDbConfig, PubSubConfig, UdfConfig, UdfEngine,
        },
        Config, Service,
    },
    http,
    middleware::udf::rhai::RhaiMiddleware,
    mongodb::db_client,
    pubsub::{ServiceAccountAuth, StaticAccessToken, SCOPES},
};

type RhaiMiddlewareBuilder =
    Arc<dyn Fn(String) -> anyhow::Result<RhaiMiddleware> + Send + Sync + 'static>;

pub struct ServiceRegistry {
    config: Arc<RwLock<Config>>,
    mongo_clients: RwLock<HashMap<String, Client>>,
    gcp_token_providers: RwLock<HashMap<String, ServiceAccountAuth>>,
    http_services: RwLock<HashMap<String, http::HttpService>>,
    udf_middlewares: RwLock<HashMap<String, RhaiMiddlewareBuilder>>,
}

impl ServiceRegistry {
    pub async fn new(mut config: Config) -> anyhow::Result<ServiceRegistry> {
        let services = take(&mut config.services);
        let mut sr = ServiceRegistry {
            config: Arc::new(RwLock::new(config.clone())),
            mongo_clients: RwLock::new(HashMap::new()),
            gcp_token_providers: RwLock::new(HashMap::new()),
            http_services: RwLock::new(HashMap::new()),
            udf_middlewares: RwLock::new(HashMap::new()),
        };

        for service in services {
            sr.add_service(service.clone()).await?;
        }

        Ok(sr)
    }

    async fn init_mongo(&self, service_cfg: &Service) -> anyhow::Result<()> {
        let cfg: &MongoDbConfig = service_cfg.try_into()?;

        let client = db_client(cfg.name.to_owned(), &cfg.connection_string).await?;
        self.mongo_clients
            .write()
            .await
            .insert(cfg.name.clone(), client);
        Ok(())
    }

    async fn init_pubsub(&self, service_cfg: &Service) -> anyhow::Result<()> {
        let ps_cfg: &PubSubConfig = service_cfg.try_into()?;

        let tp = Self::create_gcp_token_provider(&ps_cfg.auth).await?;
        self.gcp_token_providers
            .write()
            .await
            .insert(ps_cfg.name.clone(), tp);
        Ok(())
    }

    async fn init_http(&self, service_cfg: &Service) -> anyhow::Result<()> {
        let cfg: &HttpConfig = service_cfg.try_into()?;

        let http_service = http::HttpService::new(
            cfg.host.clone(),
            cfg.max_retries,
            cfg.base_backoff_ms,
            cfg.connection_timeout_sec,
            cfg.timeout_sec,
            cfg.tcp_keepalive_sec,
        )
        .with_context(|| anyhow!("failed to initialize http service for: {}", cfg.name))?;
        self.http_services
            .write()
            .await
            .insert(cfg.name.clone(), http_service);
        Ok(())
    }

    async fn init_udf(&self, service_cfg: &Service) -> anyhow::Result<()> {
        let cfg: &UdfConfig = service_cfg.try_into()?;
        if !matches!(cfg.engine, UdfEngine::Rhai) {
            anyhow::bail!(
                "unsupported udf engine: {:?} for service: {}",
                cfg.engine,
                cfg.name
            );
        }

        let script_path = cfg.script_path.clone();
        let callback = move |filename: String| -> anyhow::Result<RhaiMiddleware> {
            Ok(RhaiMiddleware::new(script_path.clone(), filename)?)
        };

        let udf_builder = Arc::new(callback);
        self.udf_middlewares
            .write()
            .await
            .insert(cfg.name.clone(), udf_builder);
        Ok(())
    }

    async fn add_service(&mut self, service_cfg: Service) -> anyhow::Result<()> {
        if self.config.read().await.has_service(service_cfg.name()) {
            bail!("service with name '{}' already exists", service_cfg.name());
        }

        match &service_cfg {
            Service::MongoDb { .. } => self.init_mongo(&service_cfg).await?,
            Service::PubSub { .. } => self.init_pubsub(&service_cfg).await?,
            Service::Http { .. } => self.init_http(&service_cfg).await?,
            Service::Udf { .. } => self.init_udf(&service_cfg).await?,
            _ => {}
        }

        self.config.write().await.services.push(service_cfg);

        Ok(())
    }

    pub async fn udf_middleware(&self, name: &str) -> anyhow::Result<RhaiMiddlewareBuilder> {
        self.udf_middlewares
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "udf middleware builder not found for service name: {}",
                    name
                )
            })
    }

    pub async fn mongodb_client(&self, name: &str) -> anyhow::Result<Client> {
        self.mongo_clients
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("mongodb client not found for service name: {}", name))
    }

    pub async fn gcp_auth(&self, name: &str) -> anyhow::Result<ServiceAccountAuth> {
        self.gcp_token_providers
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("gcp token provider not found for service name: {}", name))
    }

    pub async fn http_client(&self, name: &str) -> anyhow::Result<http::HttpService> {
        self.http_services
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("http service not found for service name: {}", name))
    }

    async fn create_gcp_token_provider(auth: &GcpAuthConfig) -> anyhow::Result<ServiceAccountAuth> {
        match auth {
            GcpAuthConfig::ServiceAccount { account_key_path } => {
                let service_account = ServiceAccount::from_file(&account_key_path, SCOPES.to_vec());
                let tp = AsyncTokenProvider::new(service_account).with_interval(600);
                tp.watch_updates().await;

                Ok(ServiceAccountAuth::new(tp))
            }
            GcpAuthConfig::StaticToken { env_token_name } => {
                let token = std::env::var(env_token_name)
                    .context("failed to get static token from env var")?;

                let tp = StaticAccessToken(token);
                Ok(ServiceAccountAuth::new(tp))
            }
        }
    }

    pub async fn service_definition(&self, service_name: &str) -> anyhow::Result<Service> {
        self.config
            .read()
            .await
            .service_by_name(service_name)
            .cloned()
            .ok_or_else(|| anyhow!("service config not found for: {}", service_name))
    }
}
