use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, anyhow};
use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use mongodb::Client;
use tokio::sync::RwLock;

use crate::{
    config::{
        Service,
        service_config::{
            GcpAuthConfig, HttpConfig, MongoDbConfig, PubSubConfig, UdfConfig, UdfEngine,
        },
    },
    http,
    middleware::udf::rhai::RhaiMiddleware,
    mongodb::db_client,
    provision::registry::in_memory::InMemoryServiceStorage,
    pubsub::{SCOPES, ServiceAccountAuth, StaticAccessToken},
};

type RhaiMiddlewareBuilder =
    Arc<dyn Fn(String) -> anyhow::Result<RhaiMiddleware> + Send + Sync + 'static>;

pub struct ServiceRegistry {
    storage: in_memory::InMemoryServiceStorage,
    mongo_clients: RwLock<HashMap<String, Client>>,
    gcp_token_providers: RwLock<HashMap<String, ServiceAccountAuth>>,
    http_services: RwLock<HashMap<String, http::HttpService>>,
    udf_middlewares: RwLock<HashMap<String, RhaiMiddlewareBuilder>>,
}

impl ServiceRegistry {
    /// Creates a new ServiceRegistry instance
    /// This is a noop constructor, services need to be
    /// initialized via `init` method
    pub fn new(storage: InMemoryServiceStorage) -> ServiceRegistry {
        Self {
            storage,
            mongo_clients: RwLock::new(HashMap::new()),
            gcp_token_providers: RwLock::new(HashMap::new()),
            http_services: RwLock::new(HashMap::new()),
            udf_middlewares: RwLock::new(HashMap::new()),
        }
    }

    pub async fn init(&mut self, seed_services: Vec<Service>) -> anyhow::Result<()> {
        for service in seed_services {
            self.register_service(service.clone()).await?;
        }
        Ok(())
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

    pub async fn register_service(&mut self, service_cfg: Service) -> anyhow::Result<()> {
        self.storage.save(service_cfg.clone()).await?;

        match &service_cfg {
            Service::MongoDb(_) => self.init_mongo(&service_cfg).await?,
            Service::PubSub(_) => self.init_pubsub(&service_cfg).await?,
            Service::Http(_) => self.init_http(&service_cfg).await?,
            Service::Udf(_) => self.init_udf(&service_cfg).await?,
            _ => {}
        }

        Ok(())
    }

    pub async fn remove_service(&mut self, service_name: &str) -> anyhow::Result<()> {
        let config_removed = self.storage.remove(service_name).await?;
        if config_removed {
            self.mongo_clients.write().await.remove(service_name);
            self.gcp_token_providers.write().await.remove(service_name);
            self.http_services.write().await.remove(service_name);
            self.udf_middlewares.write().await.remove(service_name);
            Ok(())
        } else {
            Err(anyhow!("service with name '{}' not found", service_name))
        }
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
        self.storage.get_by_name(service_name).await
    }

    pub async fn all_service_definitions(&self) -> anyhow::Result<Vec<Service>> {
        self.storage.get_all().await
    }
}

pub mod in_memory {
    use crate::config::Service;

    use anyhow::{anyhow, bail};

    pub struct InMemoryServiceStorage {
        active_services: Vec<Service>,
    }

    impl InMemoryServiceStorage {
        pub fn new() -> Self {
            Self {
                active_services: vec![],
            }
        }

        pub async fn save(&mut self, service: Service) -> anyhow::Result<()> {
            if self.has_service(service.name()) {
                bail!("service with name '{}' already exists", service.name());
            }
            self.active_services.push(service);
            Ok(())
        }

        pub async fn remove(&mut self, service_name: &str) -> anyhow::Result<bool> {
            let removed = if let Some(pos) = self
                .active_services
                .iter()
                .position(|s| s.name() == service_name)
            {
                self.active_services.remove(pos);
                true
            } else {
                false
            };

            Ok(removed)
        }

        pub async fn get_by_name(&self, service_name: &str) -> anyhow::Result<Service> {
            self.service_by_name(service_name)
                .cloned()
                .ok_or_else(|| anyhow!("service config not found for: {}", service_name))
        }

        pub async fn get_all(&self) -> anyhow::Result<Vec<Service>> {
            Ok(self.active_services.clone())
        }

        fn has_service(&self, name: &str) -> bool {
            self.active_services.iter().any(|s| s.name() == name)
        }

        fn service_by_name(&self, name: &str) -> Option<&Service> {
            self.active_services.iter().find(|s| s.name() == name)
        }
    }
}
