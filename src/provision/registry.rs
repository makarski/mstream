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
    pubsub::{SCOPES, ServiceAccountAuth, StaticAccessToken},
};

type RhaiMiddlewareBuilder =
    Arc<dyn Fn(String) -> anyhow::Result<RhaiMiddleware> + Send + Sync + 'static>;

pub struct ServiceRegistry {
    storage: ServiceStorage,
    mongo_clients: RwLock<HashMap<String, Client>>,
    gcp_token_providers: RwLock<HashMap<String, ServiceAccountAuth>>,
    http_services: RwLock<HashMap<String, http::HttpService>>,
    udf_middlewares: RwLock<HashMap<String, RhaiMiddlewareBuilder>>,
}

impl ServiceRegistry {
    /// Creates a new ServiceRegistry instance
    /// This is a noop constructor, services need to be
    /// initialized via `init` method
    pub fn new(storage: ServiceStorage) -> ServiceRegistry {
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

    async fn init_mongo(&self, cfg: MongoDbConfig) -> anyhow::Result<()> {
        let client = db_client(cfg.name.to_owned(), &cfg.connection_string).await?;
        self.mongo_clients
            .write()
            .await
            .insert(cfg.name.clone(), client);
        Ok(())
    }

    async fn init_pubsub(&self, ps_cfg: PubSubConfig) -> anyhow::Result<()> {
        let tp = Self::create_gcp_token_provider(&ps_cfg.auth).await?;
        self.gcp_token_providers
            .write()
            .await
            .insert(ps_cfg.name.clone(), tp);
        Ok(())
    }

    async fn init_http(&self, cfg: HttpConfig) -> anyhow::Result<()> {
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

    async fn init_udf(&self, cfg: UdfConfig) -> anyhow::Result<()> {
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

        match service_cfg {
            Service::MongoDb(mc) => self.init_mongo(mc).await?,
            Service::PubSub(ps) => self.init_pubsub(ps).await?,
            Service::Http(hc) => self.init_http(hc).await?,
            Service::Udf(uc) => self.init_udf(uc).await?,
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

/// Type alias for a boxed ServiceLifecycleStorage trait object
pub type ServiceStorage = Box<dyn ServiceLifecycleStorage + Send + Sync>;

#[async_trait::async_trait]
pub trait ServiceLifecycleStorage {
    async fn save(&mut self, service: Service) -> anyhow::Result<()>;
    async fn remove(&mut self, service_name: &str) -> anyhow::Result<bool>;
    async fn get_by_name(&self, service_name: &str) -> anyhow::Result<Service>;
    async fn get_all(&self) -> anyhow::Result<Vec<Service>>;
}

pub mod in_memory {
    use crate::{config::Service, provision::registry::ServiceLifecycleStorage};

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

        fn has_service(&self, name: &str) -> bool {
            self.active_services.iter().any(|s| s.name() == name)
        }

        fn service_by_name(&self, name: &str) -> Option<&Service> {
            self.active_services.iter().find(|s| s.name() == name)
        }
    }

    #[async_trait::async_trait]
    impl ServiceLifecycleStorage for InMemoryServiceStorage {
        async fn save(&mut self, service: Service) -> anyhow::Result<()> {
            if self.has_service(service.name()) {
                bail!("service with name '{}' already exists", service.name());
            }
            self.active_services.push(service);
            Ok(())
        }

        async fn remove(&mut self, service_name: &str) -> anyhow::Result<bool> {
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

        async fn get_by_name(&self, service_name: &str) -> anyhow::Result<Service> {
            self.service_by_name(service_name)
                .cloned()
                .ok_or_else(|| anyhow!("service config not found for: {}", service_name))
        }

        async fn get_all(&self) -> anyhow::Result<Vec<Service>> {
            Ok(self.active_services.clone())
        }
    }
}

pub mod mongodb_storage {
    use mongodb::{
        Database,
        bson::{self, DateTime, doc},
    };

    use crate::{
        config::Service,
        provision::{encryption::Encryptor, registry::ServiceLifecycleStorage},
    };

    #[derive(serde::Deserialize, serde::Serialize)]
    struct EncryptedService {
        name: String,
        data: Vec<u8>,
        nonce: Vec<u8>,
        updated_at: Option<DateTime>,
    }

    pub struct MongoDbServiceStorage {
        database: Database,
        coll_name: String,
        encryptor: Encryptor,
    }

    impl MongoDbServiceStorage {
        pub fn new(database: Database, coll_name: &str, encryptor: Encryptor) -> Self {
            Self {
                database,
                coll_name: coll_name.to_string(),
                encryptor,
            }
        }

        fn collection(&self) -> mongodb::Collection<EncryptedService> {
            self.database.collection(&self.coll_name)
        }

        fn decrypt_service(&self, encrypted: &EncryptedService) -> anyhow::Result<Service> {
            let plaintext = self.encryptor.decrypt(&encrypted.nonce, &encrypted.data)?;
            let service: Service = serde_json::from_slice(&plaintext)?;
            Ok(service)
        }

        fn encrypt_service(&mut self, service: &Service) -> anyhow::Result<EncryptedService> {
            let b = serde_json::to_vec(service)?;
            let encrypted = self.encryptor.encrypt(b.as_slice())?;

            let encrypted = EncryptedService {
                name: service.name().to_string(),
                data: encrypted.data,
                nonce: encrypted.nonce,
                updated_at: Some(DateTime::now()),
            };

            Ok(encrypted)
        }
    }

    #[async_trait::async_trait]
    impl ServiceLifecycleStorage for MongoDbServiceStorage {
        async fn save(&mut self, service: Service) -> anyhow::Result<()> {
            let encrypted = self.encrypt_service(&service)?;

            let filter = doc! { "name": service.name() };
            let update = doc! { "$set": bson::to_document(&encrypted)? };

            self.collection()
                .update_one(filter, update)
                .upsert(true)
                .await?;

            Ok(())
        }

        async fn remove(&mut self, service_name: &str) -> anyhow::Result<bool> {
            let filter = doc! { "name": service_name };
            let is_removed = self
                .collection()
                .delete_one(filter)
                .await
                .map(|res| res.deleted_count > 0)?;

            Ok(is_removed)
        }

        async fn get_by_name(&self, service_name: &str) -> anyhow::Result<Service> {
            let filter = doc! { "name": service_name };
            let encrypted =
                self.collection().find_one(filter).await?.ok_or_else(|| {
                    anyhow::anyhow!("service config not found for: {}", service_name)
                })?;

            let decrypted = self.decrypt_service(&encrypted)?;
            Ok(decrypted)
        }

        async fn get_all(&self) -> anyhow::Result<Vec<Service>> {
            let mut cursor = self.collection().find(doc! {}).await?;
            let mut services = Vec::new();

            while cursor.advance().await? {
                let encrypted = cursor.deserialize_current()?;
                let service = self.decrypt_service(&encrypted)?;
                services.push(service);
            }

            Ok(services)
        }
    }
}
