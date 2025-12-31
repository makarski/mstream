use std::{collections::HashMap, ffi::OsStr, path::Path, sync::Arc};

use anyhow::{Context, anyhow};
use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use mongodb::Client;
use tokio::fs;

use crate::{
    config::{
        Service,
        service_config::{
            GcpAuthConfig, HttpConfig, MongoDbConfig, PubSubConfig, UdfConfig, UdfEngine, UdfScript,
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
    mongo_clients: HashMap<String, Client>,
    gcp_token_providers: HashMap<String, ServiceAccountAuth>,
    http_services: HashMap<String, http::HttpService>,
    udf_middlewares: HashMap<String, RhaiMiddlewareBuilder>,
}

impl ServiceRegistry {
    /// Creates a new ServiceRegistry instance
    /// This is a noop constructor, services need to be
    /// initialized via `init` method
    pub fn new(storage: ServiceStorage) -> ServiceRegistry {
        Self {
            storage,
            mongo_clients: HashMap::new(),
            gcp_token_providers: HashMap::new(),
            http_services: HashMap::new(),
            udf_middlewares: HashMap::new(),
        }
    }

    pub async fn init(&mut self, seed_services: Vec<Service>) -> anyhow::Result<()> {
        for service in seed_services {
            self.register_service(service.clone()).await?;
        }
        Ok(())
    }

    async fn init_mongo(&mut self, cfg: MongoDbConfig) -> anyhow::Result<()> {
        let client = db_client(cfg.name.to_owned(), &cfg.connection_string).await?;
        self.mongo_clients.insert(cfg.name.clone(), client);
        Ok(())
    }

    async fn init_pubsub(&mut self, ps_cfg: PubSubConfig) -> anyhow::Result<()> {
        let tp = Self::create_gcp_token_provider(&ps_cfg.auth).await?;
        self.gcp_token_providers.insert(ps_cfg.name.clone(), tp);
        Ok(())
    }

    async fn init_http(&mut self, cfg: HttpConfig) -> anyhow::Result<()> {
        let http_service = http::HttpService::new(
            cfg.host.clone(),
            cfg.max_retries,
            cfg.base_backoff_ms,
            cfg.connection_timeout_sec,
            cfg.timeout_sec,
            cfg.tcp_keepalive_sec,
        )
        .with_context(|| anyhow!("failed to initialize http service for: {}", cfg.name))?;
        self.http_services.insert(cfg.name.clone(), http_service);
        Ok(())
    }

    async fn init_udf(&mut self, cfg: UdfConfig) -> anyhow::Result<()> {
        if !matches!(cfg.engine, UdfEngine::Rhai) {
            anyhow::bail!(
                "unsupported udf engine: {:?} for service: {}",
                cfg.engine,
                cfg.name
            );
        }

        create_udf_script(&cfg).await?;

        let script_path = cfg.script_path.clone();
        let callback = move |filename: String| -> anyhow::Result<RhaiMiddleware> {
            Ok(RhaiMiddleware::new(script_path.clone(), filename)?)
        };

        let udf_builder = Arc::new(callback);
        self.udf_middlewares.insert(cfg.name.clone(), udf_builder);
        Ok(())
    }

    pub async fn register_service(&mut self, service_cfg: Service) -> anyhow::Result<()> {
        match service_cfg.clone() {
            Service::MongoDb(mc) => self.init_mongo(mc).await?,
            Service::PubSub(ps) => self.init_pubsub(ps).await?,
            Service::Http(hc) => self.init_http(hc).await?,
            Service::Udf(uc) => self.init_udf(uc).await?,
            _ => {}
        }

        self.storage.save(service_cfg).await?;

        Ok(())
    }

    pub async fn remove_service(&mut self, service_name: &str) -> anyhow::Result<()> {
        let config_removed = self.storage.remove(service_name).await?;
        if config_removed {
            self.mongo_clients.remove(service_name);
            self.gcp_token_providers.remove(service_name);
            self.http_services.remove(service_name);
            self.udf_middlewares.remove(service_name);
            Ok(())
        } else {
            Err(anyhow!("service with name '{}' not found", service_name))
        }
    }

    pub async fn udf_middleware(&self, name: &str) -> anyhow::Result<RhaiMiddlewareBuilder> {
        self.udf_middlewares.get(name).cloned().ok_or_else(|| {
            anyhow!(
                "udf middleware builder not found for service name: {}",
                name
            )
        })
    }

    pub async fn mongodb_client(&self, name: &str) -> anyhow::Result<Client> {
        self.mongo_clients
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("mongodb client not found for service name: {}", name))
    }

    pub async fn gcp_auth(&self, name: &str) -> anyhow::Result<ServiceAccountAuth> {
        self.gcp_token_providers
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("gcp token provider not found for service name: {}", name))
    }

    pub async fn http_client(&self, name: &str) -> anyhow::Result<http::HttpService> {
        self.http_services
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
        let mut def = self.storage.get_by_name(service_name).await?;
        self.enrich_udf_scripts(&mut def).await?;

        Ok(def)
    }

    pub async fn all_service_definitions(&self) -> anyhow::Result<Vec<Service>> {
        let mut definitions = self.storage.get_all().await?;

        for definition in definitions.iter_mut() {
            self.enrich_udf_scripts(definition).await?;
        }

        Ok(definitions)
    }

    async fn enrich_udf_scripts(&self, service: &mut Service) -> anyhow::Result<()> {
        if let Service::Udf(udf_config) = service {
            let scripts = read_udf_scripts(Path::new(&udf_config.script_path)).await?;
            udf_config.sources = Some(scripts);
        }
        Ok(())
    }
}

async fn create_udf_script(cfg: &UdfConfig) -> anyhow::Result<()> {
    let base_path = Path::new(&cfg.script_path);

    let sources = match &cfg.sources {
        Some(s) => s,
        None => &vec![],
    };

    if sources.is_empty()
        && (!base_path.exists() || fs::read_dir(base_path).await?.next_entry().await?.is_none())
    {
        anyhow::bail!(
            "no udf sources provided and udf script dir is empty or does not exist for service: {}. script_path: {}",
            cfg.name,
            base_path.display()
        );
    }

    if !base_path.exists() {
        fs::create_dir_all(base_path).await.with_context(|| {
            anyhow!(
                "failed to create an udf script dir: {}",
                base_path.display()
            )
        })?;
    }

    for source in sources {
        let filename = &source.filename;
        if filename.contains("..") || filename.contains('/') || filename.contains('\\') {
            anyhow::bail!(
                "invalid filename '{}' in udf source for service '{}'",
                filename,
                cfg.name
            );
        }

        let script_path = base_path.join(&source.filename);
        tokio::fs::write(&script_path, &source.content)
            .await
            .with_context(|| {
                anyhow!("failed to write udf script file: {}", script_path.display())
            })?;
    }

    Ok(())
}

async fn read_udf_scripts(script_path: &Path) -> anyhow::Result<Vec<UdfScript>> {
    let mut dir = fs::read_dir(&script_path)
        .await
        .with_context(|| anyhow!("failed to read udf script file: {}", script_path.display()))?;

    let mut scripts = Vec::new();

    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension() == Some(OsStr::new("rhai")) {
            let content = fs::read_to_string(&path)
                .await
                .with_context(|| anyhow!("failed to read udf script file: {}", path.display()))?;

            let filename = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                anyhow!("failed to read filename from a script: {}", path.display())
            })?;

            let script = UdfScript {
                filename: filename.to_string(),
                content: content.clone(),
            };

            scripts.push(script);
        }
    }

    Ok(scripts)
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

    use anyhow::anyhow;

    #[derive(Default)]
    pub struct InMemoryServiceStorage {
        active_services: Vec<Service>,
    }

    impl InMemoryServiceStorage {
        pub fn new() -> Self {
            Self::default()
        }

        fn service_by_name(&self, name: &str) -> Option<&Service> {
            self.active_services.iter().find(|s| s.name() == name)
        }
    }

    #[async_trait::async_trait]
    impl ServiceLifecycleStorage for InMemoryServiceStorage {
        async fn save(&mut self, service: Service) -> anyhow::Result<()> {
            let mut service_to_save = service.clone();

            if let Service::Udf(ref mut udf_config) = service_to_save {
                udf_config.sources = None;
            }

            // overwrite existing service config if present
            if let Some(existing) = self
                .active_services
                .iter_mut()
                .find(|s| s.name() == service.name())
            {
                *existing = service_to_save;
            } else {
                self.active_services.push(service_to_save);
            }
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

        fn encrypt_service(&self, service: &Service) -> anyhow::Result<EncryptedService> {
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
            let mut service_to_save = service.clone();
            if let Service::Udf(ref mut udf_config) = service_to_save {
                udf_config.sources = None;
            }

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
