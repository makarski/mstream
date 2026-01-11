use std::{collections::HashMap, ffi::OsStr, path::Path, sync::Arc};

use anyhow::{Context, Ok, anyhow};
use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use mongodb::Client;
use tokio::fs;
use tracing::info;

use crate::{
    checkpoint::{DynCheckpointer, NoopCheckpointer},
    config::{
        Service,
        service_config::{
            GcpAuthConfig, HttpConfig, MongoDbConfig, PubSubConfig, UdfConfig, UdfEngine, UdfScript,
        },
        system::SystemConfig,
    },
    http,
    middleware::udf::rhai::RhaiMiddleware,
    mongodb::{checkpoint::MongoDbCheckpointer, db_client},
    pubsub::{SCOPES, ServiceAccountAuth, StaticAccessToken},
};

pub mod in_memory;
pub mod mongodb_storage;

type RhaiMiddlewareBuilder =
    Arc<dyn Fn(String) -> anyhow::Result<RhaiMiddleware> + Send + Sync + 'static>;

pub struct ServiceRegistry {
    storage: ServiceStorage,
    system_cfg: Option<SystemConfig>,
    mongo_clients: HashMap<String, Client>,
    gcp_token_providers: HashMap<String, ServiceAccountAuth>,
    http_services: HashMap<String, http::HttpService>,
    udf_middlewares: HashMap<String, RhaiMiddlewareBuilder>,
    checkpointer: DynCheckpointer,
}

impl ServiceRegistry {
    /// Creates a new ServiceRegistry instance
    /// This is a noop constructor, services need to be
    /// initialized via `init` method
    pub fn new(storage: ServiceStorage, system_cfg: Option<SystemConfig>) -> ServiceRegistry {
        Self {
            storage,
            system_cfg,
            mongo_clients: HashMap::new(),
            gcp_token_providers: HashMap::new(),
            http_services: HashMap::new(),
            udf_middlewares: HashMap::new(),
            checkpointer: Arc::new(NoopCheckpointer::new()),
        }
    }

    pub async fn init(&mut self) -> anyhow::Result<()> {
        let seed_services = self.storage.get_all().await?;

        for service in seed_services {
            info!("registering service: {}: {:?}", service.name(), service);
            self.register_service(service).await?;
        }

        self.init_checkpointer().await?;
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

    async fn init_checkpointer(&mut self) -> anyhow::Result<()> {
        let checkpoint_cfg = match self
            .system_cfg
            .as_ref()
            .and_then(|s| s.checkpoints.as_ref())
        {
            Some(cfg) => cfg,
            None => return Ok(()),
        };

        let db_client = self.mongodb_client(&checkpoint_cfg.service_name).await?;
        let db = db_client.database(&checkpoint_cfg.resource);
        let cp = MongoDbCheckpointer::new(db, checkpoint_cfg.resource.clone());

        self.checkpointer = Arc::new(cp);
        Ok(())
    }

    pub async fn register_service(&mut self, service_cfg: Service) -> anyhow::Result<()> {
        match &service_cfg {
            Service::MongoDb(mc) => self.init_mongo(mc.clone()).await?,
            Service::PubSub(ps) => self.init_pubsub(ps.clone()).await?,
            Service::Http(hc) => self.init_http(hc.clone()).await?,
            Service::Udf(uc) => self.init_udf(uc.clone()).await?,
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

    pub fn checkpointer(&self) -> DynCheckpointer {
        self.checkpointer.clone()
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

    /// Check if a service with the given name exists
    pub async fn service_exists(&self, service_name: &str) -> anyhow::Result<bool> {
        Ok(self.storage.get_by_name(service_name).await.is_ok())
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
    let mut dir = fs::read_dir(&script_path).await.with_context(|| {
        anyhow!(
            "failed to read udf script directory: {}",
            script_path.display()
        )
    })?;

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
                content: content,
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

#[cfg(test)]
mod service_registry_tests {
    use super::{ServiceRegistry, ServiceStorage, in_memory::InMemoryServiceStorage};
    use crate::config::{
        Service,
        service_config::{HttpConfig, UdfConfig, UdfEngine, UdfScript},
    };
    use crate::provision::registry::ServiceLifecycleStorage;
    use tempfile::tempdir;

    fn registry_with_in_memory_storage() -> ServiceRegistry {
        let storage: ServiceStorage = Box::new(InMemoryServiceStorage::new());
        ServiceRegistry::new(storage, None)
    }

    #[tokio::test]
    async fn register_http_service_initializes_client_and_persists_definition() {
        let mut registry = registry_with_in_memory_storage();

        let http_service = Service::Http(HttpConfig {
            name: "http-alpha".to_string(),
            host: "http://example.com".to_string(),
            max_retries: Some(2),
            base_backoff_ms: Some(10),
            connection_timeout_sec: Some(5),
            timeout_sec: Some(20),
            tcp_keepalive_sec: Some(30),
        });

        registry
            .register_service(http_service.clone())
            .await
            .expect("failed to register service");

        assert!(
            registry.http_client("http-alpha").await.is_ok(),
            "http client should be initialized"
        );

        let definition = registry
            .service_definition("http-alpha")
            .await
            .expect("service definition should exist");
        assert_eq!("http-alpha", definition.name());
    }

    #[tokio::test]
    async fn remove_service_clears_cached_clients_and_storage() {
        let mut registry = registry_with_in_memory_storage();

        let http_service = Service::Http(HttpConfig {
            name: "http-remove".to_string(),
            host: "http://example.com".to_string(),
            max_retries: None,
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        registry
            .register_service(http_service)
            .await
            .expect("failed to register service");

        registry
            .remove_service("http-remove")
            .await
            .expect("remove should succeed");

        assert!(
            registry.http_client("http-remove").await.is_err(),
            "http client should be removed"
        );
        assert!(
            registry.service_definition("http-remove").await.is_err(),
            "definition should be removed from storage"
        );
    }

    #[tokio::test]
    async fn register_udf_service_writes_scripts_and_rehydrates_sources() {
        let mut registry = registry_with_in_memory_storage();
        let temp_dir = tempdir().expect("failed to create temp dir");
        let script_path = temp_dir.path().join("scripts");
        let script_path_str = script_path.to_string_lossy().to_string();

        let udf_service = Service::Udf(UdfConfig {
            name: "udf-alpha".to_string(),
            script_path: script_path_str,
            engine: UdfEngine::Rhai,
            sources: Some(vec![UdfScript {
                filename: "script.rhai".to_string(),
                content: "fn transform(input, attrs) { result(input, attrs) }".to_string(),
            }]),
        });

        registry
            .register_service(udf_service)
            .await
            .expect("failed to register udf service");

        let definition = registry
            .service_definition("udf-alpha")
            .await
            .expect("udf definition should exist");
        match definition {
            Service::Udf(mut cfg) => {
                let scripts = cfg.sources.take().expect("sources should be rehydrated");
                assert_eq!(1, scripts.len());
                assert_eq!("script.rhai", scripts[0].filename);
                assert_eq!(
                    "fn transform(input, attrs) { result(input, attrs) }",
                    scripts[0].content
                );
            }
            other => panic!("expected udf service, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn register_udf_service_rejects_unsupported_engine() {
        let mut registry = registry_with_in_memory_storage();
        let temp_dir = tempdir().expect("failed to create temp dir");
        let udf_service = Service::Udf(UdfConfig {
            name: "udf-unsupported".to_string(),
            script_path: temp_dir.path().to_string_lossy().to_string(),
            engine: UdfEngine::Undefined,
            sources: Some(vec![UdfScript {
                filename: "script.rhai".to_string(),
                content: "fn noop() {}".to_string(),
            }]),
        });

        let err = registry.register_service(udf_service).await.unwrap_err();
        assert!(
            err.to_string().contains("unsupported udf engine"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn register_udf_service_rejects_invalid_filename() {
        let mut registry = registry_with_in_memory_storage();
        let temp_dir = tempdir().expect("failed to create temp dir");
        let script_path = temp_dir.path().join("scripts");

        let udf_service = Service::Udf(UdfConfig {
            name: "udf-invalid-filename".to_string(),
            script_path: script_path.to_string_lossy().to_string(),
            engine: UdfEngine::Rhai,
            sources: Some(vec![UdfScript {
                filename: "../script.rhai".to_string(),
                content: "fn noop() {}".to_string(),
            }]),
        });

        let err = registry.register_service(udf_service).await.unwrap_err();
        assert!(
            err.to_string().contains("invalid filename"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn register_udf_service_rejects_empty_directory_without_sources() {
        let mut registry = registry_with_in_memory_storage();
        let temp_dir = tempdir().expect("failed to create temp dir");
        let script_path = temp_dir.path().join("empty");

        let udf_service = Service::Udf(UdfConfig {
            name: "udf-empty".to_string(),
            script_path: script_path.to_string_lossy().to_string(),
            engine: UdfEngine::Rhai,
            sources: None,
        });

        let err = registry.register_service(udf_service).await.unwrap_err();
        assert!(
            err.to_string().contains("no udf sources provided"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn service_definition_rehydrates_scripts_after_cold_start() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let scripts_dir = temp_dir.path().join("scripts");
        tokio::fs::create_dir_all(&scripts_dir)
            .await
            .expect("failed to create scripts dir");
        tokio::fs::write(scripts_dir.join("existing.rhai"), "fn cold() { 1 }")
            .await
            .expect("failed to write script");

        let mut storage_impl = InMemoryServiceStorage::new();
        let service = Service::Udf(UdfConfig {
            name: "udf-cold".to_string(),
            script_path: scripts_dir.to_string_lossy().to_string(),
            engine: UdfEngine::Rhai,
            sources: None,
        });
        storage_impl
            .save(service)
            .await
            .expect("failed to persist service");

        let storage: ServiceStorage = Box::new(storage_impl);
        let registry = ServiceRegistry::new(storage, None);

        let definition = registry
            .service_definition("udf-cold")
            .await
            .expect("udf definition should exist after cold start");
        match definition {
            Service::Udf(cfg) => {
                let scripts = cfg.sources.expect("sources should be rehydrated");
                assert_eq!(1, scripts.len());
                assert_eq!("existing.rhai", scripts[0].filename);
                assert_eq!("fn cold() { 1 }", scripts[0].content);
            }
            other => panic!("expected udf service, got {:?}", other),
        }
    }
}
