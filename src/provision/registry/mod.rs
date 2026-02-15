use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{Context, anyhow};
use gauth::{serv_account::ServiceAccount, token_provider::AsyncTokenProvider};
use mongodb::Client;
use tracing::info;

use crate::{
    checkpoint::{DynCheckpointer, NoopCheckpointer},
    config::{
        Service,
        service_config::{
            GcpAuthConfig, HttpConfig, MongoDbConfig, PubSubConfig, UdfConfig, UdfEngine,
        },
        system::SystemConfig,
    },
    http,
    middleware::udf::rhai::RhaiMiddleware,
    mongodb::{checkpoint::MongoDbCheckpointer, db_client, test_suite::MongoDbTestSuiteStore},
    pubsub::srvc::SchemaService,
    pubsub::{SCOPES, ServiceAccountAuth, StaticAccessToken},
    schema::{DynSchemaRegistry, NoopSchemaRegistry, SchemaProvider, mongo::MongoDbSchemaProvider},
    testing::{DynTestSuiteStore, NoopTestSuiteStore},
};

pub mod in_memory;
pub mod mongodb_storage;
mod udf;

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
    schema_registry: DynSchemaRegistry,
    test_suite_store: DynTestSuiteStore,
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
            schema_registry: Arc::new(NoopSchemaRegistry),
            test_suite_store: Arc::new(NoopTestSuiteStore),
        }
    }

    pub async fn init(&mut self) -> anyhow::Result<()> {
        let seed_services = self.storage.get_all().await?;

        for service in seed_services {
            info!("registering service: {}: {:?}", service.name(), service);
            self.register_service(service).await?;
        }

        self.init_mongo_stores().await?;
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

        udf::create_udf_script(&cfg).await?;

        let script_path = cfg.script_path.clone();
        let callback = move |filename: String| -> anyhow::Result<RhaiMiddleware> {
            Ok(RhaiMiddleware::new(script_path.clone(), filename)?)
        };

        let udf_builder = Arc::new(callback);
        self.udf_middlewares.insert(cfg.name.clone(), udf_builder);
        Ok(())
    }

    async fn mongo_database_for(
        &self,
        service_name: &str,
        component: &str,
    ) -> anyhow::Result<mongodb::Database> {
        let service_def = self.storage.get_by_name(service_name).await?;

        let db_name = match &service_def {
            Service::MongoDb(cfg) => &cfg.db_name,
            _ => {
                return Err(anyhow!(
                    "{} service '{}' must be a MongoDB service",
                    component,
                    service_name
                ));
            }
        };

        let client = self.mongodb_client(service_name).await?;
        Ok(client.database(db_name))
    }

    async fn init_mongo_stores(&mut self) -> anyhow::Result<()> {
        let sys_cfg = match &self.system_cfg {
            Some(cfg) => cfg,
            None => return Ok(()),
        };

        if let Some(cfg) = &sys_cfg.checkpoints {
            let db = self
                .mongo_database_for(&cfg.service_name, "checkpoints")
                .await?;
            self.checkpointer = Arc::new(MongoDbCheckpointer::new(db, cfg.resource.clone()));
        }

        if let Some(cfg) = &sys_cfg.schemas {
            let db = self
                .mongo_database_for(&cfg.service_name, "schemas")
                .await?;
            self.schema_registry = Arc::new(MongoDbSchemaProvider::new(db, cfg.resource.clone()));
        }

        if let Some(cfg) = &sys_cfg.test_suites {
            let db = self
                .mongo_database_for(&cfg.service_name, "test_suites")
                .await?;
            self.test_suite_store = Arc::new(MongoDbTestSuiteStore::new(db, cfg.resource.clone()));
        }

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

        let mut to_save = service_cfg;
        self.enrich_udf_scripts(&mut to_save).await?;
        self.storage.save(to_save).await?;

        Ok(())
    }

    pub async fn update_udf_resource(
        &mut self,
        service_name: &str,
        filename: &str,
        content: &str,
    ) -> anyhow::Result<()> {
        udf::validate_script_filename(filename)?;

        let mut service = self.storage.get_by_name(service_name).await?;

        let udf_config = match &mut service {
            Service::Udf(cfg) => cfg,
            _ => anyhow::bail!("service '{}' is not a UDF service", service_name),
        };

        let script_path = Path::new(&udf_config.script_path);
        let file_path = script_path.join(filename);
        if !file_path.exists() {
            anyhow::bail!(
                "resource '{}' not found in service '{}'",
                filename,
                service_name
            );
        }

        tokio::fs::write(&file_path, content)
            .await
            .with_context(|| {
                anyhow!(
                    "failed to write resource '{}' for service '{}'",
                    filename,
                    service_name
                )
            })?;

        let scripts = udf::read_udf_scripts(script_path).await?;
        udf_config.sources = Some(scripts);
        self.storage.save(service).await?;

        Ok(())
    }

    /// Returns true if the service is used by system configuration
    pub fn is_system_service(&self, service_name: &str) -> bool {
        self.system_cfg
            .as_ref()
            .map(|cfg| cfg.has_system_components(service_name).is_some())
            .unwrap_or(false)
    }

    pub async fn remove_service(&mut self, service_name: &str) -> anyhow::Result<()> {
        if let Some(system_cfg) = &self.system_cfg {
            if let Some(components) = system_cfg.has_system_components(service_name) {
                anyhow::bail!(
                    "cannot remove service '{}' as it is used by system components: {:?}",
                    service_name,
                    components
                );
            }
        }

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

    fn lookup<T: Clone>(map: &HashMap<String, T>, name: &str, label: &str) -> anyhow::Result<T> {
        map.get(name)
            .cloned()
            .ok_or_else(|| anyhow!("{} not found for service name: {}", label, name))
    }

    pub async fn udf_middleware(&self, name: &str) -> anyhow::Result<RhaiMiddlewareBuilder> {
        Self::lookup(&self.udf_middlewares, name, "udf middleware builder")
    }

    pub async fn mongodb_client(&self, name: &str) -> anyhow::Result<Client> {
        Self::lookup(&self.mongo_clients, name, "mongodb client")
    }

    pub async fn gcp_auth(&self, name: &str) -> anyhow::Result<ServiceAccountAuth> {
        Self::lookup(&self.gcp_token_providers, name, "gcp token provider")
    }

    pub async fn http_client(&self, name: &str) -> anyhow::Result<http::HttpService> {
        Self::lookup(&self.http_services, name, "http service")
    }

    pub fn checkpointer(&self) -> DynCheckpointer {
        self.checkpointer.clone()
    }

    pub fn schema_registry(&self) -> DynSchemaRegistry {
        self.schema_registry.clone()
    }

    pub async fn schema_registry_for(
        &self,
        service_name: &str,
    ) -> anyhow::Result<DynSchemaRegistry> {
        let service = self.storage.get_by_name(service_name).await?;
        match service {
            Service::PubSub(cfg) => {
                let tp = self.gcp_auth(&cfg.name).await?;
                let svc = SchemaService::with_interceptor(tp.clone()).await?;
                Ok(Arc::new(SchemaProvider::PubSub {
                    service: svc,
                    project_id: cfg.project_id.clone(),
                }))
            }
            Service::MongoDb(cfg) => {
                let collection_name = self
                    .system_cfg
                    .as_ref()
                    .and_then(|sys| sys.schemas.as_ref())
                    .map(|s| s.resource.clone())
                    .unwrap_or_else(|| "schemas".to_string());
                let client = self.mongodb_client(&cfg.name).await?;
                let db = client.database(&cfg.db_name);
                Ok(Arc::new(SchemaProvider::MongoDb(
                    MongoDbSchemaProvider::new(db, collection_name),
                )))
            }
            _ => Err(anyhow!(
                "service '{}' does not support schema operations",
                service_name
            )),
        }
    }

    pub fn test_suite_store(&self) -> DynTestSuiteStore {
        self.test_suite_store.clone()
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
            let scripts = udf::read_udf_scripts(Path::new(&udf_config.script_path)).await?;
            udf_config.sources = Some(scripts);
        }
        Ok(())
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

#[cfg(test)]
mod service_registry_tests {
    use super::{ServiceRegistry, ServiceStorage, in_memory::InMemoryServiceStorage};
    use crate::config::{
        Service,
        service_config::{HttpConfig, UdfConfig, UdfEngine, UdfScript},
        system::{
            CheckpointSystemConfig, JobLifecycle, ServiceLifecycle, StartupState, SystemConfig,
        },
    };
    use crate::provision::registry::ServiceLifecycleStorage;
    use tempfile::tempdir;

    fn registry_with_in_memory_storage() -> ServiceRegistry {
        let storage: ServiceStorage = Box::new(InMemoryServiceStorage::new());
        ServiceRegistry::new(storage, None)
    }

    fn registry_with_system_config(system_cfg: SystemConfig) -> ServiceRegistry {
        let storage: ServiceStorage = Box::new(InMemoryServiceStorage::new());
        ServiceRegistry::new(storage, Some(system_cfg))
    }

    fn job_lifecycle(service_name: &str) -> JobLifecycle {
        JobLifecycle {
            service_name: service_name.to_string(),
            resource: "jobs".to_string(),
            startup_state: StartupState::default(),
        }
    }

    fn service_lifecycle(service_name: &str) -> ServiceLifecycle {
        ServiceLifecycle {
            service_name: service_name.to_string(),
            resource: "services".to_string(),
        }
    }

    fn checkpoints(service_name: &str) -> CheckpointSystemConfig {
        CheckpointSystemConfig {
            service_name: service_name.to_string(),
            resource: "checkpoints".to_string(),
        }
    }

    fn http_service(name: &str) -> Service {
        Service::Http(HttpConfig {
            name: name.to_string(),
            host: "http://example.com".to_string(),
            max_retries: None,
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        })
    }

    fn udf_service(
        name: &str,
        script_path: &str,
        engine: UdfEngine,
        sources: Option<Vec<UdfScript>>,
    ) -> Service {
        Service::Udf(UdfConfig {
            name: name.to_string(),
            script_path: script_path.to_string(),
            engine,
            sources,
        })
    }

    fn udf_script(filename: &str, content: &str) -> UdfScript {
        UdfScript {
            filename: filename.to_string(),
            content: content.to_string(),
        }
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

        registry
            .register_service(http_service("http-remove"))
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

        registry
            .register_service(udf_service(
                "udf-alpha",
                &script_path.to_string_lossy(),
                UdfEngine::Rhai,
                Some(vec![udf_script(
                    "script.rhai",
                    "fn transform(input, attrs) { result(input, attrs) }",
                )]),
            ))
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
    async fn register_udf_service_rejects_invalid_configs() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let base_path = temp_dir.path();

        let test_cases = [
            (
                udf_service(
                    "udf-unsupported",
                    &base_path.to_string_lossy(),
                    UdfEngine::Undefined,
                    Some(vec![udf_script("script.rhai", "fn noop() {}")]),
                ),
                "unsupported udf engine",
            ),
            (
                udf_service(
                    "udf-invalid-filename",
                    &base_path.join("scripts").to_string_lossy(),
                    UdfEngine::Rhai,
                    Some(vec![udf_script("../script.rhai", "fn noop() {}")]),
                ),
                "invalid script filename",
            ),
            (
                udf_service(
                    "udf-empty",
                    &base_path.join("empty").to_string_lossy(),
                    UdfEngine::Rhai,
                    None,
                ),
                "no udf sources provided",
            ),
        ];

        for (service, expected_error) in test_cases {
            let mut registry = registry_with_in_memory_storage();
            let err = registry.register_service(service).await.unwrap_err();
            assert!(
                err.to_string().contains(expected_error),
                "expected error containing '{}', got: {}",
                expected_error,
                err
            );
        }
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
        storage_impl
            .save(udf_service(
                "udf-cold",
                &scripts_dir.to_string_lossy(),
                UdfEngine::Rhai,
                None,
            ))
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

    mod is_system_service_tests {
        use super::*;

        #[test]
        fn returns_false_when_no_system_config() {
            let registry = registry_with_in_memory_storage();
            assert!(!registry.is_system_service("any-service"));
        }

        #[test]
        fn returns_false_when_service_not_in_system_config() {
            let system_cfg = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                ..Default::default()
            };
            let registry = registry_with_system_config(system_cfg);

            assert!(!registry.is_system_service("other-service"));
        }

        #[test]
        fn returns_true_when_service_used_by_job_lifecycle() {
            let system_cfg = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                ..Default::default()
            };
            let registry = registry_with_system_config(system_cfg);

            assert!(registry.is_system_service("system-db"));
        }

        #[test]
        fn returns_true_when_service_used_by_checkpoints() {
            let system_cfg = SystemConfig {
                checkpoints: Some(checkpoints("checkpoint-db")),
                ..Default::default()
            };
            let registry = registry_with_system_config(system_cfg);

            assert!(registry.is_system_service("checkpoint-db"));
        }

        #[test]
        fn returns_true_when_service_used_by_multiple_components() {
            let system_cfg = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                service_lifecycle: Some(service_lifecycle("system-db")),
                checkpoints: Some(checkpoints("system-db")),
                ..Default::default()
            };
            let registry = registry_with_system_config(system_cfg);

            assert!(registry.is_system_service("system-db"));
        }
    }

    mod remove_system_service_tests {
        use super::*;

        #[tokio::test]
        async fn remove_service_respects_system_config() {
            let system_cfg = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                service_lifecycle: Some(service_lifecycle("system-db")),
                ..Default::default()
            };
            let mut registry = registry_with_system_config(system_cfg);

            // Register both system and non-system services
            for name in ["system-db", "user-service"] {
                registry
                    .register_service(http_service(name))
                    .await
                    .expect("failed to register service");
            }

            // System service removal should fail with component names in error
            let err_msg = registry
                .remove_service("system-db")
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err_msg.contains("cannot remove service")
                    && err_msg.contains("job_lifecycle")
                    && err_msg.contains("service_lifecycle"),
                "expected system components in error, got: {}",
                err_msg
            );

            // Non-system service removal should succeed
            registry
                .remove_service("user-service")
                .await
                .expect("should remove non-system service");
            assert!(registry.service_definition("user-service").await.is_err());
        }
    }
}
