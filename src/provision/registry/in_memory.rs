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

#[cfg(test)]
mod inmemory_service_storage_tests {
    use crate::{
        config::{
            Service,
            service_config::{HttpConfig, UdfConfig, UdfEngine, UdfScript},
        },
        provision::registry::{ServiceLifecycleStorage, in_memory::InMemoryServiceStorage},
    };

    #[tokio::test]
    async fn save_overwrites_existing_service() {
        let mut storage = InMemoryServiceStorage::new();

        let original = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://one".to_string(),
            max_retries: Some(1),
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        let updated = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://two".to_string(),
            max_retries: Some(3),
            base_backoff_ms: Some(25),
            connection_timeout_sec: Some(5),
            timeout_sec: Some(30),
            tcp_keepalive_sec: Some(15),
        });

        storage.save(original).await.unwrap();
        storage.save(updated).await.unwrap();

        let fetched = storage.get_by_name("alpha").await.unwrap();
        match fetched {
            Service::Http(cfg) => {
                assert_eq!("http://two", cfg.host);
                assert_eq!(Some(3), cfg.max_retries);
                assert_eq!(Some(25), cfg.base_backoff_ms);
            }
            other => panic!("expected http service, got {:?}", other),
        }

        let services = storage.get_all().await.unwrap();
        assert_eq!(1, services.len());
    }

    #[tokio::test]
    async fn save_strips_udf_sources_before_persisting() {
        let mut storage = InMemoryServiceStorage::new();

        let service = Service::Udf(UdfConfig {
            name: "udf-service".to_string(),
            script_path: "/tmp/udf".to_string(),
            engine: UdfEngine::Rhai,
            sources: Some(vec![UdfScript {
                filename: "script.rhai".to_string(),
                content: "fn run() {}".to_string(),
            }]),
        });

        storage.save(service.clone()).await.unwrap();

        let saved = storage.get_by_name("udf-service").await.unwrap();
        match saved {
            Service::Udf(cfg) => {
                assert!(cfg.sources.is_none());
                assert_eq!("/tmp/udf", cfg.script_path);
            }
            other => panic!("expected udf service, got {:?}", other),
        }

        if let Service::Udf(cfg) = service {
            assert!(cfg.sources.is_some());
        } else {
            panic!("expected udf service");
        }
    }

    #[tokio::test]
    async fn remove_returns_true_and_deletes_service() {
        let mut storage = InMemoryServiceStorage::new();

        let service = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://one".to_string(),
            max_retries: None,
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        storage.save(service).await.unwrap();

        let removed = storage.remove("alpha").await.unwrap();
        assert!(removed);
        assert!(storage.get_by_name("alpha").await.is_err());
    }

    #[tokio::test]
    async fn remove_returns_false_when_service_missing() {
        let mut storage = InMemoryServiceStorage::new();

        let service = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://one".to_string(),
            max_retries: None,
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        storage.save(service).await.unwrap();

        let removed = storage.remove("beta").await.unwrap();
        assert!(!removed);
        assert!(storage.get_by_name("alpha").await.is_ok());
    }

    #[tokio::test]
    async fn get_by_name_returns_cloned_service() {
        let mut storage = InMemoryServiceStorage::new();

        let service = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://one".to_string(),
            max_retries: Some(2),
            base_backoff_ms: Some(10),
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        storage.save(service).await.unwrap();

        let fetched = storage.get_by_name("alpha").await.unwrap();
        match fetched {
            Service::Http(cfg) => {
                assert_eq!("http://one", cfg.host);
                assert_eq!(Some(2), cfg.max_retries);
                assert_eq!(Some(10), cfg.base_backoff_ms);
            }
            other => panic!("expected http service, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_by_name_returns_error_when_missing() {
        let storage = InMemoryServiceStorage::new();

        let result = storage.get_by_name("missing").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_all_returns_all_services() {
        let mut storage = InMemoryServiceStorage::new();

        let http = Service::Http(HttpConfig {
            name: "alpha".to_string(),
            host: "http://one".to_string(),
            max_retries: None,
            base_backoff_ms: None,
            connection_timeout_sec: None,
            timeout_sec: None,
            tcp_keepalive_sec: None,
        });

        let udf = Service::Udf(UdfConfig {
            name: "udf-service".to_string(),
            script_path: "/tmp/udf".to_string(),
            engine: UdfEngine::Rhai,
            sources: Some(vec![UdfScript {
                filename: "script.rhai".to_string(),
                content: "fn run() {}".to_string(),
            }]),
        });

        storage.save(http).await.unwrap();
        storage.save(udf).await.unwrap();

        let mut names: Vec<_> = storage
            .get_all()
            .await
            .unwrap()
            .into_iter()
            .map(|service| {
                let name = service.name().to_string();
                if let Service::Udf(cfg) = &service {
                    assert!(cfg.sources.is_none());
                }
                name
            })
            .collect();

        names.sort();
        assert_eq!(vec!["alpha".to_string(), "udf-service".to_string()], names);
    }
}
