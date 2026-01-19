use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct SystemConfig {
    pub encryption_key_path: Option<String>,
    // todo: implement
    // pub generate_encryption_key: bool,
    pub job_lifecycle: Option<JobLifecycle>,
    pub service_lifecycle: Option<ServiceLifecycle>,
    pub checkpoints: Option<CheckpointSystemConfig>,
}

impl SystemConfig {
    pub fn has_system_components(&self, service_name: &str) -> Option<Vec<&'static str>> {
        let mut components = Vec::new();
        if self.has_job_lifecycle(service_name) {
            components.push("job_lifecycle");
        }

        if self.has_service_lifecycle(service_name) {
            components.push("service_lifecycle");
        }

        if self.has_checkpoints(service_name) {
            components.push("checkpoints");
        }

        if components.is_empty() {
            None
        } else {
            Some(components)
        }
    }

    fn has_job_lifecycle(&self, service_name: &str) -> bool {
        self.job_lifecycle
            .as_ref()
            .filter(|jlc| -> bool { jlc.service_name == service_name })
            .is_some()
    }

    fn has_service_lifecycle(&self, service_name: &str) -> bool {
        self.service_lifecycle
            .as_ref()
            .filter(|slc| -> bool { slc.service_name == service_name })
            .is_some()
    }

    fn has_checkpoints(&self, service_name: &str) -> bool {
        self.checkpoints
            .as_ref()
            .filter(|cp| -> bool { cp.service_name == service_name })
            .is_some()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct JobLifecycle {
    pub service_name: String,
    pub resource: String,
    #[serde(default)]
    pub startup_state: StartupState,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ServiceLifecycle {
    pub service_name: String,
    pub resource: String,
}

#[derive(Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum StartupState {
    #[default]
    SeedFromFile,
    ForceFromFile,
    Keep,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CheckpointSystemConfig {
    pub service_name: String,
    pub resource: String,
}

#[cfg(test)]
mod tests {
    use super::*;

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

    mod has_system_components_tests {
        use super::*;

        #[test]
        fn returns_none_when_service_not_used() {
            let config = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                ..Default::default()
            };

            assert!(config.has_system_components("other-service").is_none());
        }

        #[test]
        fn returns_job_lifecycle_when_matched() {
            let config = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                ..Default::default()
            };

            let components = config.has_system_components("system-db");
            assert_eq!(components.unwrap(), vec!["job_lifecycle"]);
        }

        #[test]
        fn returns_service_lifecycle_when_matched() {
            let config = SystemConfig {
                service_lifecycle: Some(service_lifecycle("system-db")),
                ..Default::default()
            };

            let components = config.has_system_components("system-db");
            assert_eq!(components.unwrap(), vec!["service_lifecycle"]);
        }

        #[test]
        fn returns_checkpoints_when_matched() {
            let config = SystemConfig {
                checkpoints: Some(checkpoints("checkpoint-db")),
                ..Default::default()
            };

            let components = config.has_system_components("checkpoint-db");
            assert_eq!(components.unwrap(), vec!["checkpoints"]);
        }

        #[test]
        fn returns_multiple_components_when_service_used_by_all() {
            let config = SystemConfig {
                job_lifecycle: Some(job_lifecycle("system-db")),
                service_lifecycle: Some(service_lifecycle("system-db")),
                checkpoints: Some(checkpoints("system-db")),
                ..Default::default()
            };

            let components = config.has_system_components("system-db").unwrap();
            assert_eq!(components.len(), 3);
            assert!(components.contains(&"job_lifecycle"));
            assert!(components.contains(&"service_lifecycle"));
            assert!(components.contains(&"checkpoints"));
        }

        #[test]
        fn returns_none_when_all_components_are_none() {
            let config = SystemConfig::default();
            assert!(config.has_system_components("any-service").is_none());
        }

        #[test]
        fn returns_partial_match_when_different_services_configured() {
            let config = SystemConfig {
                job_lifecycle: Some(job_lifecycle("jobs-db")),
                service_lifecycle: Some(service_lifecycle("services-db")),
                checkpoints: Some(checkpoints("checkpoint-db")),
                ..Default::default()
            };

            assert_eq!(
                config.has_system_components("jobs-db").unwrap(),
                vec!["job_lifecycle"]
            );
            assert_eq!(
                config.has_system_components("services-db").unwrap(),
                vec!["service_lifecycle"]
            );
            assert_eq!(
                config.has_system_components("checkpoint-db").unwrap(),
                vec!["checkpoints"]
            );
            assert!(config.has_system_components("unknown-db").is_none());
        }
    }
}
