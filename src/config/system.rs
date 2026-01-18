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
        if let Some(job_lifecycle) = &self.job_lifecycle {
            if job_lifecycle.service_name == service_name {
                components.push("job_lifecycle");
            }
        }
        if let Some(service_lifecycle) = &self.service_lifecycle {
            if service_lifecycle.service_name == service_name {
                components.push("service_lifecycle");
            }
        }
        if let Some(checkpoints) = &self.checkpoints {
            if checkpoints.service_name == service_name {
                components.push("checkpoints");
            }
        }

        if components.is_empty() {
            None
        } else {
            Some(components)
        }
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

    mod has_system_components_tests {
        use super::*;

        #[test]
        fn returns_none_when_service_not_used() {
            let config = SystemConfig {
                encryption_key_path: None,
                job_lifecycle: Some(JobLifecycle {
                    service_name: "system-db".to_string(),
                    resource: "jobs".to_string(),
                    startup_state: StartupState::default(),
                }),
                service_lifecycle: None,
                checkpoints: None,
            };

            assert!(config.has_system_components("other-service").is_none());
        }

        #[test]
        fn returns_job_lifecycle_when_matched() {
            let config = SystemConfig {
                encryption_key_path: None,
                job_lifecycle: Some(JobLifecycle {
                    service_name: "system-db".to_string(),
                    resource: "jobs".to_string(),
                    startup_state: StartupState::default(),
                }),
                service_lifecycle: None,
                checkpoints: None,
            };

            let components = config.has_system_components("system-db");
            assert!(components.is_some());
            assert_eq!(components.unwrap(), vec!["job_lifecycle"]);
        }

        #[test]
        fn returns_service_lifecycle_when_matched() {
            let config = SystemConfig {
                encryption_key_path: None,
                job_lifecycle: None,
                service_lifecycle: Some(ServiceLifecycle {
                    service_name: "system-db".to_string(),
                    resource: "services".to_string(),
                }),
                checkpoints: None,
            };

            let components = config.has_system_components("system-db");
            assert!(components.is_some());
            assert_eq!(components.unwrap(), vec!["service_lifecycle"]);
        }

        #[test]
        fn returns_checkpoints_when_matched() {
            let config = SystemConfig {
                encryption_key_path: None,
                job_lifecycle: None,
                service_lifecycle: None,
                checkpoints: Some(CheckpointSystemConfig {
                    service_name: "checkpoint-db".to_string(),
                    resource: "checkpoints".to_string(),
                }),
            };

            let components = config.has_system_components("checkpoint-db");
            assert!(components.is_some());
            assert_eq!(components.unwrap(), vec!["checkpoints"]);
        }

        #[test]
        fn returns_multiple_components_when_service_used_by_all() {
            let config = SystemConfig {
                encryption_key_path: None,
                job_lifecycle: Some(JobLifecycle {
                    service_name: "system-db".to_string(),
                    resource: "jobs".to_string(),
                    startup_state: StartupState::default(),
                }),
                service_lifecycle: Some(ServiceLifecycle {
                    service_name: "system-db".to_string(),
                    resource: "services".to_string(),
                }),
                checkpoints: Some(CheckpointSystemConfig {
                    service_name: "system-db".to_string(),
                    resource: "checkpoints".to_string(),
                }),
            };

            let components = config.has_system_components("system-db");
            assert!(components.is_some());
            let components = components.unwrap();
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
                encryption_key_path: None,
                job_lifecycle: Some(JobLifecycle {
                    service_name: "jobs-db".to_string(),
                    resource: "jobs".to_string(),
                    startup_state: StartupState::default(),
                }),
                service_lifecycle: Some(ServiceLifecycle {
                    service_name: "services-db".to_string(),
                    resource: "services".to_string(),
                }),
                checkpoints: Some(CheckpointSystemConfig {
                    service_name: "checkpoint-db".to_string(),
                    resource: "checkpoints".to_string(),
                }),
            };

            let jobs_components = config.has_system_components("jobs-db");
            assert_eq!(jobs_components.unwrap(), vec!["job_lifecycle"]);

            let services_components = config.has_system_components("services-db");
            assert_eq!(services_components.unwrap(), vec!["service_lifecycle"]);

            let checkpoint_components = config.has_system_components("checkpoint-db");
            assert_eq!(checkpoint_components.unwrap(), vec!["checkpoints"]);

            assert!(config.has_system_components("unknown-db").is_none());
        }
    }
}
