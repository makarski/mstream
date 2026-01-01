use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct SystemConfig {
    pub encryption_key_path: Option<String>,
    // todo: implement
    // pub generate_encryption_key: bool,
    pub job_lifecycle: Option<JobLifecycle>,
    pub service_lifecycle: Option<ServiceLifecycle>,
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
