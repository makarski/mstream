use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct SystemConfig {
    pub job_lifecycles: Option<JobLifecycle>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct JobLifecycle {
    pub service_name: String,
    pub resource: String,
    #[serde(default)]
    pub startup_state: StartupState,
}

#[derive(Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum StartupState {
    #[default]
    SeedFromFile,
    ForceFromFile,
    Keep,
}
