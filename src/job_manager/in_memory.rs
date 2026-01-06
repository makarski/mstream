use std::collections::{HashMap, HashSet};

use crate::job_manager::JobMetadata;

pub struct InMemoryJobStore {
    jobs: HashMap<String, JobMetadata>,
    jobs_by_service: HashMap<String, HashSet<String>>,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            jobs_by_service: HashMap::new(),
        }
    }

    fn add_deps(&mut self, job_name: &str, deps: &[String]) {
        for service_name in deps {
            self.jobs_by_service
                .entry(service_name.clone())
                .or_insert_with(HashSet::new)
                .insert(job_name.to_string());
        }
    }

    fn clear_deps(&mut self, job_name: &str) {
        let job_deps = self
            .jobs
            .get(job_name)
            .map(|metadata| metadata.service_deps.clone())
            .unwrap_or_default();

        for service_name in job_deps {
            if let Some(jobs) = self.jobs_by_service.get_mut(&service_name) {
                jobs.remove(job_name);

                if jobs.is_empty() {
                    self.jobs_by_service.remove(&service_name);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl super::JobLifecycleStorage for InMemoryJobStore {
    async fn list_all(&self) -> anyhow::Result<Vec<JobMetadata>> {
        Ok(self.jobs.values().cloned().collect())
    }

    async fn save(&mut self, metadata: JobMetadata) -> anyhow::Result<()> {
        if let Some(existing) = self.jobs.get(&metadata.name) {
            if existing.service_deps != metadata.service_deps {
                self.clear_deps(&metadata.name);
            }
        }

        if !metadata.service_deps.is_empty() {
            self.add_deps(&metadata.name, &metadata.service_deps);
        }

        let name = metadata.name.clone();
        self.jobs.insert(name, metadata);
        Ok(())
    }

    async fn get(&self, name: &str) -> anyhow::Result<Option<JobMetadata>> {
        Ok(self.jobs.get(name).cloned())
    }

    async fn get_dependent_jobs(&self, service_name: &str) -> anyhow::Result<Vec<String>> {
        let names = if let Some(job_names) = self.jobs_by_service.get(service_name) {
            job_names.iter().cloned().collect()
        } else {
            Vec::new()
        };

        Ok(names)
    }
}
