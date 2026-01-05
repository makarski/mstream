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

        let encrypted = self.encrypt_service(&service_to_save)?;

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
        let encrypted = self
            .collection()
            .find_one(filter)
            .await?
            .ok_or_else(|| anyhow::anyhow!("service config not found for: {}", service_name))?;

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
