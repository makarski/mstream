use mongodb::bson;
use serde::{Deserialize, Serialize};

pub mod consumer;
pub mod producer;

#[derive(Serialize, Deserialize)]
pub struct KafkaOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl TryInto<Vec<u8>> for KafkaOffset {
    type Error = bson::ser::Error;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        bson::to_vec(&self)
    }
}
