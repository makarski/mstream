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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_offset_json_serialization_roundtrip() {
        let offset = KafkaOffset {
            topic: "events".to_string(),
            partition: 3,
            offset: 12345,
        };

        let serialized = serde_json::to_string(&offset).expect("json serialize");
        let deserialized: KafkaOffset =
            serde_json::from_str(&serialized).expect("json deserialize");

        assert_eq!(deserialized.topic, "events");
        assert_eq!(deserialized.partition, 3);
        assert_eq!(deserialized.offset, 12345);
    }

    #[test]
    fn kafka_offset_bson_serialization_roundtrip() {
        let offset = KafkaOffset {
            topic: "user-actions".to_string(),
            partition: 0,
            offset: 999999,
        };

        let serialized = bson::to_vec(&offset).expect("bson serialize");
        let deserialized: KafkaOffset = bson::from_slice(&serialized).expect("bson deserialize");

        assert_eq!(deserialized.topic, "user-actions");
        assert_eq!(deserialized.partition, 0);
        assert_eq!(deserialized.offset, 999999);
    }

    #[test]
    fn kafka_offset_try_into_vec_u8() {
        let offset = KafkaOffset {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 42,
        };

        let bytes: Vec<u8> = offset.try_into().expect("try_into Vec<u8>");
        assert!(!bytes.is_empty());

        // Verify we can deserialize from the bytes
        let deserialized: KafkaOffset = bson::from_slice(&bytes).expect("deserialize from bytes");
        assert_eq!(deserialized.topic, "test-topic");
        assert_eq!(deserialized.partition, 1);
        assert_eq!(deserialized.offset, 42);
    }

    #[test]
    fn kafka_offset_handles_negative_offset() {
        // Kafka uses -1 for special offsets like OFFSET_END
        let offset = KafkaOffset {
            topic: "special".to_string(),
            partition: 0,
            offset: -1,
        };

        let bytes: Vec<u8> = offset.try_into().expect("serialize negative offset");
        let deserialized: KafkaOffset = bson::from_slice(&bytes).expect("deserialize");

        assert_eq!(deserialized.offset, -1);
    }
}
