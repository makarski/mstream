use anyhow::anyhow;
use apache_avro::AvroSchema;
use mgocdc::pubsub::api::AcknowledgeRequest;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::{Deserialize, Serialize};

// PUBSUB constants
const PUBSUB_SCHEMA: &str = "projects/mgocdc/schemas/employee-integration-test";
const PUBSUB_TOPIC: &str = "projects/mgocdc/topics/employee-test-poc";
const PUBSUB_SUBSCRIPTION: &str = "projects/mgocdc/subscriptions/employee-test-sub";

// DB constants
pub const DB_NAME: &str = "integration-tests";
pub const DB_COLLECTION: &str = "employees";
pub const DB_CONNECTION: &str = "mongodb://localhost:27017";

#[derive(AvroSchema, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Employee {
    pub id: i32,
    pub name: String,
    pub age: i32,
    pub is_active: bool,
    pub long_number: i64,
    pub rating: f64,
}

pub async fn start_app_listener(rx: tokio::sync::oneshot::Receiver<bool>, ps_access_token: String) {
    use mgocdc::cmd::listener;
    use mgocdc::config::{Config, Connector};

    tokio::spawn(async move {
        let config = Config {
            connectors: vec![Connector {
                name: "employee".to_owned(),
                db_connection: DB_CONNECTION.to_owned(),
                db_name: DB_NAME.to_owned(),
                db_collection: DB_COLLECTION.to_owned(),
                schema: PUBSUB_SCHEMA.to_owned(),
                topic: PUBSUB_TOPIC.to_owned(),
            }],
        };

        listener::listen(config, ps_access_token).await.unwrap();
        rx.await.unwrap();
    });
}

pub async fn setup_db(coll: &Collection<Employee>) -> anyhow::Result<Vec<Employee>> {
    let docs = fixtures()
        .into_iter()
        .map(|(before, _, _)| before)
        .collect::<Vec<_>>();

    coll.insert_many(docs.clone(), None).await?;
    Ok(docs)
}

pub async fn drop_db(db: Database) -> anyhow::Result<()> {
    Ok(db.drop(None).await?)
}

pub async fn pull_from_pubsub(access_token: &str) -> anyhow::Result<Vec<Employee>> {
    use mgocdc::pubsub::{api::PullRequest, sub::subscriber};

    let mut sub = subscriber(access_token).await?;
    let response = sub
        .pull(PullRequest {
            subscription: PUBSUB_SUBSCRIPTION.to_owned(),
            max_messages: 5,
            return_immediately: false,
        })
        .await?;

    let avro_schema = Employee::get_schema();
    let mut employees = vec![];

    for message in response.into_inner().received_messages {
        let msg = message
            .message
            .ok_or_else(|| anyhow!("message not found"))?;

        let mut buffer = msg.data.as_slice();
        let avro_value = apache_avro::from_avro_datum(&avro_schema, &mut buffer, None)?;

        let employee: Employee = apache_avro::from_value(&avro_value)?;
        employees.push(employee);

        sub.acknowledge(AcknowledgeRequest {
            subscription: PUBSUB_SUBSCRIPTION.to_owned(),
            ack_ids: vec![message.ack_id],
        })
        .await?;
    }

    Ok(employees)
}

pub fn fixtures() -> Vec<(Employee, Employee, Document)> {
    vec![
        (
            Employee {
                id: 1,
                name: "User 1".to_owned(),
                age: 20,
                is_active: true,
                long_number: 123456789_i64,
                rating: 4.5,
            },
            Employee {
                id: 1,
                name: "User 1".to_owned(),
                age: 21,
                is_active: true,
                long_number: 123456789_i64,
                rating: 4.5,
            },
            doc! { "$set": { "age": 21 } },
        ),
        (
            Employee {
                id: 2,
                name: "User 2".to_owned(),
                age: 30,
                is_active: false,
                long_number: 987654321_i64,
                rating: 3.5,
            },
            Employee {
                id: 2,
                name: "User 2".to_owned(),
                age: 30,
                is_active: true,
                long_number: 987654321_i64,
                rating: 3.5,
            },
            doc! { "$set": { "is_active": true } },
        ),
        (
            Employee {
                id: 3,
                name: "User 3".to_owned(),
                age: 40,
                is_active: true,
                long_number: 123456789_i64,
                rating: 2.5,
            },
            Employee {
                id: 3,
                name: "User 3+".to_owned(),
                age: 40,
                is_active: true,
                long_number: 123456789_i64,
                rating: 2.5,
            },
            doc! { "$set": { "name": "User 3+" } },
        ),
        (
            Employee {
                id: 4,
                name: "User 4".to_owned(),
                age: 50,
                is_active: false,
                long_number: 987654321_i64,
                rating: 1.5,
            },
            Employee {
                id: 4,
                name: "User 4".to_owned(),
                age: 50,
                is_active: false,
                long_number: 987654321_i64,
                rating: 1.6,
            },
            doc! { "$set": { "rating": 1.6 } },
        ),
        (
            Employee {
                id: 5,
                name: "User 5".to_owned(),
                age: 60,
                is_active: true,
                long_number: 123456789_i64,
                rating: 0.5,
            },
            Employee {
                id: 5,
                name: "User 5".to_owned(),
                age: 60,
                is_active: true,
                long_number: 1234567890_i64,
                rating: 0.5,
            },
            doc! { "$set": { "long_number": 1234567890_i64 } },
        ),
    ]
}
