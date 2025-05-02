use std::collections::HashMap;
use std::env;

use anyhow::anyhow;
use apache_avro::AvroSchema;
use mongodb::bson::{doc, Document};
use mongodb::Collection;
use mstream::config::{
    Encoding, GcpAuthConfig, SchemaServiceConfigReference, Service, ServiceConfigReference,
    SourceServiceConfigReference,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::service::Interceptor;

use mstream::pubsub::api::{AcknowledgeRequest, PullRequest};
use mstream::pubsub::{ServiceAccountAuth, StaticAccessToken};

// DB constants
const CONNECTOR_NAME: &str = "employee-stream-test";
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

pub async fn start_app_listener(done_ch: mpsc::Sender<String>) {
    use mstream::cmd;
    use mstream::config::{Config, Connector};

    tokio::spawn(async move {
        let config = Config {
            services: vec![
                Service::PubSub {
                    name: "pubsub".to_owned(),
                    auth: GcpAuthConfig::StaticToken {
                        env_token_name: "MSTREAM_TEST_AUTH_TOKEN".to_owned(),
                    },
                },
                Service::MongoDb {
                    name: "mongodb".to_owned(),
                    connection_string: DB_CONNECTION.to_owned(),
                    db_name: DB_NAME.to_owned(),
                },
            ],
            connectors: vec![Connector {
                name: CONNECTOR_NAME.to_owned(),
                source: SourceServiceConfigReference {
                    service_name: "mongodb".to_owned(),
                    resource: DB_COLLECTION.to_owned(),
                    output_encoding: Encoding::Bson,
                    input_encoding: None,
                    schema_id: None,
                },
                middlewares: None,
                schemas: Some(vec![SchemaServiceConfigReference {
                    id: "pubsub-schema-id".to_owned(),
                    service_name: "pubsub".to_owned(),
                    resource: env::var("PUBSUB_SCHEMA").unwrap(),
                }]),
                sinks: vec![ServiceConfigReference {
                    service_name: "pubsub".to_owned(),
                    resource: env::var("PUBSUB_TOPIC").unwrap(),
                    output_encoding: Encoding::Avro,
                    schema_id: Some("pubsub-schema-id".to_owned()),
                }],
            }],
            ..Default::default()
        };

        cmd::listen_streams(done_ch, config).await.unwrap();
    });
}

pub async fn setup_db(
    coll: &Collection<Employee>,
) -> anyhow::Result<Vec<(HashMap<String, String>, Employee)>> {
    let docs = fixtures()
        .into_iter()
        .map(|(before, _, _)| before)
        .collect::<Vec<_>>();

    coll.insert_many(docs.clone(), None).await?;

    Ok(docs
        .into_iter()
        .map(|item| {
            let attributes = generate_pubsub_attributes("insert");
            (attributes, item)
        })
        .collect::<Vec<_>>())
}

use mstream::pubsub::api::subscriber_client::SubscriberClient;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type SubscriberService<I> = SubscriberClient<InterceptedService<Channel, I>>;

async fn subscriber<I: Interceptor>(interceptor: I) -> anyhow::Result<SubscriberService<I>> {
    use mstream::pubsub::tls_transport;
    let channel = tls_transport().await?;
    Ok(SubscriberClient::with_interceptor(channel, interceptor))
}

pub fn generate_pubsub_attributes(op_type: &str) -> HashMap<String, String> {
    HashMap::from([
        ("stream_name".to_owned(), CONNECTOR_NAME.to_owned()),
        ("operation_type".to_owned(), op_type.to_owned()),
        ("database".to_owned(), DB_NAME.to_owned()),
        ("collection".to_owned(), DB_COLLECTION.to_owned()),
    ])
}

pub async fn pull_from_pubsub(
    msg_number: i32,
) -> anyhow::Result<Vec<(HashMap<String, String>, Employee)>> {
    let static_token = StaticAccessToken(
        env::var("MSTREAM_TEST_AUTH_TOKEN")
            .map_err(|err| anyhow!("env var MSTREAM_TEST_AUTH_TOKEN is not set: {}", err))?,
    );

    let auth_interceptor = ServiceAccountAuth::new(static_token);
    let mut ps_subscriber = subscriber(auth_interceptor.clone()).await?;

    log::info!("pulling from pubsub...");

    let subscription = env::var("PUBSUB_SUBSCRIPTION").unwrap();

    let response = ps_subscriber
        .pull(PullRequest {
            subscription: subscription.clone(),
            max_messages: msg_number,
            ..Default::default()
        })
        .await
        .map_err(|err| anyhow!("failed to pull from pubsub: {}", err))?;

    let avro_schema = Employee::get_schema();
    let mut employees = vec![];

    for message in response.into_inner().received_messages {
        let msg = message
            .message
            .ok_or_else(|| anyhow!("message not found"))?;

        let mut buffer = msg.data.as_slice();
        let avro_value = apache_avro::from_avro_datum(&avro_schema, &mut buffer, None)?;

        let employee: Employee = apache_avro::from_value(&avro_value)?;
        employees.push((msg.attributes, employee));

        ps_subscriber
            .acknowledge(AcknowledgeRequest {
                subscription: subscription.clone(),
                ack_ids: vec![message.ack_id],
            })
            .await
            .map_err(|err| anyhow!("failed to acknowledge message: {}", err))?;
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
