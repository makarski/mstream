use std::collections::HashMap;

use log::{debug, info};
use mongodb::{Client, Collection, bson::doc, options::UpdateOptions};
use mstream::job_manager::JobState;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

mod setup;
use setup::{
    Employee, fixtures, generate_pubsub_attributes, pull_from_pubsub, setup_db, start_app_listener,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Integration test - requires a running mongodb in docker and connection to GCP pubsub"]
async fn test_created_updated_db_to_pubsub() {
    let mongo_uri = std::env::var(setup::DB_CONNECTION_ENV)
        .expect("expected MONGO_URI env var to be set for integration tests");
    let client = Client::with_uri_str(mongo_uri).await.unwrap();
    let db = client.database(setup::DB_NAME);
    db.drop().await.unwrap();
    db.create_collection(setup::DB_COLLECTION).await.unwrap();

    let coll = db.collection(setup::DB_COLLECTION);

    // spawn change stream listener
    let (tx, _) = mpsc::unbounded_channel::<(String, JobState)>();
    start_app_listener(tx).await;

    info!("setting up db, sleeping for 10 secs");
    sleep(Duration::from_secs(10)).await;

    // testing create events
    let created_employees = setup_db(&coll).await.unwrap();
    sleep(Duration::from_secs(10)).await;
    assert_employees_eq_pubsub(created_employees).await.unwrap();

    // testing update events
    info!("modifiying db, sleeping for 20 secs...");
    let modified_employees = modify_assert_employees_db(&coll).await.unwrap();

    sleep(Duration::from_secs(20)).await;
    assert_employees_eq_pubsub(modified_employees)
        .await
        .unwrap();

    db.drop().await.unwrap();
}

async fn modify_assert_employees_db(
    coll: &Collection<Employee>,
) -> anyhow::Result<Vec<(HashMap<String, String>, Employee)>> {
    // https://www.mongodb.com/developer/languages/rust/rust-mongodb-crud-tutorial/

    let fixtures = fixtures();
    let mut modified_employees = Vec::with_capacity(fixtures.len());

    for (before, after, change_fn) in fixtures.into_iter() {
        let filter = doc! {"id": before.id};
        let options = UpdateOptions::builder().upsert(Some(false)).build();

        let result = coll
            .update_one(filter, change_fn)
            .with_options(options)
            .await?;

        assert_eq!(
            1, result.modified_count,
            "expected to update one document: employee id: {}",
            before.id
        );

        let result = coll.find_one(doc! { "id": before.id }).await?;

        assert_eq!(
            Some(after.clone()),
            result,
            "failed to assume the db entry state after mutation. employee id: {}",
            before.id
        );

        let attributes = generate_pubsub_attributes("update");
        modified_employees.push((attributes, after));
    }

    Ok(modified_employees)
}

async fn assert_employees_eq_pubsub(
    expected: Vec<(HashMap<String, String>, Employee)>,
) -> anyhow::Result<()> {
    let mut events = pull_from_pubsub(expected.len() as i32).await?;

    if events.len() < expected.len() {
        let num = expected.len() - events.len();

        debug!(
            "pulling {} more events from pubsub to match the expected number of events",
            num
        );

        let events2 = pull_from_pubsub(num as i32).await?;
        events.extend(events2);
    }

    events.sort_by(|a, b| a.1.id.cmp(&b.1.id));

    for (i, (attributes, event)) in events.into_iter().enumerate() {
        debug!("{:?}", event);

        for (k, v) in expected[i].0.iter() {
            assert_eq!(
                Some(v),
                attributes.get(k),
                "failed to assume attribute key {} contains the expected value: {}. id: {}",
                k,
                v,
                event.id
            );
        }

        assert_eq!(
            expected[i].1, event,
            "failed to assume the expected employee eq to the pubsub message. id: {}",
            event.id
        );
    }

    Ok(())
}
