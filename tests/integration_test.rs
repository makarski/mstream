use log::{debug, info};
use mongodb::{bson::doc, options::UpdateOptions, Client, Collection};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

mod setup;
use setup::{drop_db, fixtures, pull_from_pubsub, setup_db, start_app_listener, Employee};

#[tokio::test]
#[ignore = "Integration test - requires a running mongodb in docker and connection to GCP pubsub"]
async fn test_created_updated_db_to_pubsub() {
    pretty_env_logger::try_init_timed().unwrap();

    let client = Client::with_uri_str(setup::DB_CONNECTION).await.unwrap();
    let db = client.database(setup::DB_NAME);
    let coll = db.collection(setup::DB_COLLECTION);

    // spawn change stream listener
    let (tx, _) = mpsc::channel::<String>(1);
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

    drop_db(db).await.unwrap();
}

async fn modify_assert_employees_db(coll: &Collection<Employee>) -> anyhow::Result<Vec<Employee>> {
    // https://www.mongodb.com/developer/languages/rust/rust-mongodb-crud-tutorial/

    let fixtures = fixtures();
    let mut modified_employees = Vec::with_capacity(fixtures.len());

    for (before, after, change_fn) in fixtures.into_iter() {
        let filter = doc! {"id": before.id};
        let options = UpdateOptions::builder().upsert(Some(false)).build();

        let result = coll.update_one(filter, change_fn, options).await?;

        assert_eq!(
            1, result.modified_count,
            "expected to update one document: employee id: {}",
            before.id
        );

        let result = coll.find_one(doc! { "id": before.id }, None).await?;

        assert_eq!(
            Some(after.clone()),
            result,
            "failed to assume the db entry state after mutation. employee id: {}",
            before.id
        );

        modified_employees.push(after);
    }

    Ok(modified_employees)
}

async fn assert_employees_eq_pubsub(expected: Vec<Employee>) -> anyhow::Result<()> {
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

    events.sort_by(|a, b| a.id.cmp(&b.id));

    for (i, event) in events.into_iter().enumerate() {
        debug!("{:?}", event);

        assert_eq!(
            expected[i], event,
            "failed to assume the expected employee eq to the pubsub message. id: {}",
            event.id
        );
    }

    Ok(())
}
