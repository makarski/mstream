use mongodb::options::ClientOptions;
use mongodb::Client;

pub async fn db_client(name: String, conn_str: &str) -> anyhow::Result<Client> {
    let mut opts = ClientOptions::parse(conn_str).await?;
    opts.app_name = Some(name);

    Ok(Client::with_options(opts)?)
}
