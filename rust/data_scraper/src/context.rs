use crate::settings::Settings;

#[derive(Clone)]
pub struct Context {
    pub settings: Settings,
    pub http_client: reqwest::Client,
    pub pubsub_client: google_cloud_pubsub::client::Client,
    pub database_client: sqlx::PgPool,
}