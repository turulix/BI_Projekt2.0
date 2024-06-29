use google_cloud_pubsub::client::{Client, ClientConfig};
use log::{debug, error, info};
use tokio::{select, signal};
use tokio::task::JoinSet;

use crate::context::Context;
use crate::tasks::{CronTask, CronTaskExtension};

mod context;
mod helpers;
mod settings;
mod tasks;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let settings = settings::Settings::new().expect("Unable to load settings");
    let gcloud_config = ClientConfig::default().with_auth().await.unwrap();
    let pub_sub_client = Client::new(gcloud_config).await.unwrap();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let database = sqlx::PgPool::connect(&settings.database_url).await?;
    info!("Connected to database");

    let context = Context {
        settings,
        http_client: client,
        pubsub_client: pub_sub_client.clone(),
        database_client: database,
    };

    let tasks: Vec<Box<dyn CronTask>> = vec![tasks::GetSleepoverDataTask.into_boxed()];

    let mut join_set = JoinSet::new();
    for task in tasks {
        info!("Starting task: {}", task.name());
        let cloned_context = context.clone();
        join_set.spawn(async move {
            let task = task;
            let mut interval = tokio::time::interval(task.interval());
            let cancellation = shutdown_signal_future();
            tokio::pin!(cancellation);

            loop {
                select! {
                    _ = interval.tick() => {},
                    _ = &mut cancellation => {
                        info!("Task {} cancelled", task.name());
                        break;
                    }
                }

                async {
                    if let Err(e) = task.run(&cloned_context).await {
                        error!("Task {} failed: {}", task.name(), e);
                    } else {
                        debug!("Task {} completed", task.name());
                    }
                }
                .await;
            }
        });
    }

    while join_set.join_next().await.is_some() {}

    info!("All tasks completed");

    Ok(())
}

fn translate_to_month(month: i32) -> String {
    match month {
        1 => "Januar",
        2 => "Februar",
        3 => "März",
        4 => "April",
        5 => "Mai",
        6 => "Juni",
        7 => "Juli",
        8 => "August",
        9 => "September",
        10 => "Oktober",
        11 => "November",
        12 => "Dezember",
        _ => panic!("Invalid month"),
    }
    .to_string()
}

async fn shutdown_signal_future() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("signal received, starting graceful shutdown");
}
