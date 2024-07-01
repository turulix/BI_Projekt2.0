use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;

use crate::context::Context;
use crate::tasks::CronTask;

pub struct StartFerienUpdateTask;

#[async_trait]
impl CronTask for StartFerienUpdateTask {
    fn name(&self) -> &'static str {
        "StartFerienUpdate"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(7 * 24 * 60 * 60)
    }

    async fn run(&self, context: &Context) -> Result<(), Error> {
        let pub_sub_topic = context.pubsub_client.topic("new-data-added");
        if !pub_sub_topic.exists(None).await? {
            pub_sub_topic.create(None, None).await?;
        }
        let publisher = pub_sub_topic.new_publisher(None);

        let awaiter = publisher
            .clone()
            .publish(PubsubMessage {
                data: b"new data added".to_vec(),
                ..Default::default()
            })
            .await;
        awaiter.get().await?;

        Ok(())
    }
}
