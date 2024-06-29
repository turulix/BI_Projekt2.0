use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;

use crate::context::Context;
use crate::tasks::CronTask;

pub struct SendDummyEventTask;

#[async_trait]
impl CronTask for SendDummyEventTask {
    fn name(&self) -> &'static str {
        "SendDummyEvent"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(60 * 5)
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
