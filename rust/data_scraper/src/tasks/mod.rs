mod get_sleepover_data;
mod send_dummy_event;

pub use get_sleepover_data::GetSleepoverDataTask;
pub use send_dummy_event::SendDummyEventTask;


use async_trait::async_trait;

use crate::context::Context;

#[async_trait]
pub trait CronTask: Send + Sync {
    fn name(&self) -> &'static str;

    fn interval(&self) -> core::time::Duration;

    async fn run(&self, context: &Context) -> Result<(), anyhow::Error>;
}

pub trait CronTaskExtension {
    fn into_boxed(self) -> Box<Self>;
}
impl<C: CronTask + Sized> CronTaskExtension for C {
    fn into_boxed(self) -> Box<Self> {
        Box::new(self)
    }
}
