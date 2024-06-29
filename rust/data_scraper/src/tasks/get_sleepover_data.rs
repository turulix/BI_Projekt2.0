use async_trait::async_trait;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use log::{info, trace};
use serde::{Deserialize, Serialize};
use sqlx::Executor;

use crate::helpers::{
    download_derivative_information, download_derivative_to_file, get_table_of_contents,
    load_xlsx_file, parse_range, UebernachtungenNachHerkunftslandStruct,
    UebernachtungenProLandStruct,
};
use crate::tasks::CronTask;
use crate::translate_to_month;

pub struct GetSleepoverDataTask;

#[async_trait]
impl CronTask for GetSleepoverDataTask {
    fn name(&self) -> &'static str {
        "GetSleepoverData"
    }

    fn interval(&self) -> core::time::Duration {
        core::time::Duration::from_secs(60 * 60 * 12)
    }

    async fn run(&self, context: &crate::context::Context) -> Result<(), anyhow::Error> {
        let pub_sub_topic = context.pubsub_client.topic("new-data-added");
        if !pub_sub_topic.exists(None).await? {
            pub_sub_topic.create(None, None).await?;
        }

        let publisher = pub_sub_topic.new_publisher(None);

        let table_of_contents = get_table_of_contents(&context.http_client).await?;
        let mut new_data_added = false;
        info!("Successfully fetched '{}' files", table_of_contents.len());

        #[derive(Debug, Serialize, Deserialize)]
        struct Date {
            jahr: i64,
            monat: String,
        }
        let already_fetched_dates: Vec<Date> =
            sqlx::query_file_as!(Date, "src/queries/select_already_fetched_dates.sql")
                .fetch_all(&context.database_client)
                .await?;

        for x in table_of_contents {
            if already_fetched_dates
                .iter()
                .any(|y| y.jahr == x.year as i64 && y.monat == translate_to_month(x.month))
            {
                info!("Skipping: {} {}, we already have those.", x.year, x.month);
                continue;
            }

            new_data_added = true;

            let derivative_info =
                download_derivative_information(&context.http_client, &x.mods_id).await?;
            trace!("{:?}", derivative_info);

            let file =
                download_derivative_to_file(&context.http_client, &derivative_info.children[0])
                    .await?;

            let parsed_file = load_xlsx_file(&file).await?;
            let overnight_by_origin: Vec<UebernachtungenNachHerkunftslandStruct> =
                parse_range(parsed_file.uebernachtungen_nach_herkunftsland).await?;
            let overnight_by_country: Vec<UebernachtungenProLandStruct> =
                parse_range(parsed_file.uebernachtungen_pro_land).await?;

            info!("{:?}", overnight_by_origin.len());
            info!("{:?}", overnight_by_country.len());

            let mut tx = context.database_client.begin().await?;
            for x in overnight_by_origin {
                let query = sqlx::query_file!(
                    "src/queries/insert_into_uebernachtungen_nach_herkunftsland.sql",
                    x.herkunftsregion.trim(),
                    x.jahr,
                    x.monat.trim(),
                    x.ankuenfte_anzahl,
                    x.ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent,
                    x.uebernachtungen_anzahl,
                    x.uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent,
                    x.durchsch_aufenthaltsdauer_tage
                );
                tx.execute(query).await?;
            }

            for x in overnight_by_country {
                let query = sqlx::query_file!(
                    "src/queries/insert_into_uebernachtungen_pro_land.sql",
                    x.land.trim(),
                    x.wohnsitz.trim(),
                    x.jahr,
                    x.monat.trim(),
                    x.ankuenfte_anzahl,
                    x.ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent,
                    x.uebernachtungen_anzahl,
                    x.uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent,
                    x.durchsch_aufenthaltsdauer_tage
                );
                tx.execute(query).await?;
            }

            tx.commit().await?;
        }

        if new_data_added {
            let awaiter = publisher
                .publish(PubsubMessage {
                    data: b"new data added".to_vec(),
                    ..Default::default()
                })
                .await;
            awaiter.get().await?;
            info!("Published new data added message to pubsub")
        }
        Ok(())
    }
}
