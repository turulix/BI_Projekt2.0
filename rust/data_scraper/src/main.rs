use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use log::{info, trace};
use serde::{Deserialize, Serialize};
use sqlx::Executor;

use crate::helpers::{
    download_derivative_information, download_derivative_to_file, get_table_of_contents,
    load_xlsx_file, parse_range, UebernachtungenNachHerkunftslandStruct,
    UebernachtungenProLandStruct,
};

mod helpers;
mod settings;

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
    
    let pub_sub_topic = pub_sub_client.topic("new-data-added");
    if !pub_sub_topic.exists(None).await? { 
        pub_sub_topic.create(None, None).await?;
    }
    
    let publisher = pub_sub_topic.new_publisher(None);
    
    // FOR TESTING PURPOSES WE WILL SEND A MESSAGE TO THE TOPIC EVERY 5 MINUTES
    let publisher_clone = publisher.clone();
    tokio::spawn(async move {
        loop {
            let awaiter = publisher_clone.clone().publish(PubsubMessage{
                data: b"new data added".to_vec(),
                ..Default::default()
            }).await;
            awaiter.get().await.unwrap();
            info!("Published new data added message to pubsub");
            tokio::time::sleep(std::time::Duration::from_secs(60 * 5)).await;
        }
    });

    loop {
        let table_of_contents = get_table_of_contents(&client).await?;
        let mut new_data_added = false;
        info!("Successfully fetched '{}' files", table_of_contents.len());

        #[derive(Debug, Serialize, Deserialize)]
        struct Date {
            jahr: i64,
            monat: String,
        }
        let already_fetched_dates: Vec<Date> =
            sqlx::query_file_as!(Date, "src/queries/select_already_fetched_dates.sql")
                .fetch_all(&database)
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

            let derivative_info = download_derivative_information(&client, &x.mods_id).await?;
            trace!("{:?}", derivative_info);

            let file = download_derivative_to_file(&client, &derivative_info.children[0]).await?;

            let parsed_file = load_xlsx_file(&file).await?;
            let overnight_by_origin: Vec<UebernachtungenNachHerkunftslandStruct> =
                parse_range(parsed_file.uebernachtungen_nach_herkunftsland).await?;
            let overnight_by_country: Vec<UebernachtungenProLandStruct> =
                parse_range(parsed_file.uebernachtungen_pro_land).await?;

            info!("{:?}", overnight_by_origin.len());
            info!("{:?}", overnight_by_country.len());

            let mut tx = database.begin().await?;
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
            let awaiter = publisher.publish(PubsubMessage{
                data: b"new data added".to_vec(),
                ..Default::default()
            }).await;
            awaiter.get().await?;
            info!("Published new data added message to pubsub")
        }
        
        let sleep_duration = std::time::Duration::from_secs(60 * 60 * 12);
        info!(
            "Sleeping for 12 hours before fetching again. Next Execution: {}",
            (chrono::Local::now() + sleep_duration)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        );
        tokio::time::sleep(sleep_duration).await;
    }
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
