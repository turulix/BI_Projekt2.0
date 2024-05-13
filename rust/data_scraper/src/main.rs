use crate::helpers::{download_derivative_information, download_derivative_to_file, get_table_of_contents, load_xlsx_file, parse_uebernachtungen_nach_herkunftsland, Derivative, parse_uebernachtungen_pro_land};
use log::{info, trace};

mod helpers;
mod settings;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let settings = settings::Settings::new().expect("Unable to load settings");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let table_of_contents = get_table_of_contents(&client).await?;
    trace!("{:?}", table_of_contents);
    for x in table_of_contents {
        let derivative_info = download_derivative_information(&client, &x.mods_id).await?;
        trace!("{:?}", derivative_info);

        let file = download_derivative_to_file(&client, &derivative_info.children[0]).await?;

        let parsed_file = load_xlsx_file(&file).await?;
        let overnight_by_origin = parse_uebernachtungen_nach_herkunftsland(parsed_file.uebernachtungen_nach_herkunftsland).await?;
        let overnight_by_country = parse_uebernachtungen_pro_land(parsed_file.uebernachtungen_pro_land).await?;

        info!("{:?}", overnight_by_origin);
        info!("{:?}", overnight_by_country);
        
        break;
    }
    Ok(())
}
