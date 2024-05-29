use log::trace;
use reqwest::Client;
use scraper::Selector;

#[derive(Debug)]
#[allow(dead_code)]
pub struct TableOfContent {
    pub year: i32,
    pub month: i32,
    pub url: String,
    // Apparently, there is an ID that we can use to download the file.
    pub mods_id: String,
}
pub async fn get_table_of_contents(client: &Client) -> Result<Vec<TableOfContent>, anyhow::Error> {
    let initial_page_content = client
        .get("https://www.statistischebibliothek.de/mir/receive/DESerie_mods_00007671")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch initial page: {}", e))?;

    let initial_page_text = initial_page_content.text().await?;
    let document = scraper::Html::parse_document(&initial_page_text);
    let main_body = match document
        /* We can just unwrap this because we know the selector is correct. */
        .select(&Selector::parse("#main_col > div:nth-child(2) > ul").unwrap())
        .next()
    {
        None => {
            return Err(anyhow::anyhow!("Failed to find main body"));
        }
        Some(x) => x,
    };

    let mut table_of_contents = Vec::new();
    for x in main_body.select(&Selector::parse("li").unwrap()) {
        let inner_text = x.text().collect::<String>().trim().to_string();
        trace!("Inner text: {}", inner_text);
        // 2024,02
        let year = inner_text[0..4]
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("Failed to parse year: {}", e))?;
        let month = inner_text[5..7]
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("Failed to parse month: {}", e))?;

        let url = x
            .select(&Selector::parse("a").unwrap())
            .next()
            .ok_or(anyhow::anyhow!("Failed to find A Tag"))?
            .attr("href")
            .ok_or(anyhow::anyhow!("Failed to find href url"))?
            .to_string();

        let mods_id = url
            .split("/")
            .last()
            .ok_or(anyhow::anyhow!("Failed to find mod id"))?
            .to_string();
        if !mods_id.starts_with("DEHeft_mods_") {
            return Err(anyhow::anyhow!("Invalid mod id for url: {}", url));
        }

        trace!("Year: {}, Month: {}, URL: {}", year, month, url);

        table_of_contents.push(TableOfContent {
            year,
            month,
            url,
            mods_id,
        });
    }

    Ok(table_of_contents)
}
