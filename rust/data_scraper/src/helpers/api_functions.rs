use std::fs::File;
use log::warn;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tempfile::SpooledTempFile;

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectsResponse {
    #[serde(rename = "numFound")]
    pub num_found: i32,
    pub mycoreobjects: Vec<CoreObject>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CoreObject {
    #[serde(rename = "ID")]
    pub id: String,
    pub metadata: String,
    pub label: String,
    #[serde(rename = "lastModified")]
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub href: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DerivativesResponse {
    // There are more fields here, but we only care about the children.
    pub children: Vec<Derivative>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Derivative {
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    pub path: String,
    #[serde(rename = "parentPath")]
    pub parent_path: String,
    pub size: usize,
    // Ignore The Time Field.
    #[serde(rename = "contentType")]
    pub content_type: String,
    pub md5: String,
    pub extension: String,
    pub href: String,
}

pub async fn download_derivative_information(
    client: &Client,
    mod_id: &str,
) -> Result<DerivativesResponse, anyhow::Error> {
    let response: ObjectsResponse = client
        .get(&format!(
            "https://www.statistischebibliothek.de/mir/api/v1/objects/{}/derivates?format=json",
            mod_id
        ))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch objects for: {}, {}", mod_id, e))?
        .json::<ObjectsResponse>()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse json: {}", e))?;

    if response.mycoreobjects.len() > 1 {
        warn!(
            "Expected 1 derivative, got: {}. Defaulting to the first one.",
            response.mycoreobjects.len()
        );
    } else if response.mycoreobjects.is_empty() {
        return Err(anyhow::anyhow!("No derivatives found for: {}", mod_id));
    }

    let derivatives_href = format!("{}/contents?format=json", &response.mycoreobjects[0].href);
    let response: DerivativesResponse = client
        .get(&derivatives_href)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch derivatives for: {}, {}", mod_id, e))?
        .json::<DerivativesResponse>()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse json: {}", e))?;

    Ok(response)
}

pub async fn download_derivative_to_file(
    client: &Client,
    derivative: &Derivative,
) -> Result<File, anyhow::Error> {
    let response = client.get(&derivative.href).send().await.map_err(|e| {
        anyhow::anyhow!("Failed to download derivative: {}, {}", &derivative.href, e)
    })?;

    let mut file = tempfile::tempfile().map_err(|e| anyhow::anyhow!("Failed to create tempfile: {}", e))?;
    let stream = response
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get bytes: {}", e))?;
    
    std::io::copy(&mut stream.as_ref(), &mut file)
        .map_err(|e| anyhow::anyhow!("Failed to copy bytes: {}", e))?;

    Ok(file)
}
