use std::collections::{HashMap, HashSet};
use std::fs::File;

use calamine::{
    open_workbook_auto_from_rs, Data, DataType, Range, RangeDeserializerBuilder, Reader,
};
use log::{info, trace};
use serde::{Deserialize, Serialize};

pub struct XlsxData {
    pub uebernachtungen_nach_herkunftsland: Range<Data>,
    pub uebernachtungen_pro_land: Range<Data>,
}

pub async fn load_xlsx_file(file: &File) -> Result<XlsxData, anyhow::Error> {
    let mut workbook = open_workbook_auto_from_rs(file)
        .map_err(|e| anyhow::anyhow!("Failed to open workbook: {}", e))?;
    if workbook.sheet_names().is_empty() {
        return Err(anyhow::anyhow!("No sheets found in workbook"));
    }

    // We need sheet csv-45412-07 and csv-45412-08
    if !workbook.sheet_names().contains(&"csv-45412-07".to_string()) {
        return Err(anyhow::anyhow!("Sheet csv-45412-07 not found"));
    }
    if !workbook.sheet_names().contains(&"csv-45412-08".to_string()) {
        return Err(anyhow::anyhow!("Sheet csv-45412-08 not found"));
    }

    let uebernachtungen_nach_herkunftsland = workbook
        .worksheet_range("csv-45412-07")
        .map_err(|e| anyhow::anyhow!("Failed to map range for csv-45412-07: {}", e))?;

    let uebernachtungen_pro_land = workbook
        .worksheet_range("csv-45412-08")
        .map_err(|e| anyhow::anyhow!("Failed to map range for csv-45412-08: {}", e))?;
    Ok(XlsxData {
        uebernachtungen_nach_herkunftsland,
        uebernachtungen_pro_land,
    })
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UebernachtungenNachHerkunftslandStruct {
    pub herkunftsregion: String,
    pub jahr: i64,
    pub monat: String,
    pub ankuenfte_anzahl: Option<i64>,
    pub ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,
    pub uebernachtungen_anzahl: Option<i64>,
    pub uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,
    pub durchsch_aufenthaltsdauer_tage: Option<f64>,
}
pub async fn parse_uebernachtungen_nach_herkunftsland(
    data: Range<Data>,
) -> Result<Vec<UebernachtungenNachHerkunftslandStruct>, anyhow::Error> {
    let mut rows = data.rows();

    let headers = rows
        .next()
        .ok_or(anyhow::anyhow!("No headers found"))?
        .iter()
        .map(|x| x.to_string().trim().to_string())
        .collect::<Vec<String>>();

    trace!("{:?}", headers);
    let mut data = Vec::new();
    for row in rows {
        let hashmap: HashMap<String, &Data> = headers.iter().cloned().zip(row.iter()).collect();
        info!("{:?}", hashmap);
        data.push(UebernachtungenNachHerkunftslandStruct {
            herkunftsregion: hashmap.get("Herkunftsregion").unwrap().to_string(),
            jahr: hashmap
                .get("Jahr")
                .unwrap()
                .as_i64()
                .ok_or(anyhow::anyhow!("Failed to parse Jahr"))?,
            monat: hashmap
                .get("Monat")
                .unwrap()
                .as_string()
                .ok_or(anyhow::anyhow!("Failed to parse Monat"))?,
            ankuenfte_anzahl: hashmap.get("Ankuenfte_Anzahl").unwrap().as_i64(),
            ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent: hashmap
                .get("Ankuenfte_Veraenderung_zum_Vorjahreszeitraum_Prozent")
                .unwrap()
                .as_f64(),
            uebernachtungen_anzahl: hashmap.get("Uebernachtungen_Anzahl").unwrap().as_i64(),
            uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent: hashmap
                .get("Uebernachtungen_Veraenderung_zum_Vorjahreszeitraum_Prozent")
                .unwrap()
                .as_f64(),
            durchsch_aufenthaltsdauer_tage: hashmap
                .get("Durchsch_Aufenthaltsdauer_Tage")
                .unwrap()
                .as_f64(),
        });
    }
    Ok(data)
}
