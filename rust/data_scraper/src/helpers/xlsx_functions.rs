use std::collections::HashMap;
use std::fs::File;

use calamine::{Data, open_workbook_auto_from_rs, Range, Reader};
use log::{info, trace};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde::de::DeserializeOwned;
use serde_json::Value;

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
    #[serde(rename = "Herkunftsregion")]
    pub herkunftsregion: String,

    #[serde(rename = "Jahr", deserialize_with = "de_as_i64")]
    pub jahr: i64,

    #[serde(rename = "Monat")]
    pub monat: String,

    #[serde(rename = "Ankuenfte_Anzahl", deserialize_with = "de_as_i64_option")]
    pub ankuenfte_anzahl: Option<i64>,

    #[serde(
        rename = "Ankuenfte_Veraenderung_zum_Vorjahreszeitraum_Prozent",
        deserialize_with = "de_as_f64_option"
    )]
    pub ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,

    #[serde(
        rename = "Uebernachtungen_Anzahl",
        deserialize_with = "de_as_i64_option"
    )]
    pub uebernachtungen_anzahl: Option<i64>,

    #[serde(
        rename = "Uebernachtungen_Veraenderung_zum_Vorjahreszeitraum_Prozent",
        deserialize_with = "de_as_f64_option"
    )]
    pub uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,

    #[serde(
        rename = "Durchsch_Aufenthaltsdauer_Tage",
        deserialize_with = "de_as_f64_option"
    )]
    pub durchsch_aufenthaltsdauer_tage: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UebernachtungenProLandStruct {
    #[serde(rename = "Land")]
    pub land: String,

    #[serde(rename = "Wohnsitz")]
    pub wohnsitz: String,

    #[serde(rename = "Jahr", deserialize_with = "de_as_i64")]
    pub jahr: i64,

    #[serde(rename = "Monat")]
    pub monat: String,

    #[serde(rename = "Ankuenfte_Anzahl", deserialize_with = "de_as_i64_option")]
    pub ankuenfte_anzahl: Option<i64>,

    #[serde(
        rename = "Ankuenfte_Veraenderung_zum_Vorjahreszeitraum_Prozent",
        deserialize_with = "de_as_f64_option"
    )]
    pub ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,

    #[serde(
        rename = "Uebernachtungen_Anzahl",
        deserialize_with = "de_as_i64_option"
    )]
    pub uebernachtungen_anzahl: Option<i64>,

    #[serde(
        rename = "Uebernachtungen_Veraenderung_zum_Vorjahreszeitraum_Prozent",
        deserialize_with = "de_as_f64_option"
    )]
    pub uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent: Option<f64>,

    #[serde(
        rename = "Durchsch_Aufenthaltsdauer_Tage",
        deserialize_with = "de_as_f64_option"
    )]
    pub durchsch_aufenthaltsdauer_tage: Option<f64>,
}

pub async fn parse_range<T: DeserializeOwned + Serialize>(
    data: Range<Data>,
) -> Result<Vec<T>, anyhow::Error> {
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
        let hashmap: HashMap<String, Value> = headers
            .iter()
            .cloned()
            .zip(row.iter().map(|x1| into_value(x1.clone())))
            .collect();
        info!("{:?}", hashmap);

        let serialized = serde_json::to_string(&hashmap).unwrap();
        let deserialized: T = serde_json::from_str(&serialized).unwrap();

        data.push(deserialized);
    }
    Ok(data)
}

fn de_as_i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::Number(n) => {
            if n.is_f64() {
                Ok(n.as_f64().unwrap() as i64)
            } else {
                Ok(n.as_i64().unwrap())
            }
        }
        _ => Err(de::Error::custom("wrong type")),
    }
}

fn de_as_i64_option<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<i64>, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::Number(num) => {
            // Parse as Option<i64> removing any decimal parts.
            let value = if num.is_f64() {
                num.as_f64().unwrap() as i64
            } else {
                num.as_i64().unwrap()
            };
            Some(value)
        }
        Value::String(s) => {
            if s.eq(".") || s.eq("-") || s.eq("X") {
                None
            } else {
                Some(s.parse().map_err(de::Error::custom)?)
            }
        }
        _ => return Err(de::Error::custom("wrong type")),
    })
}

fn de_as_f64_option<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<f64>, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::Number(num) => {
            // Parse as Option<f64> removing any decimal parts.
            let value = if num.is_f64() {
                num.as_f64().unwrap()
            } else {
                num.as_i64().unwrap() as f64
            };
            Some(value)
        }
        Value::String(s) => {
            if s.eq(".") || s.eq("-") || s.eq("X") {
                None
            } else {
                Some(s.parse().map_err(de::Error::custom)?)
            }
        }
        _ => return Err(de::Error::custom("wrong type")),
    })
}

pub fn into_value(data: Data) -> Value {
    match data {
        Data::Empty => Value::Null,
        Data::Error(ref e) => Value::String(e.to_string()),
        Data::Bool(b) => Value::from(b),
        Data::Int(i) => Value::from(i),
        Data::Float(f) => Value::from(f),
        Data::String(s) => Value::from(s.to_string()),
        Data::DateTime(dt) => Value::from(dt.to_string()),

        Data::DateTimeIso(x) => Value::from(x.to_string()),
        Data::DurationIso(x) => Value::from(x.to_string()),
    }
}
