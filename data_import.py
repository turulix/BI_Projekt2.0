import glob
import os

import pandas as pd
import sqlalchemy
import tqdm

DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")


def main():
    conn = sqlalchemy.create_engine("postgresql://{}:{}@{}/{}".format(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME))
    product_files = glob.glob("./data/**/produkt_klima*.txt", recursive=True)
    for file in tqdm.tqdm(product_files):
        df = pd.read_csv(file, sep=";", encoding="UTF-8")
        # Trim whitespace from column names
        df.columns = df.columns.str.strip()

        # convert "MESS_DATUM" to datetime
        df["MESS_DATUM"] = pd.to_datetime(df["MESS_DATUM"], format="%Y%m%d")
        df["MESS_DATUM"] = df["MESS_DATUM"].dt.date

        df.to_sql("weather_data", conn, schema="original_data", if_exists="append", index=False)


if __name__ == "__main__":
    main()
