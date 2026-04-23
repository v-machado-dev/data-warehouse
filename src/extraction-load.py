# Importing Libraries

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Loads environment variables from .env file
load_dotenv()

# Returns database configuration from environment variables

def get_db_config():
    return {
        "host": os.getenv("DB_HOST_PROD"),
        "port": os.getenv("DB_PORT_PROD"),
        "dbname": os.getenv("DB_NAME_PROD"),
        "user": os.getenv("DB_USER_PROD"),
        "password": os.getenv("DB_PASS_PROD"),
        "schema": os.getenv("DB_SCHEMA_PROD"),
        "database_url": (
            f"postgresql://{os.getenv('DB_USER_PROD')}:{os.getenv('DB_PASS_PROD')}"
            f"@{os.getenv('DB_HOST_PROD')}:{os.getenv('DB_PORT_PROD')}"
            f"/{os.getenv('DB_NAME_PROD')}"
        )    
    }

config = get_db_config()

# Chooses tickers 
symbols = ["GC=F", "SI=F", "CL=F", "BZ=F", "ZS=F", "KC=F"]

# Creates sqlalchemy engine
engine = create_engine(config["database_url"])


# Fetches historial price data for a given ticker

def fetch_df(symbol, period="2y", interval="1d"):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)[["Close"]]
    df["symbol"] = symbol
    return df


# Uses Pandas to conect to database

def connect_to_database(df, schema ="public"):
    df.to_sql("Assets", engine, schema=schema, if_exists='append', index=True, index_label="Date")



if __name__== "__main__": 
    all_data = []
    for symbol in symbols:
        df = fetch_df(symbol)
        all_data.append(df)

    final_df = pd.concat(all_data)
    connect_to_database(final_df)
    print("Data loaded successfully")