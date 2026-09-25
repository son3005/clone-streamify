import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS streamify_stg;"))
    conn.commit()
    print("Schema streamify_stg created")

# Events list from Kafka
EVENTS = ["listen_events", "page_view_events", "auth_events"]

for event in EVENTS:
    files = glob.glob(f"data_lake/{event}/**/*.parquet", recursive=True)
    
    valid_files = [f for f in files if os.path.getsize(f) > 0]
    
    if not valid_files:
        print(f"[INFO] No valid files found for {event}")
        continue 
    
    print(f"[INFO] Found {len(valid_files)} valid files for {event}")
    df_list = [pd.read_parquet(f) for f in valid_files]
    df = pd.concat(df_list, ignore_index=True)
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(name=event, con=engine, schema="streamify_stg", if_exists="replace", index=False)
    print(f"[INFO] Loaded {len(df)} rows for {event} to streamify_stg.{event}")
print("[INFO] Load to Postgres completed")

    