import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection
engine = create_engine("postgresql://root:root@localhost:5435/ny_taxi")

# File URL
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

print("Downloading:", url)

# Read CSV (small file)
df = pd.read_csv(url)

print("Preview:")
print(df.head())

print("\nSchema:")
print(df.dtypes)

print("\nRows:", len(df))

# Ingest to Postgres
df.to_sql(
    name="taxi_zone_lookup",
    con=engine,
    if_exists="replace",
    index=False
)

print("\n✅ taxi_zone_lookup successfully ingested")
print("Table 'taxi_zone_lookup' created in PostgreSQL database.")