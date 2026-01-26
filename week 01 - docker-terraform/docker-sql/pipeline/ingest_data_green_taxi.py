import pandas as pd
from sqlalchemy import create_engine

# Database connection
engine = create_engine('postgresql://root:root@localhost:5435/ny_taxi')

# File URL
prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
file_name = 'green_tripdata_2025-11.parquet'
url = prefix + file_name

print("Downloading:", url)

# Read parquet (FULL)
df = pd.read_parquet(url)

print("Rows:", len(df))
print("Columns:", df.columns)

# Create table schema
df.head(0).to_sql(
    name='green_taxi_data',
    con=engine,
    if_exists='replace'
)

print("Table created")

# Insert data
df.to_sql(
    name='green_taxi_data',
    con=engine,
    if_exists='append',
    method='multi'
)

print("Data inserted successfully")
