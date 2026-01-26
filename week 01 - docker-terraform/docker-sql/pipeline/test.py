import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('postgresql://root:root@localhost:5435/ny_taxi')

# File prefix & name
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'
file_name = '/taxi_zone_lookup.csv'

# Print info file
print("Prefix URL :", prefix)
print("File name  :", file_name)
print("Full URL  :", prefix + file_name)

# Read a sample of the data
df = pd.read_csv(prefix + file_name, nrows=100)

# Display first rows
df.head()

# Check data types
df.dtypes

# Check data shape
df.shape

# Define data types
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Read again with schema
df = pd.read_csv(
    prefix + file_name,
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)

# Create table schema
df.head(n=0).to_sql(
    name='yellow_taxi_data',
    con=engine,
    if_exists='replace'
)

# Iterator for full load
df_iter = pd.read_csv(
    prefix + file_name,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)

for df_chunk in df_iter:
    print(len(df_chunk))

df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append') 

first = True

for df_chunk in df_iter:

    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))

print("CSV iterator initialized successfully")
