"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    # Set PyArrow timezone database location for Windows
    import platform
    if platform.system() == 'Windows':
        try:
            import certifi
            tzdata_path = os.path.join(os.path.dirname(certifi.__file__), 'tzdata')
            os.environ.setdefault('ARROW_TIMEZONE_DATABASE', tzdata_path)
        except:
            pass
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Parse dates
    start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    # Generate list of months between start and end dates
    current_date = start.replace(day=1)
    end_date_month = end.replace(day=1)
    
    all_dfs = []
    
    while current_date <= end_date_month:
        year = current_date.year
        month = current_date.strftime("%m")
        
        for taxi_type in taxi_types:
            # Fetch parquet files from:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                print(f"Fetching {url}")
                df = pd.read_parquet(url)
                
                # Standardize column names (yellow/green taxi have different naming)
                if 'tpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'tpep_pickup_datetime': 'pickup_datetime',
                        'tpep_dropoff_datetime': 'dropoff_datetime'
                    })
                elif 'lpep_pickup_datetime' in df.columns:
                    df = df.rename(columns={
                        'lpep_pickup_datetime': 'pickup_datetime',
                        'lpep_dropoff_datetime': 'dropoff_datetime'
                    })
                
                # Filter by date range
                if 'pickup_datetime' in df.columns:
                    df = df[(df['pickup_datetime'] >= start) & (df['pickup_datetime'] < end)]
                    
                    # Convert ALL timezone-aware timestamp columns to naive to avoid PyArrow timezone issues
                    for col in df.columns:
                        if pd.api.types.is_datetime64tz_dtype(df[col]):
                            df[col] = df[col].dt.tz_localize(None)
                    
                if len(df) > 0:
                    all_dfs.append(df)
                    print(f"Loaded {len(df)} rows from {taxi_type} taxi for {year}-{month}")
            except Exception as e:
                print(f"Failed to load {url}: {e}")
        
        # Move to next month
        current_date += relativedelta(months=1)
    
    if all_dfs:
        final_dataframe = pd.concat(all_dfs, ignore_index=True)
        print(f"Total rows: {len(final_dataframe)}")
        
        # Create a new dataframe from dict to completely remove PyArrow metadata
        # This prevents PyArrow timezone issues
        data_dict = {}
        for col in final_dataframe.columns:
            if pd.api.types.is_datetime64_any_dtype(final_dataframe[col]):
                # Convert datetime to Python datetime objects
                data_dict[col] = final_dataframe[col].dt.to_pydatetime()
            else:
                data_dict[col] = final_dataframe[col].values
        
        final_dataframe = pd.DataFrame(data_dict)
    else:
        # Return empty dataframe with required columns if no data found
        final_dataframe = pd.DataFrame(columns=['pickup_datetime', 'dropoff_datetime'])
    
    return final_dataframe