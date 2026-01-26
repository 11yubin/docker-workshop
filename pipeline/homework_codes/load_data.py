# hw
import pandas as pd
from sqlalchemy import create_engine

# DB
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Status check
print("Downloading")
# Green Taxi Data (Parquet)
url_green = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
df_green = pd.read_parquet(url_green)

# Zones Data (CSV)
url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
df_zones = pd.read_csv(url_zones)

print("Data to DB")
# Green Taxi -> 'green_taxi_data'
df_green.to_sql(name='green_taxi_data', con=engine, if_exists='replace', index=False)

# Zones -> 'zones'
df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)

print("Complete")