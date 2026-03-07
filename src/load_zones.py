# Loads NYC Taxi Zone lookup CSV into analytics.dim_zone
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Read CSV
df = pd.read_csv(
    r"D:\Data Engineering\NYC\src\data\taxi_zone_lookup.csv",
    usecols=['LocationID', 'Zone', 'Borough'],
    dtype={'LocationID': 'int16'}
)

# Rename columns
df.columns = ['location_id', 'zone_name', 'borough']
df['zone_code'] = None

# Fill nulls with Unknown
df['zone_name'] = df['zone_name'].fillna('Unknown')
df['borough']   = df['borough'].fillna('Unknown')

# Reorder columns
df = df[['location_id', 'zone_code', 'zone_name', 'borough']]

print(f"Zones to load: {len(df):,}")
print(df.tail())  # check the problematic rows at the end

# Truncate first then insert
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE analytics.dim_zone CASCADE"))

df.to_sql(
    'dim_zone',
    engine,
    schema='analytics',
    if_exists='append',
    index=False
)

print(f"Successfully loaded {len(df):,} zones into analytics.dim_zone")