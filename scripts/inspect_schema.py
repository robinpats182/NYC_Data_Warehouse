# import pandas as pd
import pyarrow.dataset as ds


# df = pd.read_parquet("../src/data/fhvhv_tripdata_2026-01.parquet")
dataset = ds.dataset(r"D:\Data Engineering\NYC\src\data\fhvhv_tripdata_2026-01.parquet", format="parquet")

# print(df.head())
# print(df.dtypes)
# print(df.isnull().sum())
print(f"Total rows: {dataset.count_rows():,}")