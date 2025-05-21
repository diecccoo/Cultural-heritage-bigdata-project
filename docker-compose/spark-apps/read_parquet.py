import pandas as pd

df = pd.read_parquet("rococo_metadata.parquet")

print(df.head(10))
print(df.columns)

