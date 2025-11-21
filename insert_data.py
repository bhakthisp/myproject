import mysql.connector
from datasets import load_dataset
import pandas as pd
import numpy as np

print("Loading FreshRetailNet-50K dataset...")
dataset = load_dataset("Dingdong-Inc/FreshRetailNet-50K")

# Convert train split to pandas DataFrame
df = dataset["train"].to_pandas().head(50000)
print("Dataset loaded, taking 50,000 rows.")

# Select only the scalar columns
cols = [
    "city_id", "store_id", "management_group_id",
    "first_category_id", "second_category_id", "third_category_id",
    "product_id", "dt", "sale_amount", "stock_hour6_22_cnt",
    "discount", "holiday_flag", "activity_flag",
    "precpt", "avg_temperature", "avg_humidity", "avg_wind_level"
]
df = df[cols]

# Replace NaN with None (for MySQL)
df = df.replace({np.nan: None})

# Convert Numpy types to native Python
def convert_value(x):
    if isinstance(x, (np.integer, np.floating)):
        return x.item()
    return x

# Convert date column (dt) to proper Python date or string
# If dt is like "2024-03-28", you can keep as string or convert to date
# Here we keep as string
df["dt"] = df["dt"].astype(str)

data = []
for row in df.itertuples(index=False, name=None):
    cleaned = tuple(convert_value(v) for v in row)
    data.append(cleaned)

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Bhakthi@13",   # change if yours is different
    database="smartstock"
)
cursor = conn.cursor()

# Build insert query
column_names = ", ".join(cols)
placeholders = ", ".join(["%s"] * len(cols))
query = f"INSERT INTO retail_data ({column_names}) VALUES ({placeholders})"

print("Inserting into MySQL...")
batch_size = 5000
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    cursor.executemany(query, batch)
    conn.commit()
    print(f"Inserted rows {i+1} to {i+len(batch)}")

print("Done inserting 50,000 rows.")
cursor.close()
conn.close()
