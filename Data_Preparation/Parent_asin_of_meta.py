import os 
import pandas as pd
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

pd.set_option("display.max_columns", None)    
pd.set_option("display.max_colwidth", None)   
pd.set_option("display.width", 2000) 
pd.set_option("display.max_rows", 10)

meta_path = "/.../meta_Clothing_Shoes_and_Jewelry.jsonl"
out_path = "/.../jewelry_meta.parquet"
if os.path.isdir(out_path):
    shutil.rmtree(out_path)
    
def is_jewelry(categories):
    try:
        subcats = categories[1:]  
        return any("jewelry" in x.lower() for x in subcats)
    except:
        return False

def extract_brand(row):
    details = row.get("details", None)
    brand = None
    brand_name = None
    manufacturer = None
    if isinstance(details, dict):
        brand = details.get("Brand")
        brand_name = details.get("Brand Name")
        manufacturer = details.get("Manufacturer")
    return brand, brand_name, manufacturer

df_meta=pd.read_json(meta_path,lines=True, chunksize=1000)
jewelry_set = set()
writer=None
for chunk in df_meta:
    mask = chunk['categories'].apply(is_jewelry)
    jewelry_chunk = chunk.loc[mask, ["parent_asin", "title", "average_rating", "rating_number", "price", "details", "categories"]].dropna(subset=["parent_asin"])
    # jewelry_chunk = jewelry_chunk.drop_duplicates(subset=["parent_asin"])
    new_rows=[]
    for _, row in jewelry_chunk.iterrows():
        pasin = str(row["parent_asin"]).strip()

        if pasin not in jewelry_set:
            print(pasin)
            jewelry_set.add(pasin)
            brand, brand_name, manufacturer = extract_brand(row)
            new_rows.append({
                "parent_asin": pasin,
                "brand": brand,
                "brand_name": brand_name,
                "manufacturer": manufacturer,
                "title": row["title"],
                "average_rating": row["average_rating"],
                "rating_number": row["rating_number"],
                "price": row["price"],
                "categories": row["categories"],
            })
    
    if not new_rows:
        continue

    out_df = pd.DataFrame(new_rows, columns=["parent_asin", "brand", "brand_name", "manufacturer", "title", "average_rating", "rating_number", "price", "categories"])
    out_df["price"] = pd.to_numeric(out_df["price"], errors="coerce")
    out_df["average_rating"] = pd.to_numeric(out_df["average_rating"], errors="coerce")
    out_df["rating_number"] = pd.to_numeric(out_df["rating_number"], errors="coerce")
    out_df["parent_asin"] = out_df["parent_asin"].astype("string")
    out_df["brand"] = out_df["brand"].astype("string")
    out_df["brand_name"] = out_df["brand_name"].astype("string")
    out_df["manufacturer"] = out_df["manufacturer"].astype("string")
    out_df["title"] = out_df["title"].astype("string")
    
    schema = pa.schema([
        ("parent_asin", pa.string()),
        ("brand", pa.string()),
        ("brand_name", pa.string()),
        ("manufacturer", pa.string()),
        ("title", pa.string()),
        ("average_rating", pa.float64()),
        ("rating_number", pa.int64()),
        ("price", pa.float64()),
        ("categories", pa.list_(pa.string())),
    ])
    table = pa.Table.from_pandas(out_df, schema=schema, preserve_index=False)
    
    if writer is None:
        writer = pq.ParquetWriter(out_path, schema, compression="snappy")

    writer.write_table(table)

if writer is not None:
    writer.close()

print(len(jewelry_set))
