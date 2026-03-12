import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

jewelry_meta_path = "/.../jewelry_meta.parquet"
jewelry_meta = pd.read_parquet(jewelry_meta_path)  
jewelry_meta = jewelry_meta.dropna(subset=["parent_asin"])
jewelry_meta["parent_asin"] = jewelry_meta["parent_asin"].astype(str).str.strip()

jewelry_meta_small = jewelry_meta[["parent_asin", "brand", "brand_name", "manufacturer", "title", "average_rating", "rating_number", "price", "categories"]].copy()
jewelry_meta_small = jewelry_meta_small.rename(columns={"title": "product_title"})
# print(jewelry_meta_small["parent_asin"].head(5).tolist())

review_path = "/beegfs/dehghani/NLP/Amazon2023/Clothing_Shoes_and_Jewelry.jsonl"
out_dir = "/beegfs/dehghani/NLP/Amazon2023/"
out_parquet_dir = os.path.join(out_dir, "Jewelry_review_files")

if os.path.exists(out_parquet_dir):
    shutil.rmtree(out_parquet_dir)
os.makedirs(out_parquet_dir, exist_ok=True)

schema = pa.schema([
    ("parent_asin", pa.string()),
    ("brand", pa.string()),
    ("brand_name", pa.string()),
    ("manufacturer", pa.string()),
    ("product_title", pa.string()),
    ("average_rating", pa.float64()),
    ("rating_number", pa.float64()),
    ("price", pa.float64()),
    ("categories", pa.list_(pa.string())),
    ("rating", pa.float64()),
    ("review_title", pa.string()),
    ("review", pa.string()),
    ("asin", pa.string()),
    ("timestamp", pa.timestamp("ns", tz="UTC")),
])

df_review = pd.read_json(review_path, lines=True, chunksize=100000)
total_matches = 0
chunk_i = 0
writers={}
for Review_chunk in df_review:
    chunk_i+=1
    Review_chunk["parent_asin"] = Review_chunk["parent_asin"].astype(str).str.strip()
    merged = Review_chunk.merge(jewelry_meta_small, on="parent_asin", how="inner")
    if merged.empty:
        continue
    out = merged[["parent_asin", "brand", "brand_name", "manufacturer", "product_title", "average_rating", "rating_number", "price", "categories", "rating", "title", "text", "asin", "timestamp"]].rename(columns={"title": "review_title", "text": "review"})

    out["timestamp"]=pd.to_datetime(out["timestamp"], errors="coerce", utc=True)

    out["year"] = out["timestamp"].dt.year
    out["month"] = out["timestamp"].dt.month

    out = out.dropna(subset=["year", "month"])
    if out.empty:
        continue

    out["year"]=out["year"].astype(int)
    out["month"]= out["month"].astype(int)

    for (y, m), part_df in out.groupby(["year", "month"], sort=False):
        part_dir = os.path.join(out_parquet_dir, f"year={y}", f"month={m}")
        os.makedirs(part_dir, exist_ok=True)
        file_path = os.path.join(part_dir, "reviews.parquet") 

        part_df = part_df.drop(columns=["year", "month"])

        out["parent_asin"] = out["parent_asin"].astype("string")
        out["brand"] = out["brand"].astype("string")
        out["brand_name"] = out["brand_name"].astype("string")
        out["manufacturer"] = out["manufacturer"].astype("string")
        out["product_title"]= out["product_title"].astype("string")
        out["review_title"]= out["review_title"].astype("string")
        out["review"] = out["review"].astype("string")
        out["asin"] = out["asin"].astype("string")
        out["average_rating"]= pd.to_numeric(out["average_rating"], errors="coerce")
        out["rating_number"] = pd.to_numeric(out["rating_number"], errors="coerce")
        out["price"]= pd.to_numeric(out["price"], errors="coerce")
        out["rating"] = pd.to_numeric(out["rating"], errors="coerce")

        table = pa.Table.from_pandas(part_df, schema=schema, preserve_index=False)

        key = (int(y), int(m))
        if key not in writers:
            writers[key] = pq.ParquetWriter(file_path, schema, compression="snappy")

        writers[key].write_table(table)
        total_matches += table.num_rows
    
    # table = pa.Table.from_pandas(out, preserve_index=False)

    # pq.write_to_dataset(table,root_path=out_parquet_dir,partition_cols=["year", "month"],compression="snappy",)

    if chunk_i%10==0:
        print(chunk_i, total_matches)

for w in writers.values():
    w.close()

print(total_matches)
