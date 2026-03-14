import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
import re

jewelry_meta_path = "/.../jewelry_meta.parquet"
review_path = "/.../Clothing_Shoes_and_Jewelry.jsonl"
out_dir = "/.../"
out_parquet_dir = os.path.join(out_dir, "Jewelry_review_files")

jewelry_meta = pd.read_parquet(jewelry_meta_path)
jewelry_meta = jewelry_meta.dropna(subset=["parent_asin"])
jewelry_meta["parent_asin"] = jewelry_meta["parent_asin"].astype(str).str.strip()

jewelry_meta_small = jewelry_meta[["parent_asin", "brand", "brand_name", "manufacturer", "title", "average_rating", "rating_number", "price", "categories"]].copy()
jewelry_meta_small = jewelry_meta_small.rename(columns={"title": "product_title"})
jewelry_meta_small = jewelry_meta_small.drop_duplicates(subset=["parent_asin"])
valid_parent_asins = set(jewelry_meta_small["parent_asin"])
jewelry_meta_small = jewelry_meta_small.set_index("parent_asin")

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
    ("categories", pa.string()),
    ("rating", pa.float64()),
    ("review_title", pa.string()),
    ("review", pa.string()),
    ("asin", pa.string()),
    ("timestamp", pa.timestamp("ns", tz="UTC")),
])

def get_next_file_number(part_dir):
    if not os.path.exists(part_dir):
        return 0
    pat = re.compile(r"reviews-(\d{4})\.parquet$")
    nums = []
    for f in os.listdir(part_dir):
        m = pat.match(f)
        if m:
            nums.append(int(m.group(1)))
    if not nums:
        return 0
    return max(nums) + 1

def close_all_writers(writers_dict):
    for w in writers_dict.values():
        w.close()
    writers_dict.clear()

writers = {}
total_matches = 0
chunk_i = 0
rotation_every=100
current_file_number=0

try:
    for review_chunk in pd.read_json(review_path, lines=True, chunksize=100000):
        chunk_i += 1
        t0 = time.time()
        if chunk_i > 1 and (chunk_i - 1) % rotation_every == 0:
            close_all_writers(writers)
            current_file_number += 1

        review_chunk["parent_asin"] =review_chunk["parent_asin"].astype(str).str.strip()
        review_chunk = review_chunk[review_chunk["parent_asin"].isin(valid_parent_asins)]
        if review_chunk.empty:
            continue

        merged = review_chunk.join(jewelry_meta_small, on="parent_asin", how="inner", validate="many_to_one")
        if merged.empty:
            continue
        
        out = merged[["parent_asin", "brand", "brand_name", "manufacturer", "product_title", "average_rating", "rating_number", "price", "categories", "rating", "title", "text", "asin", "timestamp"]].rename(columns={"title": "review_title", "text": "review"})

        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)

        out["year"] = out["timestamp"].dt.year
        #out["month"] = out["timestamp"].dt.month
        #out = out.dropna(subset=["year", "month"])  
        out = out.dropna(subset=["year"])        
        if out.empty:
            continue

        out["year"] = out["year"].astype(int)
        #out["month"] = out["month"].astype(int)

        str_cols = ["parent_asin", "brand", "brand_name", "manufacturer", "product_title", "categories", "review_title", "review", "asin"]
        for c in str_cols:
            out[c] = out[c].astype("string")
            
        num_cols = ["average_rating", "rating_number", "price", "rating"]
        for c in num_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        
        for y, part_df in out.groupby("year", sort=False):
        #for (y, m), part_df in out.groupby(["year", "month"], sort=False):
            #part_dir = os.path.join(out_parquet_dir, f"year={y}", f"month={m}")
            part_dir = os.path.join(out_parquet_dir, f"year={y}")

            os.makedirs(part_dir, exist_ok=True)
            #file_path = os.path.join(part_dir, "reviews.parquet")
            file_path = os.path.join(part_dir, f"reviews-{current_file_number:04d}.parquet")
            
            #part_df = part_df.drop(columns=["year", "month"]).copy()
            part_df = part_df.drop(columns=["year"]).copy()

            table = pa.Table.from_pandas(part_df, schema=schema, preserve_index=False)

            #key = (int(y), int(m), current_file_number)
            key = (int(y), current_file_number)
            if key not in writers:
                writers[key] = pq.ParquetWriter(file_path, schema, compression="snappy")

            writers[key].write_table(table)
            total_matches += table.num_rows

        print(f"Chunk {chunk_i}: done in {time.time() - t0:.2f}s | total_matches={total_matches}", flush=True)

finally:
    close_all_writers(writers)
    #for w in writers.values():
       # w.close()

print(total_matches)
