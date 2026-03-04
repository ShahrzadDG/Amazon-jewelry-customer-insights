import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

jewelry_meta_path = "/beegfs/dehghani/NLP/Amazon_review/jewelry_meta.parquet"
jewelry_meta = pd.read_parquet(jewelry_meta_path)  
jewelry_meta = jewelry_meta.dropna(subset=["parent_asin"])
jewelry_meta["parent_asin"] = jewelry_meta["parent_asin"].astype(str).str.strip()

jewelry_meta_small = jewelry_meta[["parent_asin", "categories"]]
# print(jewelry_meta_small["parent_asin"].head(5).tolist())

review_path = "/beegfs/dehghani/NLP/Amazon_review/Clothing_Shoes_and_Jewelry.jsonl"
out_dir = "/beegfs/dehghani/NLP/Amazon_review/"
out_parquet_dir = os.path.join(out_dir, "Jewelry_review")


if os.path.exists(out_parquet_dir):
    shutil.rmtree(out_parquet_dir)
os.makedirs(out_parquet_dir, exist_ok=True)

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
    out = merged[["parent_asin", "categories", "rating", "title", "text", "timestamp"]].rename(columns={"text": "review"})

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
        table = pa.Table.from_pandas(part_df, preserve_index=False)

        key = (int(y), int(m))
        if key not in writers:
            writers[key] = pq.ParquetWriter(file_path, table.schema, compression="snappy")

        writers[key].write_table(table)
        total_matches += table.num_rows
    

    # pq.write_to_dataset(table,root_path=out_parquet_dir,partition_cols=["year", "month"],compression="snappy",)

    total_matches += len(out)
    if chunk_i%10==0:
        print(chunk_i, total_matches)

for w in writers.values():
    w.close()

print(total_matches)
