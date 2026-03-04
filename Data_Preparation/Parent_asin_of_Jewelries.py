import os 
import pandas as pd
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

pd.set_option("display.max_columns", None)    
pd.set_option("display.max_colwidth", None)   
pd.set_option("display.width", 2000) 
pd.set_option("display.max_rows", 10)

meta_path = "/.../Amazon_review/meta_Clothing_Shoes_and_Jewelry.jsonl"
out_path = "/.../NLP/Amazon_review/jewelry_meta.parquet"
if os.path.isdir(out_path):
    shutil.rmtree(out_path)
def is_jewelry(categories):
    try:
        subcats = categories[1:]  
        return any("jewelry" in x.lower() for x in subcats)
    except:
        return False
        
df_meta=pd.read_json(meta_path,lines=True, chunksize=1000)
jewelry_set = set()
writer=None
for chunk in df_meta:
    mask = chunk['categories'].apply(is_jewelry)
    jewelry_chunk = chunk.loc[mask, ["parent_asin", "categories"]].dropna(subset=["parent_asin"])
    new_rows=[]
    for pasin, cats in zip(jewelry_chunk["parent_asin"].tolist(), jewelry_chunk["categories"].tolist()):
        if pasin not in jewelry_set:
            print(pasin)
            jewelry_set.add(pasin)
            new_rows.append((pasin, cats))
    if not new_rows:
        continue

    out_df = pd.DataFrame(new_rows, columns=["parent_asin", "categories"])
    table = pa.Table.from_pandas(out_df, preserve_index=False)

    if writer is None:
        writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")

    writer.write_table(table)

if writer is not None:
    writer.close()

print(len(jewelry_set))
