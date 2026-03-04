
The preprocessing pipeline consists of two main stages:

1. Identifying Jewelry Products:
   
   Parent_asin_of_jewelries.py
   
   Input: Amazon Clothing, Shoes & Jewelry metadata file (meta_Clothing_Shoes_and_Jewelry.jsonl)
   
   Method:
   
      1. Metadata filtering (select only jewelry items)
   
      2. Providing jewelry product list (store their unique parent_asin identifiers) 
   
2. Extracting Reviews for Jewelry Products:
   
   Jewelry_reviews.py
   
   Input: "Clothing_Shoes_and_Jewelry.jsonl" file and "jewelry_meta.parquet" which is output of the step 1.
   
   Method:
   
      1. Review filtering (keep only jewelry reviews)
   
      2. Structured parquet dataset partitioned by year/month
