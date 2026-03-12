
The preprocessing pipeline consists of two main stages:

1. Identifying Jewelry Products:
   
   Parent_asin_of_meta.py
   
   Input: Amazon Clothing, Shoes & Jewelry metadata file (meta_Clothing_Shoes_and_Jewelry.jsonl)
   
   Method:
   
      1. Filter the metadata to keep only items that belong to jewelry-related categories.
   
      2. Extract the corresponding jewelry products and store their unique parent_asin identifiers.
  
      3. Collect additional useful metadata such as the product title, average rating, rating number, price, brand, and manufacturer, and store the results in a structured dataset.
   
2. Extracting Reviews for Jewelry Products:
   
   Jewelry_reviews.py
   
   Input: "Clothing_Shoes_and_Jewelry.jsonl" file and "jewelry_meta.parquet" which is output of the step 1.
   
   Method:
   
      1. Filter the reviews to keep only those associated with the jewelry products identified in the previous step.
  
      2. The final dataset is stored in a parquet format and partitioned by year and month. Each year/month directory contains a reviews.parquet file with the processed review data.
         
output/
 ├── year=YYYY/
 │   ├── month=MM/
 │   │   └── reviews.parquet
 

The reviews.parquet file includes the following columns: parent_asin, brand, brand_name, manufacturer, product_title, average_rating, rating_number, price, categories, rating, review_title, review, asin, timestamp
