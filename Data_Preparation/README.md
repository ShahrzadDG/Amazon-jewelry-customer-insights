
The preprocessing pipeline consists of two main stages:

1. Identifying Jewelry Products: Parent_asin_of_jewelries.py
2. Extracting Reviews for Jewelry Products: Jewelry_reviews.py

The workflow is:

Via Parent_asin_of_jewelries.py: Amazon Clothing, Shoes & Jewelry Dataset -->  Metadata Filtering (Select only jewelry items) --> Jewelry Product List (parent_asin) 

Via Jewelry_reviews.py --> Review Filtering (Keep only jewelry reviews) --> Structured Parquet Dataset partitioned by year/month
