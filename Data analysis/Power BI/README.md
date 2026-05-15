## Data Cleaning and Preparation for Power BI

This notebook prepares the jewelry review dataset for use in a Power BI dashboard. The raw data is stored in yearly folders as Parquet files, and the script processes all available files year by year.

Cleaning and preprocessing steps:

- Loaded only the required columns from each Parquet file:
  `parent_asin`, `brand`, `average_rating`, `rating_number`, `price`, `rating`, `timestamp`, and `categories`.

- Removed rows with missing values in the required fields.

- Cleaned text columns by converting them to string format and removing extra spaces.

- Converted numeric columns to valid numeric format:
  - `average_rating`
  - `rating_number`
  - `price`
  - `rating`

- Converted the `timestamp` column to datetime format.

- Extracted jewelry category information from the `categories` column:
  - `Jewelry_type` was taken from the second-to-last category.
  - `Jewelry_subtype` was taken from the last category.

- Removed the original `categories` column after extracting the required category information.

- Kept only valid product records using the following conditions:
  - `rating_number` must be greater than 0.
  - `average_rating` must be between 0 and 5.
  - `price` must be greater than 0.

- Extracted `year` and `month` from the review timestamp to support time-based analysis in Power BI.

- Removed rows with missing values after all transformations.

- Selected and saved the final cleaned columns:
  - `parent_asin`
  - `brand`
  - `average_rating`
  - `rating_number`
  - `price`
  - `rating`
  - `Jewelry_type`
  - `Jewelry_subtype`
  - `year`
  - `month`

- Saved the final cleaned dataset as a single Parquet file:
  `powerbi_data.parquet`

The final dataset is optimized for Power BI and can be used for dashboard analysis.
