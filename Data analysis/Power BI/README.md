## Data Cleaning and Preparation for Power BI

This notebook prepares the jewelry review dataset for use in a Power BI dashboard. The raw data is stored in yearly folders as Parquet files, and the script processes all available files year by year.

Cleaning and preprocessing steps:

- Loaded only the required columns from each Parquet file:
  `parent_asin`, `brand`, `average_rating`, `rating_number`, `price`, `rating`, `timestamp`, and `categories`.

- Removed rows with missing values in the required fields.

- Cleaned text columns by converting them to string format and removing extra spaces.

- Converted numeric columns to valid numeric format:
  - `average_rating`, `rating_number`, `price`, `rating`

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

## Exploratory Data Analysis (EDA)

This notebook performs an exploratory data analysis (EDA) on the cleaned jewelry dataset prepared for the Power BI dashboard. The goal of the analysis was to better understand the dataset structure, identify trends and anomalies, and validate the quality of the cleaned data before building visual dashboards.

The analysis started with a general inspection of the dataset by:

- Loading the cleaned Parquet dataset
- Checking column names and data types
- Examining dataset dimensions
- Generating descriptive statistics
- Detecting missing values
- Checking for duplicate rows

During the EDA process, additional cleaning and category standardization were performed:

- Removed non-jewelry categories such as:
  - `Girls`, `Boys`, `Women`, `Men`, `Clothing, Shoes & Jewelry`

- Merged similar jewelry categories into unified labels for better analysis consistency:
  - `Charms & Charm Bracelets` -->  `Charm Bracelets`
  - `Necklaces & Pendants` --> `Necklaces`
  - `Pendants & Coins` --> `Necklaces`
  - `Piercing Jewelry` --> `Body Jewelry`
  - `Shoe, Jewelry & Watch Accessories` --> `Watch Accessories`

- Saved the refined dataset as: `cleaned_powerBI_data.parquet`

The notebook includes several visual and statistical analyses to understand the dataset:

Product distribution over time:

- Counted the number of unique products (`parent_asin`) per year
- Visualized yearly product growth using bar charts

Price analysis:

- Investigated minimum and maximum prices
- Analyzed price distribution using:
  - Histogram of log-prices
  - KDE (Kernel Density Estimation) plots
- Limited extreme outliers for better visualization clarity

Time vs price trends:

- Explored how jewelry prices changed over the years
- Created scatter plots of:
  - `year` vs `price`
- Compared price evolution for selected jewelry categories such as:
  - Rings
  - Necklaces

Relationship analysis:

- Used pair plots to analyze relationships between:
  - `price`
  - `average_rating`
  - `rating_number`
  - `year`

- Calculated and visualized correlation matrices using heatmaps

Category-level insights:

- Computed:
  - Average price per jewelry type
  - Maximum price per jewelry type

- Used boxplots to compare price distributions across jewelry categories

This exploratory analysis helped:

- Validate the cleaned dataset
- Understand product and pricing trends
- Identify category imbalances and outliers
- Discover relationships between ratings, popularity, price, and time
- Prepare meaningful KPIs and visuals for the Power BI dashboard


## Amazon Jewelry Market Dashboard

The dashboard provides insights into product distribution, pricing behavior, customer ratings, brand performance, and jewelry category analysis across multiple years. The objective of this project was to transform raw marketplace data into a clear and interactive business intelligence solution that supports exploratory analysis and decision making.

The dashboard includes:

- KPI Summary Cards
- Total number of distinct products
- Total number of distinct brands
- Number of jewelry categories
- Minimum and maximum product prices
- Interactive Filtering
- Rating range
- Year range
- Price range
- Brand selection
- Jewelry type and subtype filtering
- Trend Analysis
- Median jewelry price evolution over time
- Category Analysis
- Median price distribution across jewelry types
- Brand Performance Analysis
- Number of customer ratings per brand
- Average product ratings by jewelry category
