This project's goal is to extract insights from Amazon jewelry reviews using data analysis and natural language processing techniques.

**Dataset Source**

The data used in this project came from the Amazon Review Dataset (2023) provided by McAuley Lab:

https://amazon-reviews-2023.github.io/

From the available categories, the dataset Clothing, Shoes and Jewelry was downloaded. 

Since this dataset contains multiple product types, additional processing was performed to extract only jewelry-related products and their corresponding reviews.

For details on data preparation, see: [data prepration](https://github.com/ShahrzadDG/Amazon-jewelry-customer-insights/tree/main/Data_Preparation)

**Data Analysis part**

This project analyzed customer reviews from the jewelry subset dataset of Amazon. The analysis began with exploratory data analysis (EDA) to better understand the data. Brand performance, rating distributions, and trends across price segments over the period 2004–2023 were examined. Natural language processing (NLP) was used to extract complaint patterns and keyword trends from review text. And regression analysis was applied to quantify the impact of different complaint types on product ratings.Key performance indicators (KPIs) were defined to benchmark brand satisfaction, complaint severity, and value-for-money across different price segments. 

The analysing codes and results are provided in [data analysis](https://github.com/ShahrzadDG/Amazon-jewelry-customer-insights/tree/main/Data%20analysis) folder.


**Data Science Part**

This part of the project focused on applying natural language processing (NLP) techniques to extract insights from review data.

So far, a transformer-based sentiment classification model was developed using DistilBERT. Reviews were classified into negative, neutral, and positive categories based on their ratings. An additional experiment was conducted to evaluate whether RoBERTa could improve performance on the neutral class, which is typically more difficult to classify. 

The more detailed information, codes, and results are in the [data science](https://github.com/ShahrzadDG/Amazon-jewelry-customer-insights/tree/main/Data%20science) folder.

Further steps of this project include (In progress):

- Topic modeling to identify dominant themes in reviews

- Aspect-based sentiment analysis (e.g., durability, design, size)

- LLM-based summarization

- Early product success prediction

- Fake review detection




