This project aims to extract insights from Amazon jewelry reviews using data analysis and natural language processing techniques.

**Dataset Source**

The data used in this project comes from the Amazon Review Dataset (2023) provided by McAuley Lab:

https://amazon-reviews-2023.github.io/

From the available categories, the dataset Clothing, Shoes and Jewelry was downloaded. 

Since this dataset contains multiple product types, additional processing was performed to extract only the jewelry-related products and their corresponding reviews.

**Data Analysis part**

This project analyzes customer reviews from jewelry subset dataset of Amazon. The analysis begins with exploratory data analysis (EDA) to better understand the data. Brand performance, rating distributions, trends over time, and price segments are examined. The goal is to extract insights from large-scale text review and rating data and identify patterns in customer sentiment, preferences, and common issues.

The analysis resulted in findings on customer satisfaction trends, frequently mentioned product features, and sentiment distributions. These insights can be used to better understand customer expectations and support data-driven decisions in product development and marketing.


**Data Science Part**

This part of the project focuses on applying natural language processing (NLP) techniques to extract insights from review data.

So far, a transformer-based sentiment classification model has been developed using DistilBERT. Reviews are classified into negative, neutral, and positive categories based on their ratings. An additional experiment was conducted to evaluate whether RoBERTa could improve performance on the neutral class, which is typically more difficult to classify. 

Further steps of this project includes (In progress):

- Topic modeling to identify dominant themes in reviews

- Aspect-based sentiment analysis (e.g., durability, design, size)

- LLM-based summarization

- Early product success prediction

- Fake review detection




