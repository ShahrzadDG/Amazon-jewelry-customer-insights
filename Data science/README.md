This project aims to build an pipeline for extracting insights from large-scale review data from Amazon jwelery using NLP techniques. The overall goal is to do sentiment classification and provide deeper understanding through topic modeling, aspect-based sentiment analysis, and LLM-driven summarization. To ultimately being enable to perform early product success prediction and detect of potentially fake reviews.

Completed (so far):
Sentiment Classification

A transformer-based sentiment classification system has been developed using DistilBERT and RoBERTa.

Reviews are classified into negative, neutral, and positive classes
Labels are derived from rating scores
Stratified data splitting is used to preserve class distribution
Evaluation includes:
accuracy
macro-averaged precision, recall, and F1-score
confusion matrix and per-class analysis

A focused experiment was conducted to evaluate whether RoBERTa could improve performance on the neutral class, but results indicate that the main limitation lies in the dataset (class imbalance and label ambiguity) rather than the model architecture.

In Progress
Topic Modeling
Extracting dominant themes and topics from reviews
Goal: identify recurring patterns (e.g., complaints, praises)
Aspect-Based Sentiment Analysis
Moving from overall sentiment → fine-grained sentiment
Example aspects:
durability
design
size
Goal: understand what exactly users like or dislike
