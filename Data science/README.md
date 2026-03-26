This part of the project implements a sentiment classification pipeline for Amazon Jewelry reviews using transformer-based models, specifically DistilBERT and RoBERTa.

The dataset is first preprocessed by selecting relevant columns, removing missing values, and generating sentiment labels from rating column (negative, neutral, positive). The data is then split into training, validation, and test sets (). Text inputs are tokenized using the corresponding pretrained tokenizers for each model.

Two separate models (DistilBERT and RoBERTa) are fine-tuned for a 3-class classification task using the Hugging Face Transformers framework. During training, macro-averaged F1-score is used as the main evaluation metric to properly account for class imbalance ().

After training, both models are evaluated on a held-out test set. Predictions are generated and compared with ground-truth labels to compute performance metrics such as accuracy, precision, recall, and F1-score. The results are saved as CSV files, enabling further analysis such as confusion matrices, per-class performance, and error inspection.



