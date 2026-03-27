The python code here is a sentiment analysis pipeline using Hugging Face Transformers and a pretrained DistilBERT model. The goal is to classify Amazon jewelry product reviews into three sentiment categories: negative, neutral, and positive.

First the Amazon review data that is stored in multiple .parquet files and organized by year is loaded. For each file, the relevant columns such as ratings, review titles, and review text are extracted. The data is then cleaned by removing invalid ratings, handling missing values, and combining the title and review into a single text field to create a more informative input for the model.

Next, sentiment labels are generated based on the reviewer's rating. Reviews with low ratings are classified as negative, mid-range ratings as neutral, and high ratings as positive. These labels are encoded into numerical form to be used for training the model.

To ensure a fair evaluation, the dataset is split into training, validation, and test datasets using stratified sampling. This is particularly important because the dataset is imbalanced, with more positive reviews than negative or neutral ones.

The processed data is then converted into the Hugging Face Dataset format and tokenized using the DistilBERT tokenizer. The tokenizer transforms raw text into numerical representations that the model can understand. Truncation is also applied to limit the sequence length.

A pretrained DistilBERT model is fine-tuned for sequence classification with three output classes. The model is trained using the Hugging Face Trainer API, which simplifies the training loop and evaluation. During training, multiple evaluation metrics are computed, including accuracy, macro F1-score, precision, and recall. The macro-averaged metrics are particularly important for handling class imbalance, as they treat all classes equally.

After training, the model is evaluated on the test dataset. Predictions are generated and compared against the true labels to produce a detailed classification report. The predictions are also saved to a CSV file for further analysis.

Finally, the trained model and tokenizer are saved to disk, allowing them to be reused later for inference or further fine-tuning.
