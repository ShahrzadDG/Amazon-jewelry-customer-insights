The python code here is a sentiment analysis pipeline using Hugging Face Transformers and a pretrained DistilBERT model. The goal is to classify Amazon jewelry product reviews into three sentiment categories: negative, neutral, and positive.

First the Amazon review data that is stored in multiple .parquet files and organized by year is loaded. For each file, the relevant columns such as ratings, review titles, and review text are extracted. The data is then cleaned by removing invalid ratings, handling missing values, and combining the title and review into a single text field to create a more informative input for the model.

Next, sentiment labels are generated based on the reviewer's rating. Reviews with low ratings are classified as negative, mid-range ratings as neutral, and high ratings as positive. These labels are encoded into numerical form to be used for training the model.

To ensure a fair evaluation, the dataset is split into training, validation, and test datasets using stratified sampling. This is particularly important because the dataset is imbalanced, with more positive reviews than negative or neutral ones.

The processed data is then converted into the Hugging Face Dataset format and tokenized using the DistilBERT tokenizer. The tokenizer transforms raw text into numerical representations that the model can understand. Truncation is also applied to limit the sequence length.

A pretrained DistilBERT model is fine-tuned for sequence classification with three output classes. The model is trained using the Hugging Face Trainer API, which simplifies the training loop and evaluation. During training, multiple evaluation metrics are computed, including accuracy, macro F1-score, precision, and recall. The macro-averaged metrics are particularly important for handling class imbalance, as they treat all classes equally.

After training, the model is evaluated on the test dataset. Predictions are generated and compared against the true labels to produce a detailed classification report. The predictions are also saved to a CSV file for further analysis.

Finally, the trained model and tokenizer are saved to disk, allowing them to be reused later for inference or further fine-tuning.

#### Evaliation

After fine-tuning the DistilBERT model, I evaluated its performance on the test dataset to measure how well it generalizes to unseen Amazon jewelry reviews. The evaluation notebook (.ipynb file) loads the saved model and tokenizer, applies them to the test data, and analyzes the predictions using several complementary metrics and visualizations. This makes it possible to assess not only the overall accuracy of the classifier, but also how well it performs on each sentiment class individually.

<img width="554" height="502" alt="image" src="https://github.com/user-attachments/assets/e5ef4ddf-7dca-456a-acb3-14e2206c7b76" />


Before interpreting the results, I first examined the class distribution of the test dataset. This step is important because the dataset is imbalanced, with positive reviews representing the majority of the samples, while neutral reviews appear much less frequently. Visualizing the class distribution helps explain why accuracy alone is not sufficient for evaluation and why macro-averaged metrics are especially important in this project.

To verify that the saved model works correctly in inference mode, I tested it on a few example review sentences. The notebook loads the fine-tuned DistilBERT model and predicts the sentiment of simple positive, neutral, and negative review examples. This provides a quick qualitative check that the model has learned the expected sentiment patterns and can produce meaningful predictions outside the training loop.

The notebook also evaluates the model at the probability level by applying the softmax function to the output logits. This produces a probability score for each sentiment class and allows the predicted class to be selected from the highest probability. Inspecting these probabilities gives more insight into how confident the model is in its predictions and is useful for later confidence analysis.

To monitor the fine-tuning process, the notebook reads the training log and extracts the history of training loss and validation metrics across epochs. The results show that the model improved during training and that the validation performance remained stable. In particular, the macro F1-score was used as the main selection metric, which is more appropriate than plain accuracy for an imbalanced three-class sentiment problem.

The final model achieved strong performance on the test dataset, with an accuracy of about 92.93%. However, because the dataset is highly imbalanced, I also report macro precision, macro recall, and macro F1-score. The macro F1-score is approximately 0.795, which gives a more balanced view of performance across the negative, neutral, and positive classes. Weighted F1 is higher because it is influenced more strongly by the dominant positive class.

The classification report provides a detailed breakdown of precision, recall, and F1-score for each sentiment category. The model performs very well on the positive class, which is expected because this class has the largest number of samples in the dataset. It also performs strongly on the negative class. The most difficult class is neutral, which has the lowest precision, recall, and F1-score. This result is reasonable because neutral reviews are often less explicit and can overlap linguistically with both positive and negative opinions.

The confusion matrix helps visualize how predictions are distributed across the three classes. It shows that most positive and negative reviews are classified correctly, while neutral reviews are more frequently confused with the other two categories. This confirms the pattern observed in the classification report: the model distinguishes clear sentiment better than ambiguous or mixed sentiment.

<img width="688" height="559" alt="image" src="https://github.com/user-attachments/assets/6ac76855-9268-4614-8d50-179d26319199" />

In addition to the raw confusion matrix, the notebook also includes a row normalized confusion matrix. This version is particularly useful because it shows the proportion of correct and incorrect predictions within each class rather than the absolute counts. Since the dataset is imbalanced, the normalized matrix provides a fairer view of per class behavior and makes it easier to see that the neutral class remains the most challenging one.

<img width="661" height="561" alt="image" src="https://github.com/user-attachments/assets/cab040ea-7273-4e67-b1bc-6484ed878494" />

To further evaluate the classifier, the notebook performs a multiclass ROC analysis using one-vs-rest ROC curves. These curves illustrate the trade off between true positive rate and false positive rate for each class. The ROC analysis provides an additional perspective on how well the model separates each sentiment category from the others. It is especially useful for understanding the ranking quality of the model’s predicted probabilities, rather than only its final hard class decisions.

<img width="690" height="490" alt="image" src="https://github.com/user-attachments/assets/f4c89acf-085f-4f46-b229-ddf02b29d6af" />

The notebook also compares the confidence scores of correct and incorrect predictions. This analysis helps determine whether the model tends to make wrong predictions with high confidence or whether incorrect predictions are mostly uncertain. A well behaved model should generally show higher confidence for correct predictions and lower confidence for incorrect ones. This plot is useful for judging calibration quality and for identifying whether probability calibration methods might further improve reliability.

<img width="690" height="490" alt="image" src="https://github.com/user-attachments/assets/c97a96dc-5f07-48db-8754-becb23bab9c5" />

In general, the fine-tuned DistilBERT model performs very well on the sentiment classification task, especially for positive and negative reviews. The results also show that class imbalance affects the performance, particularly for the neutral class, which is harder to separate from the others. By combining accuracy, macro-averaged metrics, classification reports, confusion matrices, ROC curves, and confidence analysis, this evaluation provides a comprehensive view of the model’s strengths and limitations.

