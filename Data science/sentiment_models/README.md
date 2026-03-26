In this part of the project transformer-based sentiment classifications have been developed using DistilBERT and RoBERTa.

The dataset is first preprocessed by selecting relevant columns, removing missing values, and generating sentiment labels from rating column (negative, neutral, positive). The data is then split into training, validation, and test sets (The dataset is class-imbalanced, with a larger number of reviews having ratings above 4. Hence, positive class dominant. To address this during data splitting, stratified sampling was used to preserve the original class distribution across training, validation, and test datasets). 

Text inputs are tokenized using the corresponding pretrained tokenizers for each model.

Two separate models (DistilBERT and RoBERTa) are fine-tuned for a 3-class classification task using the Hugging Face Transformers framework. During training, macro-averaged F1-score is used as the main evaluation metric to properly account for class imbalance.

After training, both models are evaluated on a test dataset. Predictions are generated and compared with ground truth labels to compute performance metrics such as accuracy, precision, recall, and F1-score. The results are saved as CSV files, enabling further analysis such as confusion matrices, per-class performance, and error inspection.

Although both DistilBERT and RoBERTa were trained and evaluated, the primary goal was not a general model comparison. Instead, the objective was to investigate whether RoBERTa, as a larger and more expressive transformer model, could improve performance on the neutral sentiment class, which is typically more difficult to classify.

In this dataset, the neutral class is both underrepresented and less clearly defined (since labels are derived from rating scores rather than manual sentiment annotations). As a result, the model tends to perform worse on neutral samples compared to positive and negative ones.

Despite its increased capacity, RoBERTa did not yield a substantial improvement in neutral-class performance. This suggests that the limitation is not primarily due to model capacity, but is more likely related to class imbalance (fewer neutral samples), label noise (rating-based labeling does not always reflect true sentiment), and inherent ambiguity in neutral sentiment.


The main bottleneck in this task is the data, not the model.


Potential improvements include:

- Improving label quality (e.g., manual annotation).
    
- And applying techniques such as class weighted loss or data augmentation.

