import pandas as pd
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from numpy.random import test
from sklearn.model_selection import train_test_split
from datasets import Dataset, DatasetDict
from transformers import AutoTokenizer
import evaluate
from transformers import AutoModelForSequenceClassification
from transformers import TrainingArguments, Trainer
from transformers import DataCollatorWithPadding
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import torch
print(torch.cuda.is_available())

base_path = Path("/.../Amazon2023/Jewelry_review_files/")
year_folders=sorted(base_path.glob("year=*"))

dfs=[]
for year_folder in year_folders:
    parquet_files=sorted(year_folder.glob("*.parquet"))
    for parquet_file in parquet_files:
        df=pd.read_parquet(parquet_file, columns=['parent_asin', 'rating', 'review_title', 'review'])
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df = df[df['rating'].between(1, 5)]
        df = df.dropna(subset=['rating'])
        df['review_title'] = df['review_title'].fillna("").astype(str)
        df['review']=df['review'].fillna("").astype(str)
        df['total_review'] = (df['review_title'].str.strip() + " " + df['review'].str.strip()).str.strip()
        df = df[df['total_review'].str.len()>0]
        dfs.append(df[['rating', 'total_review']])

reviews=pd.concat(dfs, ignore_index=True)
print(reviews.shape)
reviews.head()

reviews["sentiment"] = np.select([reviews["rating"] <=2.5 ,(reviews["rating"] >= 2.5) & (reviews["rating"] <= 3.5),reviews["rating"] > 3.5],["negative", "neutral", "positive"],default="unknown")
reviews["label"]=np.select([reviews["rating"] <=2.5 ,(reviews["rating"] >= 2.5) & (reviews["rating"] <= 3.5),reviews["rating"] > 3.5],["0", "1", "2"],default="unknown")
print(reviews.shape)
#reviews.head()
#reviews["sentiment"].value_counts().sort_index().plot(kind="bar")
#plt.title("Class distribution")
#plt.ylabel("Count")
#plt.show()

#percentage of how many rating exist in each segment
reviews["sentiment"].value_counts(normalize=True)
#dataset is imbalance so I use stratify
train_df, testval_df = train_test_split(reviews, test_size=0.2, stratify=reviews['label'], random_state=42)
test_df, val_df = train_test_split(testval_df, test_size=0.5, stratify=testval_df['label'], random_state=42)
print("train shape:", train_df.shape)
print("val shape:", val_df.shape)
print("test shape:", test_df.shape)
train_df["label"] = train_df["label"].astype(int)
val_df["label"] = val_df["label"].astype(int)
test_df["label"] = test_df["label"].astype(int)

#convert to HF dataset
dataset=DatasetDict({"train": Dataset.from_pandas(train_df[["total_review", "label"]], preserve_index=False),
                     "validation": Dataset.from_pandas(val_df[["total_review", "label"]], preserve_index=False),
                     "test": Dataset.from_pandas(test_df[["total_review", "label"]], preserve_index=False)})

#prepare data and tokenize the text
model_checkpoint = 'distilbert-base-uncased'
tokenizer = AutoTokenizer.from_pretrained(model_checkpoint)
def tokenize_func(batch):
    return tokenizer(batch['total_review'], truncation=True, max_length=256)

tokenized_datasets = dataset.map(tokenize_func, batched=True, batch_size=1000,remove_columns=["total_review"],)
# tokenized_datasets = tokenized_datasets.remove_columns(["total_review"])
tokenized_datasets = tokenized_datasets.rename_column("label", "labels")
# tokenized_datasets.set_format("torch")
#tokenized_datasets

#evaluation
accuracy_metric=evaluate.load("accuracy")
#dataset is imbalance, it has more data with rating > 4 so I am doing other types of evaluations
f1_metric=evaluate.load("f1")
precision_metric=evaluate.load("precision")
recall_metric=evaluate.load("recall")

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=1)
    
    accuracy=accuracy_metric.compute(predictions=predictions, references=labels)
    f1=f1_metric.compute(predictions=predictions, references=labels, average="macro")
    precision=precision_metric.compute(predictions=predictions, references=labels, average="macro")
    recall=recall_metric.compute(predictions=predictions, references=labels, average="macro")
    return {"accuracy": accuracy["accuracy"], "f1_macro": f1["f1"], "precision_macro": precision["precision"], "recall_macro": recall["recall"]}

model = AutoModelForSequenceClassification.from_pretrained(model_checkpoint, num_labels=3, id2label={0: "negative", 1: "neutral", 2: "positive"},label2id={"negative": 0, "neutral": 1, "positive": 2})
model.to("cuda")

data_collator = DataCollatorWithPadding(tokenizer=tokenizer)
training_args = TrainingArguments(
    output_dir="/beegfs/dehghani/NLP/Amazon2023/",
    learning_rate=2e-5,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    num_train_epochs=3,
    weight_decay=0.01,
    evaluation_strategy="epoch",
    save_strategy="epoch",
    logging_strategy="epoch",
    load_best_model_at_end=True,
    metric_for_best_model="f1_macro",
    greater_is_better=True,
    save_total_limit=2,
    report_to="none",
    push_to_hub=False
)

trainer = Trainer(model=model, args=training_args, train_dataset=tokenized_datasets["train"], eval_dataset=tokenized_datasets["validation"], data_collator=data_collator, compute_metrics=compute_metrics)
trainer.train()

#evaluation
test_results=trainer.evaluate(tokenized_datasets["test"])
print(test_results)

pred_output = trainer.predict(tokenized_datasets["test"])
y_pred = np.argmax(pred_output.predictions, axis=1)
y_true = np.array(test_df["label"])

print(classification_report(y_true, y_pred,target_names=["negative", "neutral", "positive"]))

#confusion matrix
#cm = confusion_matrix(y_true, y_pred)
#disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["negative", "neutral", "positive"])
#disp.plot(cmap="Blues")
#plt.title("Confusion Matrix")
#plt.show()

results_df = pd.DataFrame({"y_true": y_true,"y_pred": y_pred})

results_df.to_csv("/beegfs/dehghani/NLP/Amazon2023/test_predictions.csv", index=False)

save_path = "/beegfs/dehghani/NLP/Amazon2023/"
trainer.save_model(save_path)
tokenizer.save_pretrained(save_path)




