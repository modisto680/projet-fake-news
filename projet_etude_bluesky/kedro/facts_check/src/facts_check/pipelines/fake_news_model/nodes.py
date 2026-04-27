from typing import Any, Dict, Tuple

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import pandas as pd
from facts_check.utils import track_emissions


import re

def _clean_text(text: str) -> str:
    """Nettoyage identique à celui appliqué aux posts Bluesky."""
    if not isinstance(text, str):
        return ""
    # Suppression URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    # Suppression @mentions (fréquentes sur Bluesky)
    text = re.sub(r"@\S+", "", text)
    # Suppression caractères spéciaux inutiles et normalisation espaces
    text = re.sub(r"\s+", " ", text).strip()
    return text

def prepare_training_data(fake_news_raw: pd.DataFrame,
                          true_news_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Prépare les données en utilisant à la fois les TITRES et le TEXTE complet
    pour rendre le modèle robuste aux formats courts (comme les posts Bluesky).
    """
    # Ajouter label
    fake_news_raw["label"] = 1
    true_news_raw["label"] = 0

    # Création d'un dataset hybride : Titres + Textes
    # On concatène les titres et les textes pour avoir des exemples courts et longs
    fakes_titles = fake_news_raw[["title", "label"]].rename(columns={"title": "text"})
    fakes_text = fake_news_raw[["text", "label"]]
    
    trues_titles = true_news_raw[["title", "label"]].rename(columns={"title": "text"})
    trues_text = true_news_raw[["text", "label"]]

    data = pd.concat([fakes_titles, fakes_text, trues_titles, trues_text], ignore_index=True)
    
    # Nettoyage NLP harmonisé
    data["text"] = data["text"].apply(_clean_text)
    
    # Supprimer les lignes vides après nettoyage
    data = data[data["text"].str.len() > 10]

    return data

@track_emissions(project_name="Thumalien Training")
def train_model(training_data: pd.DataFrame) -> Tuple[SkPipeline, Dict[str, Any]]:
    # Nettoyage minimal (déjà fait dans prepare_training_data mais on sécurise)
    df = training_data.dropna(subset=["text", "label"]).copy()
    
    X = df["text"]
    y = df["label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = SkPipeline(
        steps=[
            ("tfidf", TfidfVectorizer(
                stop_words="english", 
                max_df=0.9, 
                min_df=5, 
                ngram_range=(1, 2),
                max_features=20000
            )),
            ("clf", LogisticRegression(max_iter=3000, class_weight="balanced", C=1.0)),
        ]
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average="binary", zero_division=0
    )

    metrics = {
        "n_samples": int(len(df)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "mean_proba_fake": float(y_proba.mean())
    }

    return model, metrics