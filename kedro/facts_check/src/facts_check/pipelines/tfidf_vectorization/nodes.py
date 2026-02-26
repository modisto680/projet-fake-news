from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from pymongo import MongoClient

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
import joblib


def load_posts_processed_from_mongo(
    mongo_uri: str,
    mongo_params: dict,
    tfidf_params: dict,
) -> pd.DataFrame:
    """
    Charge les posts depuis MongoDB (collection posts_processed) dans un DataFrame.
    """
    db_name = mongo_params["db"]
    in_col = tfidf_params["input_collection"]
    text_field = tfidf_params["text_field"]
    max_posts = tfidf_params.get("max_posts")

    client = MongoClient(mongo_uri)
    col = client[db_name][in_col]
    
    import logging, os
    log = logging.getLogger(__name__)
    log.warning("[DEBUG MONGO] mongo_uri=%s", mongo_uri)
    log.warning("[DEBUG MONGO] db=%s collection=%s", db_name, in_col)
    log.warning("[DEBUG MONGO] text_field=%s", text_field)
    log.warning("[DEBUG MONGO] estimated_count=%s", col.estimated_document_count())
    
    projection = {
        "_id": 1,
        "uri": 1,
        "cid": 1,
        "indexedAt": 1,
        "_collected_at": 1,
        "_source": 1,
        "_query": 1,
        "author": 1,
        "labels": 1,
        text_field: 1,
        "text_len": 1,
    }

    cursor = col.find({}, projection=projection)
    if isinstance(max_posts, int) and max_posts > 0:
        cursor = cursor.limit(max_posts)

    docs = list(cursor)
    df = pd.DataFrame(docs)

    # sécurise le champ texte
    if text_field not in df.columns:
        df[text_field] = ""
    df[text_field] = df[text_field].fillna("").astype(str)

    # filtre les textes vides
    df = df[df[text_field].str.strip().astype(bool)].reset_index(drop=True)

    return df


def fit_transform_tfidf(
    df: pd.DataFrame,
    tfidf_params: dict,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Fit TF-IDF sur les textes + (optionnel) réduction SVD.
    Retourne:
    - df enrichi avec features (svd_* si activé)
    - artefacts (vectorizer, svd, vocab_size, etc.)
    """
    text_field = tfidf_params["text_field"]

    vectorizer = TfidfVectorizer(
        min_df=tfidf_params.get("min_df", 2),
        max_df=tfidf_params.get("max_df", 0.95),
        ngram_range=tuple(tfidf_params.get("ngram_range", [1, 1])),
        max_features=tfidf_params.get("max_features", None),
        lowercase=True,
    )
    import logging
    log = logging.getLogger(__name__)

    log.warning("[DEBUG TFIDF] rows=%s", len(df))
    log.warning("[DEBUG TFIDF] text_field=%s in_columns=%s", text_field, text_field in df.columns)
    log.warning(
        "[DEBUG TFIDF] non_empty=%s",
        (df[text_field].fillna("").astype(str).str.strip() != "").sum()
    )
    log.warning(
        "[DEBUG TFIDF] sample=%s",
        df[text_field].fillna("").astype(str).head(5).tolist()
    )

    X = vectorizer.fit_transform(df[text_field].tolist())  # sparse matrix
    vocab_size = len(vectorizer.vocabulary_)

    use_svd = bool(tfidf_params.get("use_svd", True))
    svd = None
    X_reduced = None

    if use_svd:
        n_components = int(tfidf_params.get("svd_components", 100))
        random_state = int(tfidf_params.get("random_state", 42))
        svd = TruncatedSVD(n_components=n_components, random_state=random_state)
        X_reduced = svd.fit_transform(X)  # dense (n_posts, n_components)

        # ajoute les colonnes svd_0..svd_{n-1}
        for i in range(n_components):
            df[f"svd_{i}"] = X_reduced[:, i].astype(float)

    artefacts = {
        "vectorizer": vectorizer,
        "svd": svd,
        "vocab_size": vocab_size,
        "use_svd": use_svd,
        "svd_components": int(tfidf_params.get("svd_components", 0)) if use_svd else 0,
    }

    return df, artefacts


def save_tfidf_artefacts(
    artefacts: Dict[str, Any],
    tfidf_params: dict,
) -> Dict[str, str]:
    """
    Sauvegarde le vectorizer (et svd si utilisé) sur disque via joblib.
    Retourne les chemins sauvegardés.
    """
    # Kedro a généralement un dossier data/, mais on reste simple:
    # on stocke dans un dossier "data/08_reporting" (tu peux changer)
    out_dir = "../../data/processed"  # volume partagé avec la racine du projet
    vectorizer_path = f"{out_dir}/tfidf_vectorizer.joblib"
    joblib.dump(artefacts["vectorizer"], vectorizer_path)

    saved = {"vectorizer_path": vectorizer_path}

    if artefacts.get("svd") is not None:
        svd_path = f"{out_dir}/tfidf_svd.joblib"
        joblib.dump(artefacts["svd"], svd_path)
        saved["svd_path"] = svd_path

    return saved


def save_posts_tfidf_to_mongo(
    df: pd.DataFrame,
    mongo_uri: str,
    mongo_params: dict,
    tfidf_params: dict,
) -> int:
    """
    Sauvegarde dans MongoDB une version enrichie des posts.
    On stocke surtout svd_* (petit vecteur dense) pour alimenter un modèle IA.
    """
    db_name = mongo_params["db"]
    out_col = tfidf_params["output_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][out_col]

    # index pour dédup
    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
    except Exception:
        pass

    now = int(time.time())
    touched = 0

    records = df.to_dict(orient="records")

    for doc in records:
        # nettoyage : ObjectId n'est pas JSON-serializable facilement -> on l'enlève
        doc.pop("_id", None)

        doc["tfidf_at"] = now

        key_uri = doc.get("uri")
        key_cid = doc.get("cid")

        if key_uri:
            filt = {"uri": key_uri}
        elif key_cid:
            filt = {"cid": key_cid}
        else:
            continue

        res = col.update_one(filt, {"$set": doc}, upsert=True)
        touched += int(res.matched_count) + int(res.modified_count) + int(res.upserted_id is not None)

    return touched
    