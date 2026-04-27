"""
This is a boilerplate pipeline 'emotion_analysis'
generated using Kedro 1.1.1
"""
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from typing import Any, Dict, List

# =====================
# NODE 1 – LOAD CLEANED
# =====================
def load_cleaned_posts_from_mongo(
    mongo_uri: str,
    mongo_params: dict,
) -> List[Dict[str, Any]]:
    """
    Charge les posts nettoyés depuis la collection cleaned_posts.
    """
    db_name = mongo_params["db"]
    cleaned_collection = mongo_params["processed_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][cleaned_collection]

    projection = {
        "_id": 1,
        "uri": 1,
        "cid": 1,
        "text_clean": 1,
    }

    docs = list(col.find({}, projection=projection))

    return docs

# =====================
# NODE 2 – VADER SCORING
# =====================
def compute_vader_scores(
    cleaned_posts: List[Dict[str, Any]],
    emotion_params: dict,
) -> List[Dict[str, Any]]:
    """
    Calcule les scores VADER pour chaque post nettoyé.
    """

    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    analyzer = SentimentIntensityAnalyzer()

    text_field = emotion_params["text_field"]
    vader_fields = emotion_params["vader_fields"]

    enriched_posts: List[Dict[str, Any]] = []

    for doc in cleaned_posts:
        text = doc.get(text_field)

        if not isinstance(text, str) or not text.strip():
            continue

        scores = analyzer.polarity_scores(text)

        emotion_doc = {
            "_id": doc.get("_id"),
            "uri": doc.get("uri"),
            "cid": doc.get("cid"),
        }

        for field in vader_fields:
            emotion_doc[field] = scores.get(field, 0.0)

        enriched_posts.append(emotion_doc)

    return enriched_posts

# =====================
# NODE 3 – CLUSTERING
# =====================
def cluster_emotions(
    emotion_posts: List[Dict[str, Any]],
    emotion_params: dict,
) -> List[Dict[str, Any]]:
    """
    Applique KMeans sur les scores émotionnels.
    """

    from sklearn.cluster import KMeans
    import numpy as np

    if not emotion_posts:
        return []

    vader_fields = emotion_params["vader_fields"]
    kmeans_params = emotion_params["kmeans"]

    # Construction matrice
    X = np.array([
        [doc.get(field, 0.0) for field in vader_fields]
        for doc in emotion_posts
    ])

    kmeans = KMeans(
        n_clusters=kmeans_params["n_clusters"],
        random_state=kmeans_params["random_state"],
        n_init=kmeans_params["n_init"],
    )

    clusters = kmeans.fit_predict(X)

    # Ajout cluster aux documents
    clustered_docs: List[Dict[str, Any]] = []

    for doc, cluster_id in zip(emotion_posts, clusters):
        new_doc = doc.copy()
        new_doc["cluster"] = int(cluster_id)
        clustered_docs.append(new_doc)

    return clustered_docs

# =====================
# NODE 4 – SAVE EMOTIONS
# =====================
def save_emotion_clusters_to_mongo(
    clustered_posts: List[Dict[str, Any]],
    mongo_uri: str,
    mongo_params: dict,
    emotion_params: dict,
) -> int:
    """
    Sauvegarde les scores émotionnels et clusters dans MongoDB (upsert).
    """

    from pymongo import MongoClient

    if not clustered_posts:
        return 0

    db_name = mongo_params["db"]
    out_col = emotion_params["output_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][out_col]

    # index pour éviter doublons
    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
    except Exception:
        pass

    touched = 0

    for doc in clustered_posts:
        key_uri = doc.get("uri")
        key_cid = doc.get("cid")

        if key_uri:
            filt = {"uri": key_uri}
        elif key_cid:
            filt = {"cid": key_cid}
        else:
            continue

        res = col.update_one(filt, {"$set": doc}, upsert=True)
        touched += (
            int(res.matched_count)
            + int(res.modified_count)
            + int(res.upserted_id is not None)
        )

    return touched