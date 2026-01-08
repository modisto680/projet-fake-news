from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import numpy as np
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans


def load_processed_posts_from_mongo(
    mongo_uri: str,
    mongo_params: dict,
    vectorization_params: dict,
) -> List[Dict[str, Any]]:
    """
    Lit les posts preprocessés depuis MongoDB.
    """
    db_name = mongo_params["db"]
    in_col = vectorization_params["input_collection"]
    text_field = vectorization_params["text_field"]
    max_posts = vectorization_params.get("max_posts")

    client = MongoClient(mongo_uri)
    col = client[db_name][in_col]

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

    return list(cursor)


def compute_embeddings(
    posts: List[Dict[str, Any]],
    vectorization_params: dict,
) -> Dict[str, Any]:
    """
    Calcule un embedding pour chaque post (sur text_clean).
    Retourne un dict avec: posts (alignés) + embeddings numpy.
    """
    model_name = vectorization_params["model_name"]
    batch_size = int(vectorization_params.get("batch_size", 64))
    text_field = vectorization_params["text_field"]

    model = SentenceTransformer(model_name)

    texts: List[str] = []
    kept_posts: List[Dict[str, Any]] = []

    for p in posts:
        t = p.get(text_field)
        if isinstance(t, str) and t.strip():
            texts.append(t.strip())
            kept_posts.append(p)

    if not texts:
        return {"posts": [], "embeddings": np.zeros((0, 0), dtype=np.float32)}

    emb = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,  # utile pour KMeans / similarité
    ).astype(np.float32)

    return {"posts": kept_posts, "embeddings": emb}


def cluster_embeddings(
    payload: Dict[str, Any],
    clustering_params: dict,
) -> List[Dict[str, Any]]:
    """
    Applique KMeans sur les embeddings et ajoute cluster_id à chaque post.
    """
    posts: List[Dict[str, Any]] = payload["posts"]
    emb: np.ndarray = payload["embeddings"]

    if len(posts) == 0:
        return []

    n_clusters = int(clustering_params["n_clusters"])
    random_state = int(clustering_params.get("random_state", 42))

    km = KMeans(n_clusters=n_clusters, random_state=random_state, n_init="auto")
    labels = km.fit_predict(emb)

    out: List[Dict[str, Any]] = []
    now = int(time.time())

    store_embedding = bool(clustering_params.get("store_embedding", False))

    for p, cid in zip(posts, labels):
        doc = dict(p)  # copie

        doc["cluster_id"] = int(cid)
        doc["clustered_at"] = now

        # Optionnel : stocker l'embedding (attention: plus lourd)
        if store_embedding:
            doc["embedding"] = emb[len(out)].tolist()

        out.append(doc)

    return out


def save_clustered_posts_to_mongo(
    clustered_posts: List[Dict[str, Any]],
    mongo_uri: str,
    mongo_params: dict,
    vectorization_params: dict,
) -> int:
    """
    Upsert dans la collection output_collection.
    Dédup par uri (ou cid).
    """
    if not clustered_posts:
        return 0

    db_name = mongo_params["db"]
    out_col = vectorization_params["output_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][out_col]

    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
        col.create_index("cluster_id")
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
        touched += int(res.matched_count) + int(res.modified_count) + int(res.upserted_id is not None)

    return touched
