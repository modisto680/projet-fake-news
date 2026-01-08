from __future__ import annotations

import re
import time
from typing import Any, Dict, List

from pymongo import MongoClient


# =====================
# Regex utilitaires
# =====================
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


# =====================
# NODE 1 – LOAD RAW
# =====================
def load_raw_posts_from_mongo(
    mongo_uri: str,
    mongo_params: dict,
) -> List[Dict[str, Any]]:
    """
    Charge les posts bruts depuis MongoDB.
    """
    db_name = mongo_params["db"]
    raw_col = mongo_params["raw_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][raw_col]

    projection = {
        "_id": 1,
        "uri": 1,
        "cid": 1,
        "_collected_at": 1,
        "_query": 1,
        "_source": 1,
        "indexedAt": 1,
        "author": 1,
        "record": 1,
        "labels": 1,
    }

    docs = list(col.find({}, projection=projection))
    return docs


# =====================
# NODE 2 – PREPROCESS
# =====================
def preprocess_posts(
    raw_posts: List[Dict[str, Any]],
    preprocess_params: dict,
) -> List[Dict[str, Any]]:
    """
    Nettoie le texte et prépare les documents pour le NLP.
    """
    min_len = preprocess_params["min_text_len"]
    processed: List[Dict[str, Any]] = []

    for doc in raw_posts:
        record = doc.get("record") or {}
        text = record.get("text") or ""

        if not isinstance(text, str):
            continue

        text_clean = _URL_RE.sub(" ", text)
        text_clean = _WS_RE.sub(" ", text_clean).strip()

        if len(text_clean) < min_len:
            continue

        processed.append(
            {
                "uri": doc.get("uri"),
                "cid": doc.get("cid"),
                "indexedAt": doc.get("indexedAt"),
                "_collected_at": doc.get("_collected_at"),
                "_source": doc.get("_source"),
                "_query": doc.get("_query"),
                "author": doc.get("author"),
                "labels": doc.get("labels"),
                "text_raw": text,
                "text_clean": text_clean,
                "text_len": len(text_clean),
                "processed_at": int(time.time()),
            }
        )

    return processed


# =====================
# NODE 3 – SAVE PROCESSED
# =====================
def save_processed_posts_to_mongo(
    processed_posts: List[Dict[str, Any]],
    mongo_uri: str,
    mongo_params: dict,
) -> int:
    """
    Sauvegarde les posts preprocessés dans MongoDB (upsert).
    """
    if not processed_posts:
        return 0

    db_name = mongo_params["db"]
    out_col = mongo_params["processed_collection"]

    client = MongoClient(mongo_uri)
    col = client[db_name][out_col]

    # Index pour éviter les doublons
    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
    except Exception:
        pass

    touched = 0

    for doc in processed_posts:
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
