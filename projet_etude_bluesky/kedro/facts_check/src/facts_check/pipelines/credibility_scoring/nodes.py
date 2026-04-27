"""
Pipeline : credibility_scoring
Applique le modèle fake_news_model sur les posts nettoyés (cleaned_posts)
et écrit les scores de crédibilité dans MongoDB (collection scored_posts).
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import pandas as pd
from facts_check.utils import track_emissions
from pymongo import MongoClient
from sklearn.pipeline import Pipeline as SkPipeline

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Node 1 — Chargement des posts nettoyés depuis MongoDB
# ---------------------------------------------------------------------------

def load_cleaned_posts_for_scoring(
    mongo_uri: str,
    mongo_params: dict,
    credibility_params: dict,
) -> pd.DataFrame:
    """
    Charge la collection cleaned_posts depuis MongoDB dans un DataFrame.
    Seuls les posts ayant un texte non-vide dans text_clean sont retenus.
    """
    db_name = mongo_params["db"]
    in_col = credibility_params["input_collection"]   # "cleaned_posts"
    text_field = credibility_params["text_field"]      # "text_clean"
    max_posts = credibility_params.get("max_posts")

    # Fallback sur la variable d'environnement si le paramètre YAML est vide
    if not mongo_uri:
        mongo_uri = os.environ["MONGO_URI"]
    client = MongoClient(mongo_uri)
    col = client[db_name][in_col]

    log.info("[credibility] Connexion MongoDB : db=%s collection=%s", db_name, in_col)
    log.info("[credibility] Documents estimés : %s", col.estimated_document_count())

    projection = {
        "_id": 0,       # On exclut _id (non sérialisable facilement)
        "uri": 1,
        "cid": 1,
        "author": 1,
        "indexedAt": 1,
        "_collected_at": 1,
        "_source": 1,
        "_query": 1,
        text_field: 1,
    }

    cursor = col.find({}, projection=projection)
    if isinstance(max_posts, int) and max_posts > 0:
        cursor = cursor.limit(max_posts)

    docs = list(cursor)
    df = pd.DataFrame(docs)

    if df.empty:
        log.warning("[credibility] cleaned_posts est vide — aucun post à scorer.")
        return df

    # Sécuriser le champ texte
    if text_field not in df.columns:
        df[text_field] = ""
    df[text_field] = df[text_field].fillna("").astype(str)

    # Filtrer les textes vides
    before = len(df)
    df = df[df[text_field].str.strip().astype(bool)].reset_index(drop=True)
    log.info("[credibility] Posts chargés : %s (filtrés vides : %s)", len(df), before - len(df))

    return df


# ---------------------------------------------------------------------------
# Node 2 — Scoring : inférence du modèle + calcul du score de crédibilité
# ---------------------------------------------------------------------------

@track_emissions(project_name="Thumalien Scoring")
def score_posts(
    df: pd.DataFrame,
    fake_news_model: SkPipeline,
    credibility_params: dict,
) -> pd.DataFrame:
    """
    Applique le modèle de fake news sur le texte de chaque post.

    Champs ajoutés au DataFrame :
    - is_fake (bool)          : True si le modèle prédit une fake news
    - fake_proba (float)      : probabilité brute d'être une fake news [0-1]
    - credibility_score (float): score de crédibilité = 1 - fake_proba  [0-1]
      → proche de 1 = post très crédible
      → proche de 0 = post probablement fake
    """
    if df.empty:
        log.warning("[credibility] DataFrame vide — scoring ignoré.")
        df["is_fake"] = pd.Series(dtype=bool)
        df["fake_proba"] = pd.Series(dtype=float)
        df["credibility_score"] = pd.Series(dtype=float)
        return df

    text_field = credibility_params["text_field"]
    texts = df[text_field].tolist()

    log.info("[credibility] Scoring de %s posts...", len(texts))

    # Nettoyage NLP basique avant scoring (pour correspondre à l'entraînement)
    import re
    def _clean(t):
        t = re.sub(r"http\S+|www\S+|https\S+", "", t, flags=re.MULTILINE)
        t = re.sub(r"@\S+", "", t)
        return re.sub(r"\s+", " ", t).strip()
    
    clean_texts = [_clean(t) for t in texts]

    # Prédictions
    # Au lieu d'utiliser model.predict() qui a un seuil fixe à 0.5,
    # on utilise les probabilités avec un seuil ajustable (ex: 0.7)
    probas = fake_news_model.predict_proba(clean_texts)    # [[p_vrai, p_fake], ...]
    fake_probas = probas[:, 1]
    
    # SEUIL DE DÉCISION ANTI-BIAIS : on ne classe fake que si P > 0.7
    threshold = credibility_params.get("threshold", 0.7)
    is_fake_arr = (fake_probas > threshold)

    credibility_scores = 1.0 - fake_probas

    df = df.copy()
    df["is_fake"] = is_fake_arr
    df["fake_proba"] = fake_probas.round(4)
    df["credibility_score"] = credibility_scores.round(4)

    n_fake = int(df["is_fake"].sum())
    n_total = len(df)
    log.info(
        "[credibility] Résultats (seuil=%.2f) : %s/%s posts détectés comme fake (%.1f%%)",
        threshold, n_fake, n_total, 100 * n_fake / n_total if n_total > 0 else 0,
    )


    return df


# ---------------------------------------------------------------------------
# Node 3 — Sauvegarde dans MongoDB (upsert)
# ---------------------------------------------------------------------------

def save_scored_posts_to_mongo(
    df: pd.DataFrame,
    mongo_uri: str,
    mongo_params: dict,
    credibility_params: dict,
) -> int:
    """
    Sauvegarde (upsert) les posts scorés dans la collection scored_posts.
    Clé de déduplication : uri ou cid.
    Utilise bulk_write par batches pour des performances optimales.
    Retourne le nombre de documents touchés.
    """
    from pymongo import UpdateOne

    if df.empty:
        log.warning("[credibility] Rien à sauvegarder.")
        return 0

    db_name = mongo_params["db"]
    out_col = credibility_params["output_collection"]   # "scored_posts"
    batch_size = 1000

    # Fallback sur la variable d'environnement si le paramètre YAML est vide
    if not mongo_uri:
        mongo_uri = os.environ["MONGO_URI"]
    client = MongoClient(mongo_uri)
    col = client[db_name][out_col]

    # Index de déduplication
    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
    except Exception:
        pass

    now = int(time.time())
    touched = 0
    records = df.to_dict(orient="records")

    # Construire les opérations bulk
    ops = []
    for doc in records:
        doc["scored_at"] = now
        key_uri = doc.get("uri")
        key_cid = doc.get("cid")

        if key_uri:
            filt = {"uri": key_uri}
        elif key_cid:
            filt = {"cid": key_cid}
        else:
            continue

        ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))

    # Envoyer par batches
    for i in range(0, len(ops), batch_size):
        batch = ops[i: i + batch_size]
        result = col.bulk_write(batch, ordered=False)
        touched += (
            result.matched_count
            + result.modified_count
            + result.upserted_count
        )
        log.info(
            "[credibility] Batch %s/%s traité (%s docs).",
            i // batch_size + 1,
            -(-len(ops) // batch_size),  # division plafond
            len(batch),
        )

    log.info("[credibility] %s documents écrits dans '%s'.", touched, out_col)
    return touched
