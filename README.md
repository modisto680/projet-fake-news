# Thumalien — Bluesky Fake News (jusqu'à la vectorisation)

Ce dépôt correspond au projet **jusqu'à l'étape "NLP + vectorisation"** :
1. **Collecte** des posts Bluesky (endpoints officiels)
2. **Stockage RAW** dans MongoDB (collections dédiées)
3. **Prétraitement NLP** (nettoyage)
4. **Vectorisation TF‑IDF** (avec option SVD)
5. **Automatisation via Airflow** + **Docker**

## 1) Pré-requis (débutant)
- Docker Desktop installé et lancé
- Un compte Bluesky
- Une base MongoDB (Atlas ou local)

## 2) Configuration (important)
1. Copie le fichier `.env.example` en `.env`
2. Ouvre `.env` et renseigne :
   - `BLUESKY_IDENTIFIER`
   - `BLUESKY_PASSWORD`
   - `MONGO_URI`
   - `MONGO_DB`

⚠️ Ne partage jamais ton `.env` (mot de passe).

## 3) Lancer le projet (Docker + Airflow)
Dans un terminal, à la racine du projet :

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow est accessible sur http://localhost:8080
- user: `airflow`
- password: `airflow` (modifiable dans `.env`)

## 4) Pipeline (ce que fait le DAG)
DAG : `thumalien_collect_and_vectorize`

1) `scripts/login.py`  
→ crée `scripts/token.json`

2) `scripts/collect_posts.py`  
→ collecte via :
- `app.bsky.feed.getTimeline`
- `app.bsky.feed.searchPosts` (Discover/Trending/Hot Topics)
- `app.bsky.feed.getPopular` (best-effort)

→ écrit dans MongoDB (RAW) :
- `timeline`
- `feed_discover`
- `feed_trending`
- `feed_hot_topics`
- `feed_popular`

3) `kedro run`  
→ pipeline Kedro :
- nettoyage NLP → `cleaned_posts`
- TF‑IDF (+ SVD optionnel) → `vectorized_posts` + artefacts dans `data/processed/`

## 5) Exécuter manuellement (sans Airflow)
```bash
python scripts/login.py
python scripts/collect_posts.py

cd kedro/facts_check
kedro run
```

## 6) Où sont les résultats ?
Dans MongoDB :
- RAW : collections listées plus haut
- NLP : `cleaned_posts`
- TF‑IDF : `vectorized_posts`

Sur disque (dans le conteneur et sur ta machine via le volume `./data`) :
- `data/processed/tfidf_vectorizer.joblib`
- `data/processed/svd.joblib` (si activé)
- `data/processed/tfidf_matrix.joblib` (optionnel)

## Dépannage rapide
- Si Mongo n'est pas utilisé : vérifie `MONGO_URI` dans `.env` et redémarre docker.
- Si `token.json` manque : lance la tâche `login_bluesky` (ou `python scripts/login.py`).
