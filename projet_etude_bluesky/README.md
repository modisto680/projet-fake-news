# Thumalien — Détection de Fake News sur Bluesky

Projet Mastère 1 Data & IA — Client fictif : **Thumalien**.

Objectif : détecter automatiquement les fake news publiées sur Bluesky via un pipeline NLP complet, avec score de crédibilité, analyse émotionnelle, et dashboard de visualisation.

---

## 🏗️ Architecture générale

```
Bluesky API → Collecte → MongoDB (RAW)
                ↓
         Kedro Pipeline
         ┌─────────────────────────────────────┐
         │  1. preprocessing                   │  → cleaned_posts (MongoDB)
         │  2. tfidf_vectorization             │  → vectorized_posts (MongoDB)
         │                                     │    + 07_model_output/*.joblib
         │  3. fake_news_model                 │  → 06_models/fake_news_model.pkl
         │                                     │    + 08_reporting/metrics.json
         │  4. emotion_analysis                │  → emotion_clusters_vader (MongoDB)
         │  5. credibility_scoring             │  → scored_posts (MongoDB)
         └─────────────────────────────────────┘
                ↓
         Dashboard Streamlit (port 8501)
```

L'ensemble est orchestré par **Apache Airflow** et conteneurisé avec **Docker**.

---

## ✅ État d'avancement

| Composant | Description | Statut |
|-----------|-------------|--------|
| `scripts/login.py` | Authentification Bluesky → `token.json` | ✅ Fait |
| `scripts/collect_posts.py` | Collecte des posts via API Bluesky | ✅ Fait |
| `kedro/pipeline: preprocessing` | Nettoyage NLP (suppression URLs, normalisation) | ✅ Fait |
| `kedro/pipeline: tfidf_vectorization` | TF-IDF + réduction SVD (100 composantes) | ✅ Fait |
| `kedro/pipeline: fake_news_model` | Entraînement Logistic Regression (Anti-Biais : Titres) | ✅ Fait |
| `kedro/pipeline: emotion_analysis` | Scores VADER + clustering KMeans (8 clusters) | ✅ Fait |
| `kedro/pipeline: credibility_scoring` | Scoring de crédibilité (Seuil 0.7 + Bulk Write) | ✅ Fait |
| DAG Airflow | Orchestration toutes les 6h (+ tâche `score_credibility`) | ✅ Fait |
| Dashboard Streamlit | Visualisation des scores, KPIs, graphiques (port 8501) | ✅ Fait |
| Explicabilité IA (SHAP) | Analyse mot-à-mot de la décision via `explain_post.py` | ✅ Fait |
| Suivi énergétique (CodeCarbon) | Green IT / empreinte CO₂ | ❌ À faire |

---

## 📦 Pré-requis

- **Docker Desktop** installé et lancé
- Un compte **Bluesky**
- Une base **MongoDB** (Atlas ou locale)
- Python 3.11+ (pour exécution locale hors Docker)

---

## ⚙️ Configuration

1. Copie `.env.example` en `.env`
2. Renseigne les variables :

```env
BLUESKY_IDENTIFIER=ton_identifiant
BLUESKY_PASSWORD=ton_mot_de_passe
MONGO_URI=mongodb+srv://...
MONGO_DB=thumalien
```

> ⚠️ Ne partage jamais ton `.env`.

---

## 🐋 Lancer avec Docker + Airflow

```bash
# Première fois uniquement : initialise la base Airflow
docker compose up airflow-init

# Lance tous les services en arrière-plan
docker compose up -d
```

Airflow : [http://localhost:8080](http://localhost:8080)
- user : `admin` / password : `admin`

Le DAG `thumalien_collect_and_vectorize` s'exécute **toutes les 6 heures** automatiquement.

---

## 🔁 Pipeline Airflow (DAG : `thumalien_collect_and_vectorize`)

```
login_bluesky → collect_raw_posts → kedro_preprocess_and_vectorize → score_credibility
```

| Tâche Airflow | Script | Action |
|---|---|---|
| `login_bluesky` | `scripts/login.py` | Récupère un token JWT → `token.json` |
| `collect_raw_posts` | `scripts/collect_posts.py` | Collecte timeline, discover, trending, hot_topics, popular |
| `kedro_preprocess_and_vectorize` | `kedro run` | Preprocessing + TF-IDF |
| `score_credibility` | `kedro run --pipeline credibility_scoring` | Scoring de crédibilité → `scored_posts` (MongoDB) |

> **Note :** `fake_news_model` et `emotion_analysis` se lancent manuellement (voir ci-dessous).
> **Note :** Pour relancer uniquement le scoring sans collecte, utiliser l'UI Airflow → marquer les tâches précédentes comme "Success" manuellement.

---

## 🧪 Exécution manuelle (sans Airflow)

```bash
# Collecte
python scripts/login.py
python scripts/collect_posts.py

# Pipelines Kedro
cd kedro/facts_check

kedro run                                      # preprocessing + tfidf (default)
kedro run --pipeline fake_news_model           # entraîner le modèle de classification
kedro run --pipeline emotion_analysis          # calcul des scores VADER + KMeans
kedro run --pipeline credibility_scoring       # scorer les posts nettoyés → scored_posts
```

---

## 🤖 Pipelines Kedro détaillés

### 1. `preprocessing`
- **Entrée :** collections RAW MongoDB (`timeline`, `feed_discover`, `feed_trending`, `feed_hot_topics`, `feed_popular`)
- **Traitement :** suppression des URLs, normalisation des espaces, filtre longueur minimale
- **Sortie :** collection MongoDB `cleaned_posts` (champ `text_clean`)

### 2. `tfidf_vectorization`
- **Entrée :** `cleaned_posts`
- **Traitement :** TF-IDF (max 50 000 features, ngram 1-2) + réduction SVD (100 composantes)
- **Sorties :**
  - MongoDB `vectorized_posts` (vecteurs SVD `svd_0`…`svd_99`)
  - `data/07_model_output/tfidf_vectorizer.joblib`
  - `data/07_model_output/tfidf_svd.joblib`

### 3. `fake_news_model`
- **Entrée :** `data/01_raw/Fake.csv` + `data/01_raw/True.csv` (datasets publics étiquetés)
- **Traitement :** TF-IDF + Régression Logistique, split 80/20
- **Sorties :**
  - `data/06_models/fake_news_model.pkl`
  - `data/08_reporting/fake_news_metrics.json` (accuracy, F1, precision, recall)

### 4. `emotion_analysis`
- **Entrée :** `cleaned_posts` (MongoDB)
- **Traitement :** scores VADER (`neg`, `neu`, `pos`, `compound`) + KMeans (8 clusters)
- **Sortie :** collection MongoDB `emotion_clusters_vader`

### 5. `credibility_scoring`
- **Entrée :** `cleaned_posts` (MongoDB) + `data/06_models/fake_news_model.pkl`
- **Traitement :** inférence du modèle TF-IDF + Logistic Regression sur les posts réels Bluesky
- **Sortie :** collection MongoDB `scored_posts` avec les champs :
  - `credibility_score` : score [0-1], proche de 1 = très crédible
  - `is_fake` : `True` si détecté comme fake news
  - `fake_proba` : probabilité brute [0-1]
  - `scored_at` : timestamp Unix de l'analyse
- **Note perf :** utilise `bulk_write` par batches de 1000 (x20 plus rapide qu'un upsert séquentiel)

---

## 📂 Structure du projet

```
projet_etude_bluesky/
├── .env                              # Variables d'environnement (non versionné)
├── .gitignore
├── Dockerfile                        # Image Airflow + dépendances
├── docker-compose.yml                # Services : postgres, airflow, streamlit
├── requirements.txt
├── requirements-airflow.txt
├── README.md
│
├── docs/
│   └── cours/                        # Ressources pédagogiques (PDFs)
│
├── scripts/
│   ├── login.py                      # Authentification Bluesky
│   ├── collect_posts.py              # Collecte des posts
│   └── token.json                    # Token JWT (généré automatiquement)
│
├── dags/
│   └── thumalien_pipeline_dag.py     # DAG Airflow (toutes les 6h)
│
├── streamlit_app/
│   ├── app.py                        # Dashboard de visualisation (port 8501)
│   └── requirements.txt              # Dépendances Streamlit
│
├── plugins/                          # Extensions Airflow (vide)
├── logs/                             # Logs Airflow (généré automatiquement)
│
└── kedro/
    └── facts_check/
        ├── conf/
        │   ├── base/
        │   │   ├── catalog.yml                      # Datasets Kedro
        │   │   ├── parameters.yml                   # Paramètres MongoDB + TF-IDF + credibility
        │   │   └── parameters_emotion_analysis.yml
        │   └── local/                               # Surcharges locales (vide)
        ├── data/
        │   ├── 01_raw/
        │   │   ├── Fake.csv
        │   │   └── True.csv
        │   ├── 06_models/
        │   │   └── fake_news_model.pkl
        │   ├── 07_model_output/
        │   │   ├── tfidf_vectorizer.joblib
        │   │   └── tfidf_svd.joblib
        │   └── 08_reporting/
        │       └── fake_news_metrics.json
        ├── pyproject.toml
        ├── requirements.txt
        └── src/facts_check/
            ├── pipeline_registry.py
            └── pipelines/
                ├── preprocessing/
                ├── tfidf_vectorization/
                ├── fake_news_model/
                ├── emotion_analysis/
                └── credibility_scoring/  # ← Nouveau
```

---

## 📊 Résultats et sorties

### MongoDB
| Collection | Contenu |
|---|---|
| `timeline`, `feed_*` | Posts bruts Bluesky (RAW) |
| `cleaned_posts` | Posts nettoyés + `text_clean` |
| `vectorized_posts` | Posts + vecteurs SVD (`svd_0`…`svd_99`) |
| `emotion_clusters_vader` | Scores VADER + cluster KMeans |
| `scored_posts` | Posts + `credibility_score`, `is_fake`, `fake_proba`, `scored_at` |

### Disque (`kedro/facts_check/data/`)
| Fichier | Description |
|---|---|
| `06_models/fake_news_model.pkl` | Modèle de classification sérialisé |
| `07_model_output/tfidf_vectorizer.joblib` | Vectorizer TF-IDF |
| `07_model_output/tfidf_svd.joblib` | Modèle SVD |
| `08_reporting/fake_news_metrics.json` | Accuracy, F1, precision, recall |

---

## 🌐 Dashboard Streamlit

Accessible sur **[http://localhost:8501](http://localhost:8501)** dès que les containers Docker sont lancés.

Fonctionnalités :
- KPIs globaux (nb posts scorés, % fake, score moyen)
- Histogramme de distribution des scores de crédibilité
- Camembert Fake vs Crédible
- Évolution temporelle du score moyen
- Tableau filtrable des posts avec barre de progression

---

## 🚀 Prochaines étapes

1. **Explicabilité IA** — Afficher les mots les plus influents dans la décision du modèle (SHAP)
2. **Suivi énergétique** — Intégrer `CodeCarbon` pour mesurer l'empreinte CO₂ du pipeline

---

## 🛠️ Dépannage

| Problème | Solution |
|---|---|
| Docker ne démarre pas | Vérifier que Docker Desktop est lancé (icône baleine dans la barre des tâches) |
| `token.json` manquant | `python scripts/login.py` ou lancer la tâche `login_bluesky` dans Airflow |
| Mongo inaccessible | Vérifier `MONGO_URI` dans `.env` et redémarrer Docker |
| Pipeline Kedro vide | Vérifier que `cleaned_posts` contient bien des données |
