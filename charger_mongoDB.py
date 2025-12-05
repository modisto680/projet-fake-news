import os
import json
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient

# === Détection du dossier de travail ===
if Path("/opt/airflow").exists():
    BASE_DIR = Path("/opt/airflow/scripts")
else:
    BASE_DIR = Path(__file__).resolve().parent

# === Chargement du .env ===
env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    print(f"⚠️  Fichier .env introuvable à {env_path}, les variables doivent être déjà définies.")

# === Lecture des variables d'environnement ===
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DBNAME = os.getenv("MONGO_DBNAME")

if not MONGO_URI or not MONGO_DBNAME:
    raise ValueError("❌ Variables MONGO_URI ou MONGO_DBNAME manquantes dans le fichier .env")

# === Connexion à MongoDB ===
client = MongoClient(MONGO_URI)
db = client[MONGO_DBNAME]
collection = db["bluesky_posts"]  # ✅ nom plus générique

# === Lecture du fichier JSON ===
data_path = BASE_DIR / "bluesky_posts_all.json"
if not data_path.exists():
    raise FileNotFoundError(f"❌ Le fichier {data_path} est introuvable.")

with open(data_path, "r", encoding="utf-8") as f:
    posts = json.load(f)

# === Nettoyage optionnel : éviter doublons via URI ===
if posts:
    # On peut éviter les doublons en vérifiant l'URI
    existing_uris = set(doc["uri"] for doc in collection.find({}, {"uri": 1}) if "uri" in doc)
    new_posts = [p for p in posts if p.get("uri") not in existing_uris]

    if new_posts:
        collection.insert_many(new_posts)
        print(f"✅ {len(new_posts)} nouveaux documents insérés dans la collection '{collection.name}' de la base '{MONGO_DBNAME}'.")
    else:
        print("⚠️ Aucun nouveau post à insérer (tous déjà présents).")
else:
    print("⚠️ Aucun post trouvé dans le fichier JSON.")
