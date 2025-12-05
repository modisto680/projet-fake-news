import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path

# === Détection automatique de l'environnement (local vs Docker) ===
# Si le dossier /opt/airflow existe, on est dans le conteneur Docker
if Path("/opt/airflow").exists():
    BASE_DIR = Path("/opt/airflow/scripts")
else:
    BASE_DIR = Path(__file__).resolve().parent  # dossier du script local

# === Chargement du .env ===
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    print(f"⚠️  Fichier .env introuvable à {ENV_PATH}, les variables d'environnement doivent être déjà définies.")

# === Chargement du token ===
token_path = BASE_DIR / "token.json"
if not token_path.exists():
    raise FileNotFoundError(f"❌ Le fichier token.json est introuvable à l'emplacement : {token_path}")

with open(token_path, "r", encoding="utf-8") as f:
    token_data = json.load(f)
ACCESS_TOKEN = token_data.get("accessJwt")

if not ACCESS_TOKEN:
    raise ValueError("❌ Aucun accessJwt trouvé dans token.json. Vérifie ton fichier de token.")

# === Paramètres de l’API ===
FEED_URI = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot"
API_URL = "https://bsky.social/xrpc/app.bsky.feed.getFeed"


def get_whats_hot_posts(limit: int = 30):
    """Récupère les posts du flux 'What's Hot'."""
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = {"feed": FEED_URI, "limit": limit}

    try:
        response = requests.get(API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print("🌐 Erreur réseau :", e)
        return []

    feed = data.get("feed", [])
    posts = []

    for item in feed:
        post_data = item.get("post", {})
        record = post_data.get("record", {})
        author = post_data.get("author", {})

        posts.append({
            "uri": post_data.get("uri"),
            "cid": post_data.get("cid"),
            "text": record.get("text"),
            "author": author.get("handle"),
            "author_displayName": author.get("displayName"),
            "createdAt": record.get("createdAt"),
            "likeCount": post_data.get("likeCount", 0),
            "repostCount": post_data.get("repostCount", 0),
            "replyCount": post_data.get("replyCount", 0)
        })

    return posts


if __name__ == "__main__":
    posts = get_whats_hot_posts(limit=30)
    if posts:
        output_path = BASE_DIR / "whats_hot.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        print(f"✅ {len(posts)} posts sauvegardés dans : {output_path}")
    else:
        print("⚠️ Aucun post récupéré.")
