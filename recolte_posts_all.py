import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
import logging


logger = logging.getLogger(__name__)
# === Détection automatique de l'environnement (local vs Docker) ===
if Path("/opt/airflow").exists():
    BASE_DIR = Path("/opt/airflow/scripts")
else:
    BASE_DIR = Path(__file__).resolve().parent

# === Chargement du .env ===
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    logger.info(f"⚠️  Fichier .env introuvable à {ENV_PATH}, les variables doivent être déjà définies.")

# === Chargement du token ===
token_path = BASE_DIR / "token.json"
if not token_path.exists():
    raise FileNotFoundError(f"❌ Le fichier token.json est introuvable à l'emplacement : {token_path}")

with open(token_path, "r", encoding="utf-8") as f:
    token_data = json.load(f)
ACCESS_TOKEN = token_data.get("accessJwt")

if not ACCESS_TOKEN:
    raise ValueError("❌ Aucun accessJwt trouvé dans token.json. Vérifie ton fichier de token.")

# === Configuration des endpoints ===
ENDPOINTS = {
    "whats_hot": {
        "url": "https://bsky.social/xrpc/app.bsky.feed.getFeed",
        "params": {
            "feed": "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot"
        }
    },
    "timeline": {
        "url": "https://bsky.social/xrpc/app.bsky.feed.getTimeline",
        "params": {}
    },
    "discover": {
        "url": "https://public.api.bsky.app/xrpc/app.bsky.unspecced.getPopularFeedGenerators",
        "params": {}
    },
    "popular": {
        "url": "https://public.api.bsky.app/xrpc/app.bsky.feed.getPopular",
        "params": {}
    },
    "trending": {
        "url": "https://public.api.bsky.app/xrpc/app.bsky.feed.getTrendingTopics",
        "params": {}
    }
}


def fetch_posts(endpoint_name: str, limit: int = 30):
    """Récupère les posts pour un endpoint donné."""
    if endpoint_name not in ENDPOINTS:
        logger.info(f"❌ Endpoint inconnu : {endpoint_name}")
        return []

    endpoint = ENDPOINTS[endpoint_name]
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = endpoint["params"].copy()
    params["limit"] = limit

    try:
        response = requests.get(endpoint["url"], headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 501:
            logger.info(f"🚫 Endpoint {endpoint_name} non disponible (501).")
            return []
        logger.info(f"🌐 Erreur HTTP sur {endpoint_name} :", e)
        return []
    except requests.exceptions.RequestException as e:
        logger.info(f"🌐 Erreur réseau sur {endpoint_name} :", e)
        return []

    posts = []

    # === Cas spécifique : Discover (feed generators) ===
    if endpoint_name == "discover":
        feeds = data.get("feeds", [])
        logger.info(f"🔎 {len(feeds)} feeds découverts. Téléchargement des posts…")

        for f in feeds[:5]:  # on limite à 5 pour ne pas surcharger
            uri = f.get("uri")
            if not uri:
                continue

            sub_params = {"feed": uri, "limit": limit}
            try:
                sub_resp = requests.get(
                    "https://bsky.social/xrpc/app.bsky.feed.getFeed",
                    headers=headers,
                    params=sub_params,
                    timeout=10
                )
                sub_resp.raise_for_status()
                sub_data = sub_resp.json()
                sub_feed = sub_data.get("feed", [])
                for item in sub_feed:
                    post_data = item.get("post", {})
                    record = post_data.get("record", {})
                    author = post_data.get("author", {})

                    posts.append({
                        "source": f"discover::{f.get('displayName', 'unknown')}",
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
            except requests.exceptions.RequestException as e:
                logger.info(f"⚠️ Erreur sur le feed Discover {uri} :", e)

        logger.info(f"✅ {len(posts)} posts récupérés depuis Discover.")
        return posts

    # === Cas standard (whats_hot, timeline, etc.) ===
    feed = data.get("feed", data.get("posts", []))

    for item in feed:
        post_data = item.get("post", item)
        record = post_data.get("record", {})
        author = post_data.get("author", {})

        posts.append({
            "source": endpoint_name,
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

    logger.info(f"✅ {len(posts)} posts récupérés sur {endpoint_name}")
    return posts


if __name__ == "__main__":
    all_posts = []
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DBNAME = os.getenv("MONGO_DBNAME")

    for endpoint_name in ENDPOINTS.keys():
        posts = fetch_posts(endpoint_name, limit=30)
        all_posts.extend(posts)

    if all_posts:
        output_path = BASE_DIR / "bluesky_posts_all.json"
        # envoie mongo
        # with open(output_path, "w", encoding="utf-8") as f:
        #     json.dump(all_posts, f, ensure_ascii=False, indent=2)
        # logger.info(f"📁 {len(all_posts)} posts au total sauvegardés dans : {output_path}")
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DBNAME]
        collection = db["bluesky_posts"]
        existing_uris = set(doc["uri"] for doc in collection.find({}, {"uri": 1}) if "uri" in doc)
        new_posts = [p for p in posts if p.get("uri") not in existing_uris]
        logger.info(f"📁 {len(all_posts)} posts au total récupérés.")
        collection.insert_many(new_posts)
        logger.info(f"📁 {len(all_posts)} posts au total récupérés.")
        logger.info(f"✅ {len(new_posts)} nouveaux documents insérés dans la collection '{collection.name}' de la base '{MONGO_DBNAME}'.")
    else:
        logger.info("⚠️ Aucun nouveau post à insérer (tous déjà présents).")

