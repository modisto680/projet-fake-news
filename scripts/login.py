import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path

# === Définition du chemin racine du projet ===
BASE_DIR = Path(r"C:\Users\boiss\Desktop\facts_check")

# === Chargement des variables d'environnement ===
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

BLUESKY_IDENTIFIER = os.getenv("BLUESKY_IDENTIFIER")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")

print("DEBUG - Identifiant chargé :", BLUESKY_IDENTIFIER)
print("DEBUG - Mot de passe chargé :", "****" if BLUESKY_PASSWORD else None)

# === URL de connexion à Bluesky ===
url = "https://bsky.social/xrpc/com.atproto.server.createSession"

# === Données à envoyer ===
payload = {
    "identifier": BLUESKY_IDENTIFIER,
    "password": BLUESKY_PASSWORD
}

headers = {"Content-Type": "application/json"}

# === Requête HTTP ===
response = requests.post(url, headers=headers, data=json.dumps(payload))

# === Vérification du résultat ===
if response.status_code == 200:
    tokens = response.json()

    # Chemin de sauvegarde du token.json
    token_path = BASE_DIR / "token.json"

    # Sauvegarde du token
    with open(token_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)

    print(f"✅ Token sauvegardé dans : {token_path}")

else:
    print(f"❌ Échec de la connexion ({response.status_code}) : {response.text}")
