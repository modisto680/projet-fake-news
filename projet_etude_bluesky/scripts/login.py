\
"""
Bluesky login (ATProto) -> crée/rafraîchit token.json

Niveau débutant :
- Mets tes identifiants dans .env (à la racine du projet OU dans scripts/.env)
- Lance: python scripts/login.py
- Le script écrit scripts/token.json (à ne pas committer)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent  # .../scripts
PROJECT_ROOT = BASE_DIR.parent              # racine du projet

# On accepte .env à la racine OU dans scripts/
for env_path in [PROJECT_ROOT / ".env", BASE_DIR / ".env"]:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        break

TOKEN_PATH = BASE_DIR / "token.json"

BLUESKY_IDENTIFIER = os.getenv("BLUESKY_IDENTIFIER")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")

if not BLUESKY_IDENTIFIER or not BLUESKY_PASSWORD:
    raise RuntimeError(
        "BLUESKY_IDENTIFIER ou BLUESKY_PASSWORD manquant dans .env. "
        "Copie .env.example en .env puis remplis les champs."
    )

URL = "https://bsky.social/xrpc/com.atproto.server.createSession"

resp = requests.post(
    URL,
    json={"identifier": BLUESKY_IDENTIFIER, "password": BLUESKY_PASSWORD},
    timeout=20,
)
if resp.status_code != 200:
    raise RuntimeError(f"Login Bluesky échoué ({resp.status_code}): {resp.text[:300]}")

token_data = resp.json()

# Sauvegarde token.json
TOKEN_PATH.write_text(json.dumps(token_data, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"✅ token.json écrit: {TOKEN_PATH}")
print("ℹ️  Prochaine étape: python scripts/collect_posts.py")
