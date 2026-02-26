\
"""
Collecte Bluesky -> MongoDB (RAW)

Endpoints utilisés (officiels) :
- app.bsky.feed.getTimeline
- app.bsky.feed.searchPosts (pour simuler Discover/Trending/Hot Topics)
- app.bsky.feed.getPopular (si autorisé)

Collections Mongo (RAW) :
- timeline
- feed_discover
- feed_trending
- feed_hot_topics
- feed_popular
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None  # type: ignore
    UpdateOne = None  # type: ignore


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parent        # .../scripts
PROJECT_ROOT = BASE_DIR.parent                    # racine projet

# On accepte .env à la racine OU dans scripts/
for env_path in [PROJECT_ROOT / ".env", BASE_DIR / ".env"]:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        break

TOKEN_PATH = BASE_DIR / "token.json"

MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "thumalien")

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.25

MAX_RETRIES = 5
BACKOFF_BASE_SEC = 0.8

SEARCH_LIMIT = 100
MAX_PAGES_PER_QUERY = 15

TIMELINE_LIMIT = 100
MAX_PAGES_TIMELINE = 20

POPULAR_LIMIT = 100
MAX_PAGES_POPULAR = 30


# Mots-clés demandés (simulation des flux) - voir le PDF endpoints
FEED_KEYWORDS = {
    "feed_discover": ["news", "world", "science", "tech"],
    "feed_trending": ["breaking", "urgent", "live"],
    "feed_hot_topics": ["politics", "election", "covid", "crisis"],
}


# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bsky-collector")


# =========================
# TOKEN HELPERS
# =========================
def load_token(token_path: Path) -> Dict[str, Any]:
    if not token_path.exists():
        raise FileNotFoundError(
            f"token.json introuvable: {token_path}. "
            "Lance d'abord: python scripts/login.py"
        )
    return json.loads(token_path.read_text(encoding="utf-8"))


def save_token(token_path: Path, token_data: Dict[str, Any]) -> None:
    token_path.write_text(json.dumps(token_data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_pds_base_url(token_data: Dict[str, Any]) -> str:
    """
    Bluesky/ATProto : les appels XRPC doivent aller sur le PDS de ton compte.
    Il est dans token.json -> didDoc.service[].serviceEndpoint
    """
    did_doc = token_data.get("didDoc", {})
    services = did_doc.get("service", [])
    if not isinstance(services, list) or not services:
        raise ValueError("didDoc.service introuvable dans token.json")

    for s in services:
        if isinstance(s, dict) and s.get("type") == "AtprotoPersonalDataServer":
            endpoint = s.get("serviceEndpoint")
            if endpoint:
                return str(endpoint).rstrip("/")

    # fallback: premier serviceEndpoint
    endpoint = services[0].get("serviceEndpoint") if isinstance(services[0], dict) else None
    if not endpoint:
        raise ValueError("serviceEndpoint introuvable dans didDoc.service")
    return str(endpoint).rstrip("/")


def xrpc_url(pds_base: str, method: str) -> str:
    return f"{pds_base}/xrpc/{method}"


def auth_headers(access_jwt: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_jwt}"}


# =========================
# HTTP HELPERS
# =========================
def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, Any], str]:
    last_text = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            last_text = resp.text

            if resp.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                log.warning("HTTP %s -> retry %d/%d in %.2fs (%s)", resp.status_code, attempt, MAX_RETRIES, wait, url)
                time.sleep(wait)
                continue

            try:
                data = resp.json()
            except Exception:
                data = {}

            return resp.status_code, data, last_text

        except Exception as e:
            wait = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
            log.warning("Request error: %s -> retry %d/%d in %.2fs", e, attempt, MAX_RETRIES, wait)
            time.sleep(wait)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}\nLast response: {last_text[:500]}")


def refresh_session(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
) -> Dict[str, Any]:
    refresh_jwt = token_data.get("refreshJwt")
    if not refresh_jwt:
        raise RuntimeError("refreshJwt manquant dans token.json, impossible de refresh.")

    url = xrpc_url(pds_base, "com.atproto.server.refreshSession")
    headers = {"Authorization": f"Bearer {refresh_jwt}"}

    status, data, text = request_with_retries(session, "POST", url, headers=headers)

    if status != 200:
        raise RuntimeError(f"Refresh session échoué ({status}): {text[:300]}")

    for k in ["accessJwt", "refreshJwt", "did", "handle", "active"]:
        if k in data:
            token_data[k] = data[k]
    if "didDoc" in data:
        token_data["didDoc"] = data["didDoc"]

    save_token(token_path, token_data)
    log.info("✅ Token refresh OK -> token.json mis à jour.")
    return token_data


def call_xrpc_get_with_auto_refresh(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
    method: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    access_jwt = token_data.get("accessJwt")
    if not access_jwt:
        raise RuntimeError("accessJwt manquant dans token.json")

    url = xrpc_url(pds_base, method)
    status, data, text = request_with_retries(
        session, "GET", url, params=params, headers=auth_headers(access_jwt)
    )

    if status == 401:
        log.warning("Access token expiré (401). Tentative de refresh...")
        token_data = refresh_session(session, pds_base, token_data, token_path)
        access_jwt = token_data.get("accessJwt", "")
        status, data, text = request_with_retries(
            session, "GET", url, params=params, headers=auth_headers(access_jwt)
        )

    if status != 200:
        raise RuntimeError(f"XRPC {method} échoué ({status}): {text[:400]}")

    return data


# =========================
# COLLECTORS
# =========================
def get_timeline(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
    *,
    limit: int = TIMELINE_LIMIT,
    max_pages: int = MAX_PAGES_TIMELINE,
) -> List[Dict[str, Any]]:
    cursor: Optional[str] = None
    out: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        params: Dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        data = call_xrpc_get_with_auto_refresh(
            session, pds_base, token_data, token_path, "app.bsky.feed.getTimeline", params
        )

        feed = data.get("feed") or []
        cursor = data.get("cursor")

        added = 0
        for entry in feed:
            post = entry.get("post") if isinstance(entry, dict) else None
            if isinstance(post, dict):
                post["_source"] = "getTimeline"
                post["_collected_at"] = int(time.time())
                out.append(post)
                added += 1

        log.info("getTimeline page=%d got_posts=%d cursor=%s", page, added, "yes" if cursor else "no")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        if not cursor or not feed:
            break

    return out


def search_posts(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
    query: str,
    *,
    limit: int = SEARCH_LIMIT,
    max_pages: int = MAX_PAGES_PER_QUERY,
    sort: str = "latest",
    feed_name: str = "feed_discover",
) -> List[Dict[str, Any]]:
    cursor: Optional[str] = None
    out: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        params: Dict[str, Any] = {"q": query, "limit": limit, "sort": sort}
        if cursor:
            params["cursor"] = cursor

        data = call_xrpc_get_with_auto_refresh(
            session, pds_base, token_data, token_path, "app.bsky.feed.searchPosts", params
        )

        posts = data.get("posts") or []
        cursor = data.get("cursor")

        for p in posts:
            if isinstance(p, dict):
                p["_source"] = "searchPosts"
                p["_feed"] = feed_name
                p["_query"] = query
                p["_collected_at"] = int(time.time())
                out.append(p)

        log.info("searchPosts feed=%s q=%r page=%d got=%d cursor=%s", feed_name, query, page, len(posts), "yes" if cursor else "no")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        if not cursor or not posts:
            break

    return out


def get_popular(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
    *,
    limit: int = POPULAR_LIMIT,
    max_pages: int = MAX_PAGES_POPULAR,
) -> List[Dict[str, Any]]:
    cursor: Optional[str] = None
    out: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        params: Dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        data = call_xrpc_get_with_auto_refresh(
            session, pds_base, token_data, token_path, "app.bsky.feed.getPopular", params
        )

        feed = data.get("feed") or []
        cursor = data.get("cursor")

        added = 0
        for entry in feed:
            post = entry.get("post") if isinstance(entry, dict) else None
            if isinstance(post, dict):
                post["_source"] = "getPopular"
                post["_feed"] = "feed_popular"
                post["_collected_at"] = int(time.time())
                out.append(post)
                added += 1

        log.info("getPopular page=%d got_posts=%d cursor=%s", page, added, "yes" if cursor else "no")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        if not cursor or not feed:
            break

    return out


# =========================
# MONGO
# =========================
def mongo_enabled() -> bool:
    return bool(MONGO_URI) and (MongoClient is not None)


def mongo_client() -> "MongoClient":
    if not mongo_enabled():
        raise RuntimeError("Mongo non configuré (MONGO_URI manquant ou pymongo absent).")
    return MongoClient(MONGO_URI)


def mongo_upsert_posts(col, posts: List[Dict[str, Any]]) -> Tuple[int, int]:
    ops = []
    skipped = 0

    for p in posts:
        key_uri = p.get("uri")
        key_cid = p.get("cid")
        if key_uri:
            filt = {"uri": key_uri}
        elif key_cid:
            filt = {"cid": key_cid}
        else:
            skipped += 1
            continue

        ops.append(UpdateOne(filt, {"$set": p}, upsert=True))

    if not ops:
        return 0, skipped

    from pymongo.errors import BulkWriteError

    try:
        res = col.bulk_write(ops, ordered=False)
        touched = res.upserted_count + res.modified_count + res.matched_count
    except BulkWriteError:
        log.warning("Duplicates detected and ignored.")
        touched = 0

    return touched, skipped



# =========================
# MAIN
# =========================
def main():
    token_data = load_token(TOKEN_PATH)
    pds_base = get_pds_base_url(token_data)
    log.info("PDS détecté depuis token.json: %s", pds_base)

    collected_by_collection: Dict[str, List[Dict[str, Any]]] = {
        "timeline": [],
        "feed_discover": [],
        "feed_trending": [],
        "feed_hot_topics": [],
        "feed_popular": [],
    }

    with requests.Session() as session:
        # 1) Timeline
        try:
            collected_by_collection["timeline"] = get_timeline(session, pds_base, token_data, TOKEN_PATH)
        except Exception as e:
            log.error("getTimeline failed: %s", e)

        # 2) searchPosts pour simuler les flux
        for feed_name, keywords in FEED_KEYWORDS.items():
            for kw in keywords:
                try:
                    collected_by_collection[feed_name].extend(
                        search_posts(session, pds_base, token_data, TOKEN_PATH, kw, feed_name=feed_name)
                    )
                except Exception as e:
                    log.error("searchPosts failed feed=%s kw=%r: %s", feed_name, kw, e)

        # 3) Popular (best-effort)
        try:
            collected_by_collection["feed_popular"] = get_popular(session, pds_base, token_data, TOKEN_PATH)
        except Exception as e:
            log.warning("getPopular indisponible ou erreur: %s", e)

    # Dedup DANS chaque collection
    for col_name, posts in collected_by_collection.items():
        dedup: Dict[str, Dict[str, Any]] = {}
        for p in posts:
            key = p.get("uri") or p.get("cid")
            if key and key not in dedup:
                dedup[key] = p
        collected_by_collection[col_name] = list(dedup.values())

    total = sum(len(v) for v in collected_by_collection.values())
    log.info("Collected total unique posts across collections=%d", total)

    # Persist
    if mongo_enabled():
        client = mongo_client()
        db = client[MONGO_DB]

        for collection_name, posts in collected_by_collection.items():
            col = db[collection_name]
            try:
                col.create_index("uri", unique=True, sparse=True)
                col.create_index("cid", unique=True, sparse=True)
            except Exception:
                pass

            touched, skipped = mongo_upsert_posts(col, posts)
            log.info("Mongo upsert %s OK. posts=%d touched=%d skipped_no_key=%d", collection_name, len(posts), touched, skipped)

    else:
        # fallback JSON dans data/raw
        raw_dir = PROJECT_ROOT / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        out_path = raw_dir / f"bsky_raw_{int(time.time())}.json"
        out_path.write_text(json.dumps(collected_by_collection, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Mongo non configuré. Dump JSON écrit: %s", out_path)


if __name__ == "__main__":
    main()
