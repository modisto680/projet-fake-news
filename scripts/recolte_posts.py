import json
import os
import time
import random
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:
    MongoClient = None  # type: ignore
    UpdateOne = None  # type: ignore


# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"C:\Users\boiss\Desktop\facts_check")
ENV_PATH = BASE_DIR / ".env"
TOKEN_PATH = BASE_DIR / "token.json"

load_dotenv(dotenv_path=ENV_PATH)

# Mongo (facultatif)
MONGO_URI = os.getenv("MONGO_URI", "")  # ex: mongodb://localhost:27017
MONGO_DB = os.getenv("MONGO_DB", "thumalien")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "posts")

SEARCH_QUERIES = [
    "Discover",
    "Trending",
    "Hot Topics",
    "politics",
    "election",
    "vote",
    "war",
    "ukraine",
    "gaza",
    "climate",
]

SEARCH_LIMIT = 100
POPULAR_LIMIT = 100
MAX_PAGES_PER_QUERY = 25
MAX_PAGES_POPULAR = 50

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.25

MAX_RETRIES = 5
BACKOFF_BASE_SEC = 0.8


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
        raise FileNotFoundError(f"token.json introuvable: {token_path}")
    with open(token_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_token(token_path: Path, token_data: Dict[str, Any]) -> None:
    with open(token_path, "w", encoding="utf-8") as f:
        json.dump(token_data, f, ensure_ascii=False, indent=2)


def get_pds_base_url(token_data: Dict[str, Any]) -> str:
    """
    Bluesky/ATProto: les appels XRPC doivent aller sur le PDS.
    Dans ton token.json, il est dans didDoc.service[].serviceEndpoint
    """
    did_doc = token_data.get("didDoc", {})
    services = did_doc.get("service", [])
    if not isinstance(services, list) or not services:
        raise ValueError("didDoc.service introuvable dans token.json")
    # Cherche le service ATProto PDS
    for s in services:
        if isinstance(s, dict) and s.get("type") == "AtprotoPersonalDataServer":
            endpoint = s.get("serviceEndpoint")
            if endpoint:
                return str(endpoint).rstrip("/")
    # fallback: premier serviceEndpoint trouvé
    endpoint = services[0].get("serviceEndpoint") if isinstance(services[0], dict) else None
    if not endpoint:
        raise ValueError("serviceEndpoint introuvable dans didDoc.service")
    return str(endpoint).rstrip("/")


def xrpc_url(pds_base: str, method: str) -> str:
    return f"{pds_base}/xrpc/{method}"


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
    """
    Returns (status_code, json_data, raw_text).
    """
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

            # transient errors / rate limit
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                log.warning("HTTP %s -> retry %d/%d in %.2fs (%s)", resp.status_code, attempt, MAX_RETRIES, wait, url)
                time.sleep(wait)
                continue

            # Try JSON parse (even if content-type imperfect)
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
    """
    Refresh accessJwt using refreshJwt.
    - Calls com.atproto.server.refreshSession on the PDS
    - Updates token_data with new accessJwt/refreshJwt/did/handle if returned
    - Writes back token.json
    """
    refresh_jwt = token_data.get("refreshJwt")
    if not refresh_jwt:
        raise RuntimeError("refreshJwt manquant dans token.json, impossible de refresh.")

    url = xrpc_url(pds_base, "com.atproto.server.refreshSession")
    headers = {"Authorization": f"Bearer {refresh_jwt}"}

    status, data, text = request_with_retries(session, "POST", url, headers=headers)

    if status != 200:
        raise RuntimeError(f"Refresh session échoué ({status}): {text[:300]}")

    # Merge: keep didDoc etc, replace tokens if present
    for k in ["accessJwt", "refreshJwt", "did", "handle", "active"]:
        if k in data:
            token_data[k] = data[k]

    # Some implementations may return didDoc too; keep if present
    if "didDoc" in data:
        token_data["didDoc"] = data["didDoc"]

    save_token(token_path, token_data)
    log.info("✅ Token refresh OK -> token.json mis à jour.")
    return token_data


def auth_headers(access_jwt: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_jwt}"}


# =========================
# BLUESKY COLLECTORS
# =========================
def call_xrpc_get_with_auto_refresh(
    session: requests.Session,
    pds_base: str,
    token_data: Dict[str, Any],
    token_path: Path,
    method: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    GET call with:
    - accessJwt
    - if 401 -> refreshSession -> retry once
    """
    access_jwt = token_data.get("accessJwt")
    if not access_jwt:
        raise RuntimeError("accessJwt manquant dans token.json")

    url = xrpc_url(pds_base, method)
    status, data, text = request_with_retries(
        session,
        "GET",
        url,
        params=params,
        headers=auth_headers(access_jwt),
    )

    if status == 401:
        log.warning("Access token expiré (401). Tentative de refresh...")
        token_data = refresh_session(session, pds_base, token_data, token_path)
        access_jwt = token_data.get("accessJwt")
        status, data, text = request_with_retries(
            session,
            "GET",
            url,
            params=params,
            headers=auth_headers(access_jwt),
        )

    if status != 200:
        raise RuntimeError(f"XRPC {method} échoué ({status}): {text[:400]}")

    return data


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
                p["_query"] = query
                p["_collected_at"] = int(time.time())
                out.append(p)

        log.info("searchPosts q=%r page=%d got=%d cursor=%s", query, page, len(posts), "yes" if cursor else "no")
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
                post["_collected_at"] = int(time.time())
                out.append(post)
                added += 1

        log.info("getPopular page=%d got_posts=%d cursor=%s", page, added, "yes" if cursor else "no")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        if not cursor or not feed:
            break

    return out


# =========================
# MONGO (OPTIONNEL)
# =========================
def mongo_enabled() -> bool:
    return bool(MONGO_URI) and (MongoClient is not None)


def mongo_connect():
    if not mongo_enabled():
        return None
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]
    try:
        col.create_index("uri", unique=True, sparse=True)
        col.create_index("cid", unique=True, sparse=True)
    except Exception as e:
        log.warning("Mongo index warning: %s", e)
    return col


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

    res = col.bulk_write(ops, ordered=False)
    touched = res.upserted_count + res.modified_count + res.matched_count
    return touched, skipped


# =========================
# MAIN
# =========================
def main():
    token_data = load_token(TOKEN_PATH)
    pds_base = get_pds_base_url(token_data)
    log.info("PDS détecté depuis token.json: %s", pds_base)

    all_posts: List[Dict[str, Any]] = []

    with requests.Session() as session:
        # 1) searchPosts for each query
        for q in SEARCH_QUERIES:
            try:
                all_posts.extend(search_posts(session, pds_base, token_data, TOKEN_PATH, q))
            except Exception as e:
                log.error("searchPosts failed q=%r: %s", q, e)

        # 2) getPopular (best-effort)
        try:
            all_posts.extend(get_popular(session, pds_base, token_data, TOKEN_PATH))
        except Exception as e:
            log.warning("getPopular indisponible ou erreur: %s", e)

    # Dedup by uri (fallback cid)
    dedup: Dict[str, Dict[str, Any]] = {}
    for p in all_posts:
        key = p.get("uri") or p.get("cid")
        if key and key not in dedup:
            dedup[key] = p

    unique_posts = list(dedup.values())
    log.info("Collected total=%d unique=%d", len(all_posts), len(unique_posts))

    # Persist
    if mongo_enabled():
        col = mongo_connect()
        if col is None:
            log.warning("Mongo activé mais connexion impossible (pymongo manquant ?).")
        else:
            touched, skipped = mongo_upsert_posts(col, unique_posts)
            log.info("Mongo upsert OK. touched=%d skipped_no_key=%d", touched, skipped)
    else:
        out_path = BASE_DIR / "bsky_posts_dump.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(unique_posts, f, ensure_ascii=False, indent=2)
        log.info("Mongo non configuré. Dump JSON écrit: %s", out_path)


if __name__ == "__main__":
    main()
