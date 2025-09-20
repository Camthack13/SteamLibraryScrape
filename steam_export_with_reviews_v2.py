#!/usr/bin/env python3
import argparse
import csv
import sys
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

REVIEWS_URL_TMPL = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&filter=recent&num_per_page=0&day_range=30"

def build_profile_parts(profile_url: str):
    u = urlparse(profile_url.strip())
    if not u.netloc or "steamcommunity.com" not in u.netloc:
        raise ValueError("URL must be a steamcommunity.com profile URL.")
    m = re.match(r"^/(id|profiles)/([^/]+)/?", u.path or "")
    if not m:
        raise ValueError("Profile URL should look like https://steamcommunity.com/id/<name> or /profiles/<steamid64>")
    return m.group(1), m.group(2)

def build_xml_url(profile_url: str) -> str:
    kind, ident = build_profile_parts(profile_url)
    base_path = f"/{kind}/{ident}/games"
    query = "tab=all&xml=1"
    return urlunparse(("https", "steamcommunity.com", base_path, "", query, ""))

def build_html_url(profile_url: str) -> str:
    kind, ident = build_profile_parts(profile_url)
    base_path = f"/{kind}/{ident}/games/"
    query = "tab=all"
    return urlunparse(("https", "steamcommunity.com", base_path, "", query, ""))

def http_session():
    s = requests.Session()
    # a normal browser-y UA helps Steam return the embedded JS
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/118 Safari/537.36"})
    return s

def fetch_games_xml(session: requests.Session, xml_url: str, debug: bool) -> ET.Element:
    r = session.get(xml_url, timeout=30)
    r.raise_for_status()
    if debug:
        print("[debug] XML URL:", xml_url)
        print("[debug] XML head:", r.text[:600].replace("\n", " "))
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse XML: {e}") from e
    return root

def parse_games_from_xml(root: ET.Element, debug: bool):
    # try multiple shapes
    nodes = (
        root.findall(".//gamesList/games/game")
        or root.findall(".//games/game")
        or root.findall(".//game")
    )
    privacy = (root.findtext(".//privacyState") or "").strip().lower()
    game_count_text = (root.findtext(".//gameCount") or "").strip()

    if not nodes:
        if debug:
            print(f"[debug] XML privacyState={privacy or 'N/A'} gameCount={game_count_text or 'N/A'}")
        return []  # let caller decide to fallback

    def parse_hours(s):
        try:
            return float((s or "").replace(",", "")) if s else 0.0
        except ValueError:
            return 0.0

    out = []
    for g in nodes:
        def txt(tag):
            node = g.find(tag)
            return node.text.strip() if node is not None and node.text else ""
        appid = txt("appID") or txt("appId") or txt("appId64")
        name = txt("name")
        out.append({
            "appid": appid,
            "name": name,
            "hours_on_record": parse_hours(txt("hoursOnRecord")),
            "hours_last_2_weeks": parse_hours(txt("hoursLast2Weeks")),
            "store_link": txt("storeLink"),
            "logo": txt("logo"),
        })
    return out

_RG_GAMES_REGEX = re.compile(r"var\s+rgGames\s*=\s*(\[\s*{.*?}\s*\])\s*;", re.DOTALL)

def fetch_and_parse_games_from_html(session: requests.Session, html_url: str, debug: bool):
    r = session.get(html_url, timeout=30)
    r.raise_for_status()
    text = r.text
    if debug:
        print("[debug] HTML URL:", html_url)
        print("[debug] HTML head:", text[:600].replace("\n", " "))

    m = _RG_GAMES_REGEX.search(text)
    if not m:
        # Sometimes Steam compresses/obfuscates; try a looser capture for JSON and then json.loads
        # Look for a JSON array starting with {"appid":
        m2 = re.search(r"rgGames\s*=\s*(\[[^\]]*\"appid\"[^\]]*\])\s*;", text, re.DOTALL)
        if not m2:
            return []
        blob = m2.group(1)
    else:
        blob = m.group(1)

    try:
        rg = json.loads(blob)
    except json.JSONDecodeError:
        # try to clean up trailing commas or weird escapes
        blob2 = re.sub(r",(\s*])", r"\1", blob)  # remove trailing comma before ]
        rg = json.loads(blob2)

    out = []
    for g in rg:
        # observed fields in rgGames items
        appid = str(g.get("appid") or g.get("appID") or "")
        name = g.get("name") or g.get("friendly_name") or ""
        hours_on_record = float(g.get("hours_forever", 0) or 0)
        store_link = f"https://store.steampowered.com/app/{appid}/" if appid else ""
        out.append({
            "appid": appid,
            "name": name,
            "hours_on_record": hours_on_record,
            "store_link": store_link,
        })
    return out

def fetch_recent_reviews_for_app(session: requests.Session, appid: str, retries: int = 3, backoff: float = 1.2):
    url = REVIEWS_URL_TMPL.format(appid=appid)
    last_exc = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            if r.status_code in (429, 503):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            data = r.json()
            q = data.get("query_summary", {}) or {}
            score_desc = q.get("review_score_desc", "")
            score = q.get("review_score")
            total = q.get("total_reviews", 0)
            pos = q.get("total_positive", 0)
            neg = q.get("total_negative", 0)
            pct_pos = (pos / total * 100.0) if total else None
            return {
                "recent_review_score": score,
                "recent_review_desc": score_desc,
                "recent_total_reviews": total,
                "recent_positive": pos,
                "recent_negative": neg,
                "recent_percent_positive": round(pct_pos, 2) if pct_pos is not None else None,
            }
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            time.sleep((backoff ** attempt) + 0.2)
    sys.stderr.write(f"Warning: reviews fetch failed for appid {appid}: {last_exc}\n")
    return {
        "recent_review_score": None,
        "recent_review_desc": "",
        "recent_total_reviews": None,
        "recent_positive": None,
        "recent_negative": None,
        "recent_percent_positive": None,
    }

def enrich_with_reviews(session: requests.Session, games, workers: int = 8):
    out = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(fetch_recent_reviews_for_app, session, g["appid"]): g for g in games if g.get("appid")}
        for fut in as_completed(futs):
            g = futs[fut]
            reviews = fut.result()
            g2 = {**g, **reviews}
            out.append(g2)
    order = {g["appid"]: i for i, g in enumerate(games)}
    out.sort(key=lambda r: order.get(r.get("appid", ""), 0))
    return out

def default_output_name(profile_url: str) -> str:
    m = re.search(r"/(id|profiles)/([^/]+)/?", profile_url)
    who = m.group(2) if m else "steam_profile"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"steam_library_{who}_{timestamp}.csv"

def write_csv(rows, out_path: Path, include_reviews: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base_fields = ["appid", "name", "hours_on_record", "hours_last_2_weeks", "store_link", "logo"]
    review_fields = [
        "recent_review_score",
        "recent_review_desc",
        "recent_total_reviews",
        "recent_positive",
        "recent_negative",
        "recent_percent_positive",
    ]
    fieldnames = base_fields + (review_fields if include_reviews else [])
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})

def main():
    ap = argparse.ArgumentParser(description="Export Steam library to CSV (XML first, fallback to HTML rgGames; optional recent review summaries).")
    ap.add_argument("profile_url", help="Your Steam profile URL (e.g., https://steamcommunity.com/id/yourname or /profiles/7656119...)")
    ap.add_argument("-o", "--output", help="Output CSV path (default: steam_library_<name>_<timestamp>.csv)")
    ap.add_argument("--no-reviews", dest="reviews", action="store_false", help="Do NOT fetch recent review summaries (faster).")
    ap.add_argument("--workers", type=int, default=8, help="Concurrent workers for fetching reviews (default: 8).")
    ap.add_argument("--debug", action="store_true", help="Print debug info and response snippets.")
    args = ap.parse_args()

    try:
        session = http_session()
        # 1) Try XML
        xml_url = build_xml_url(args.profile_url)
        games = []
        try:
            root = fetch_games_xml(session, xml_url, args.debug)
            games = parse_games_from_xml(root, args.debug)
        except Exception as e:
            if args.debug:
                print("[debug] XML fetch/parse error:", e)

        # 2) Fallback to HTML rgGames if XML empty
        if not games:
            if args.debug:
                print("[debug] XML returned no games. Trying HTML rgGames fallback.")
            html_url = build_html_url(args.profile_url)
            games = fetch_and_parse_games_from_html(session, html_url, args.debug)

        if not games:
            raise RuntimeError("No games found via XML or HTML. Check Game details privacy and try again.")

        rows = games
        if args.reviews:
            rows = enrich_with_reviews(session, games, workers=args.workers)

        out_name = args.output or default_output_name(args.profile_url)
        out_path = Path(out_name)
        write_csv(rows, out_path, include_reviews=args.reviews)
        print(f"Exported {len(rows)} games to {out_path.resolve()}")
        if args.reviews:
            print("Included: recent review score (0–9), description, totals, and % positive (last ~30 days).")
    except (ValueError, RuntimeError, requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
