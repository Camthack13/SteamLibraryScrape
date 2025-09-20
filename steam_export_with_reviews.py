#!/usr/bin/env python3
import argparse
import csv
import sys
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

REVIEWS_URL_TMPL = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&filter=recent&num_per_page=0&day_range=30"

def build_xml_url(profile_url: str) -> str:
    u = urlparse(profile_url.strip())
    if not u.netloc:
        raise ValueError("Please include a full URL like https://steamcommunity.com/id/yourname")
    if "steamcommunity.com" not in u.netloc:
        raise ValueError("URL must be a steamcommunity.com profile URL.")
    m = re.match(r"^/(id|profiles)/([^/]+)/?", u.path or "")
    if not m:
        raise ValueError("Profile URL should look like https://steamcommunity.com/id/<name> or /profiles/<steamid64>")
    base_path = f"/{m.group(1)}/{m.group(2)}/games"
    query = "tab=all&xml=1"
    return urlunparse(("https", "steamcommunity.com", base_path, "", query, ""))

def http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SteamLibraryExport/1.1)"})
    return s

def fetch_games_xml(session: "requests.Session", xml_url: str) -> ET.Element:
    r = session.get(xml_url, timeout=30)
    r.raise_for_status()
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse XML: {e}") from e
    return root

def parse_games(root: ET.Element):
    games_nodes = root.findall(".//gamesList/games/game")
    if not games_nodes:
        privacy = root.findtext(".//privacyState")
        if privacy and privacy.lower() != "public":
            raise RuntimeError("Profile or game details are not public. Make sure 'Game details' are set to Public in Steam privacy settings.")
        raise RuntimeError("No games found in XML. Is the profile correct and games list visible?")

    def parse_hours(s):
        try:
            return float(s.replace(",", "")) if s else 0.0
        except ValueError:
            return 0.0

    games = []
    for g in games_nodes:
        def txt(tag):
            node = g.find(tag)
            return node.text.strip() if node is not None and node.text else ""
        appid = txt("appID") or txt("appId")
        name = txt("name")
        games.append({
            "appid": appid,
            "name": name,
            "hours_on_record": parse_hours(txt("hoursOnRecord")),
            "hours_last_2_weeks": parse_hours(txt("hoursLast2Weeks")),
            "store_link": txt("storeLink"),
            "logo": txt("logo"),
        })
    return games

def fetch_recent_reviews_for_app(session: requests.Session, appid: str, retries: int = 3, backoff: float = 1.2):
    """
    Uses the official store reviews endpoint (no scraping) and returns summary stats for the last ~30 days.
    """
    url = REVIEWS_URL_TMPL.format(appid=appid)
    last_exc = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            # Handle soft rate-limit
            if r.status_code in (429, 503):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            data = r.json()
            q = data.get("query_summary", {}) or {}
            score_desc = q.get("review_score_desc", "")
            score = q.get("review_score")  # 0..9
            total = q.get("total_reviews", 0)
            pos = q.get("total_positive", 0)
            neg = q.get("total_negative", 0)
            pct_pos = (pos / total * 100.0) if total else None
            return {
                "recent_review_score": score,
                "recent_review_desc": score_desc,  # e.g., "Mostly Positive"
                "recent_total_reviews": total,
                "recent_positive": pos,
                "recent_negative": neg,
                "recent_percent_positive": round(pct_pos, 2) if pct_pos is not None else None,
            }
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            # polite exponential backoff
            time.sleep((backoff ** attempt) + 0.2)
    # If it keeps failing, return blanks so the row still writes
    sys.stderr.write(f"Warning: reviews fetch failed for appid {appid}: {last_exc}\n")
    return {
        "recent_review_score": None,
        "recent_review_desc": "",
        "recent_total_reviews": None,
        "recent_positive": None,
        "recent_negative": None,
        "recent_percent_positive": None,
    }

def enrich_with_reviews(games, workers: int = 8):
    session = http_session()
    out = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(fetch_recent_reviews_for_app, session, g["appid"]): g for g in games if g.get("appid")}
        for fut in as_completed(futs):
            g = futs[fut]
            reviews = fut.result()
            g2 = {**g, **reviews}
            out.append(g2)
    # Preserve original order (ThreadPoolExecutor returns as they complete)
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
        "recent_review_score",          # 0..9 (Steam's bucketing)
        "recent_review_desc",           # e.g., "Mostly Positive"
        "recent_total_reviews",
        "recent_positive",
        "recent_negative",
        "recent_percent_positive",      # computed
    ]
    fieldnames = base_fields + (review_fields if include_reviews else [])
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})

def main():
    ap = argparse.ArgumentParser(description="Export Steam library to CSV (optionally include recent review scores).")
    ap.add_argument("profile_url", help="Your Steam profile URL (e.g., https://steamcommunity.com/id/yourname or /profiles/7656119...)")
    ap.add_argument("-o", "--output", help="Output CSV path (default: steam_library_<name>_<timestamp>.csv)")
    ap.add_argument("--no-reviews", dest="reviews", action="store_false",
                    help="Do NOT fetch recent review summaries (faster).")
    ap.add_argument("--workers", type=int, default=8,
                    help="Concurrent workers for fetching reviews (default: 8).")
    args = ap.parse_args()

    try:
        session = http_session()
        xml_url = build_xml_url(args.profile_url)
        root = fetch_games_xml(session, xml_url)
        games = parse_games(root)

        rows = games
        if args.reviews:
            rows = enrich_with_reviews(games, workers=args.workers)

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
