#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

import requests
import xml.etree.ElementTree as ET

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/118 Safari/537.36"

# Reviews endpoints
REVIEWS_RECENT = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&filter=recent&day_range={day_range}&num_per_page=0"
REVIEWS_OVERALL = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&num_per_page=0"

# Store pages / details
APPDETAILS_PC_REQ = "https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=en&filters=pc_requirements"
STORE_PAGE = "https://store.steampowered.com/app/{appid}/?l=english&cc=US"

RG_GAMES_REGEX = re.compile(r"var\s+rgGames\s*=\s*(\[\s*{.*?}\s*\])\s*;", re.DOTALL)

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

# ---------- Input parsing ----------
def parse_ids_file(path: str):
    """
    Expects lines like:  Username: 76561198064184537
    - Right side can also be a profile URL.
    - Ignores blank lines and lines starting with '#'.
    Returns list of dicts: {"label": "...", "steamid64": "..."} (deduped by steamid64)
    """
    out = []
    seen = set()
    for lineno, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # split at first ':'
        if ":" not in line:
            print(f"Warning: line {lineno} missing ':' -> {raw!r}", file=sys.stderr)
            continue
        label, ident = [p.strip() for p in line.split(":", 1)]
        if not label or not ident:
            print(f"Warning: line {lineno} not 'Label: Value' -> {raw!r}", file=sys.stderr)
            continue
        sid64 = normalize_to_steamid64(ident)
        if not sid64:
            print(f"Warning: line {lineno} could not resolve SteamID -> {raw!r}", file=sys.stderr)
            continue
        if sid64 in seen:
            # preserve the first label we saw, skip duplicates
            continue
        seen.add(sid64)
        out.append({"label": label, "steamid64": sid64})
    return out

def normalize_to_steamid64(val: str) -> Optional[str]:
    """Accepts numeric 64-bit ID or a steamcommunity profile URL; returns steamid64 or None."""
    s = val.strip()
    if s.isdigit():
        return s
    u = urlparse(s)
    if u.scheme and "steamcommunity.com" in (u.netloc or ""):
        m = re.match(r"^/(id|profiles)/([^/]+)/?", u.path or "")
        if not m:
            return None
        kind, ident = m.group(1), m.group(2)
        if kind == "profiles":
            return ident if ident.isdigit() else None
        # vanity needs resolving via XML
        try:
            r = requests.get(f"https://steamcommunity.com/id/{ident}/?xml=1", headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            sid64 = root.findtext(".//steamID64")
            return sid64
        except Exception:
            return None
    return None

# ---------- Fetch libraries (XML first, HTML fallback) ----------
def build_profile_urls(steamid64: str):
    base = f"/profiles/{steamid64}/games"
    xml_url = urlunparse(("https", "steamcommunity.com", base, "", "tab=all&xml=1", ""))
    html_url = urlunparse(("https", "steamcommunity.com", base + "/", "", "tab=all", ""))
    return xml_url, html_url

def fetch_games_xml(session: requests.Session, xml_url: str):
    r = session.get(xml_url, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    nodes = (
        root.findall(".//gamesList/games/game")
        or root.findall(".//games/game")
        or root.findall(".//game")
    )
    games = []
    for g in nodes:
        def txt(tag):
            node = g.find(tag)
            return node.text.strip() if node is not None and node.text else ""
        appid = txt("appID") or txt("appId") or txt("appId64")
        name = txt("name")
        hours_on_record = parse_hours(txt("hoursOnRecord"))
        if appid and name:
            games.append({"appid": str(appid), "name": name, "hours_on_record": hours_on_record})
    return games

def fetch_games_html_rg(session: requests.Session, html_url: str):
    r = session.get(html_url, timeout=30)
    r.raise_for_status()
    text = r.text
    m = RG_GAMES_REGEX.search(text)
    if not m:
        # looser match
        m2 = re.search(r"rgGames\s*=\s*(\[[^\]]*\"appid\"[^\]]*\])\s*;", text, re.DOTALL)
        if not m2:
            return []
        blob = m2.group(1)
    else:
        blob = m.group(1)
    try:
        arr = json.loads(blob)
    except json.JSONDecodeError:
        blob = re.sub(r",(\s*])", r"\1", blob)  # strip trailing commas
        arr = json.loads(blob)
    games = []
    for g in arr:
        appid = g.get("appid") or g.get("appID")
        name = g.get("name") or g.get("friendly_name")
        # hours_forever is sometimes a string number; sometimes missing
        hours = parse_hours(g.get("hours_forever"))
        if appid and name:
            games.append({"appid": str(appid), "name": name, "hours_on_record": hours})
    return games

def get_owned_games_for_steamid(session: requests.Session, steamid64: str):
    xml_url, html_url = build_profile_urls(steamid64)
    try:
        games = fetch_games_xml(session, xml_url)
    except Exception:
        games = []
    if not games:
        try:
            games = fetch_games_html_rg(session, html_url)
        except Exception:
            games = []
    return games

def parse_hours(val) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", "")
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0

# ---------- Enrichment (reviews + install size) ----------
def fetch_review_summary(session: requests.Session, appid: str, day_range: int = 30):
    """
    Returns dict:
      review_summary: str ("Mostly Positive" etc) OR "No reviews"
      recent_percent_positive: float | None
    Preference order: recent -> overall -> No reviews
    """
    # 1) Recent
    try:
        r = session.get(REVIEWS_RECENT.format(appid=appid, day_range=day_range), timeout=20)
        r.raise_for_status()
        q = (r.json() or {}).get("query_summary", {}) or {}
        total = int(q.get("total_reviews") or 0)
        if total > 0:
            pos = int(q.get("total_positive") or 0)
            pct = round((pos / total) * 100.0, 2)
            return {
                "review_summary": q.get("review_score_desc") or "Recent reviews",
                "recent_percent_positive": pct
            }
    except Exception:
        pass
    # 2) Overall
    try:
        r = session.get(REVIEWS_OVERALL.format(appid=appid), timeout=20)
        r.raise_for_status()
        q = (r.json() or {}).get("query_summary", {}) or {}
        total = int(q.get("total_reviews") or 0)
        if total > 0:
            pos = int(q.get("total_positive") or 0)
            pct = round((pos / total) * 100.0, 2)
            return {
                "review_summary": q.get("review_score_desc") or "Overall reviews",
                "recent_percent_positive": pct   # store overall % if recent absent
            }
    except Exception:
        pass
    # 3) None
    return {"review_summary": "No reviews", "recent_percent_positive": None}

def parse_storage_requirement_gb_from_html(html: str) -> Optional[float]:
    """
    Look for lines like 'Storage: 25 GB available space' or 'Disk Space: 10 GB'
    Return the largest number found (min/recommended often differ).
    """
    if not html:
        return None
    text = re.sub("<[^>]+>", " ", html)  # strip tags
    # common patterns
    pats = [
        r"Storage[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
        r"(?:Disk|Hard)\s*Space[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
        r"free\s+space[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
    ]
    vals = []
    for p in pats:
        for num, unit in re.findall(p, text, flags=re.IGNORECASE):
            v = float(num)
            u = unit.upper()
            if u == "MB":
                v = v / 1024.0
            elif u in ("GIB", "GB"):
                v = v
            elif u == "TB":
                v = v * 1024.0
            vals.append(v)
    if not vals:
        return None
    return round(max(vals), 2)

def fetch_install_size_gb(session: requests.Session, appid: str) -> Optional[float]:
    """
    Try appdetails pc_requirements first; if absent, scrape store page HTML.
    """
    # 1) appdetails
    try:
        r = session.get(APPDETAILS_PC_REQ.format(appid=appid), timeout=25)
        r.raise_for_status()
        data = r.json() or {}
        entry = data.get(str(appid), {})
        if entry.get("success"):
            d = entry.get("data", {}) or {}
            pc = d.get("pc_requirements") or {}
            mn = pc.get("minimum") or ""
            rc = pc.get("recommended") or ""
            val = parse_storage_requirement_gb_from_html(mn) or parse_storage_requirement_gb_from_html(rc)
            if val:
                return val
    except Exception:
        pass
    # 2) store page HTML fallback
    try:
        r = session.get(STORE_PAGE.format(appid=appid), timeout=25)
        r.raise_for_status()
        html = r.text
        # Narrow to the sys reqs block if possible
        m = re.search(r"(<div[^>]+id=\\?\"game_area_sys_req\\?\".*?</div>)", html, re.DOTALL | re.IGNORECASE)
        section = m.group(1) if m else html
        val = parse_storage_requirement_gb_from_html(section)
        if val:
            return val
    except Exception:
        pass
    return None

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Combine Steam libraries from 'Username: SteamID' lines; output unique games with owners, combined playtime, review summary, and approx install size."
    )
    ap.add_argument("--ids-file", required=True, help="Path to text file: 'Username: SteamID64 or profile URL' per line.")
    ap.add_argument("-o", "--output", default=None, help="Output CSV path (default: steam_family_combined_<timestamp>.csv)")
    ap.add_argument("--workers", type=int, default=16, help="Concurrent workers for enrichment (default: 16)")
    ap.add_argument("--day-range", type=int, default=30, help="Days for 'recent' review window (default: 30)")
    ap.add_argument("--debug", action="store_true", help="Verbose console output")
    args = ap.parse_args()

    session = http_session()

    # 1) Read & normalize IDs
    entries = parse_ids_file(args.ids_file)
    if not entries:
        print("Error: no valid lines in ids file. Expected 'Username: SteamID64'", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(entries)} account(s) in {args.ids_file}\n")

    # 2) Pull each library; track successes
    combined: dict[str, dict] = {}  # appid -> {"name": str, "owners": set[str], "hours": float}
    ok_count = 0
    fail = []

    for e in entries:
        label, sid = e["label"], e["steamid64"]
        try:
            games = get_owned_games_for_steamid(session, sid)
            if not games:
                raise RuntimeError("No games visible (privacy?)")
            # aggregate
            for g in games:
                appid = g["appid"]
                name = g["name"]
                hrs = float(g.get("hours_on_record") or 0.0)
                if appid not in combined:
                    combined[appid] = {"name": name, "owners": set(), "hours": 0.0}
                combined[appid]["owners"].add(sid)
                combined[appid]["hours"] += hrs
            ok_count += 1
            print(f"✅ {label} ({sid}): {len(games)} games")
        except Exception as ex:
            fail.append(f"{label} ({sid}) — {ex}")
            print(f"❌ {label} ({sid}): {ex}")

    if not combined:
        print("\nError: no games found across provided accounts.", file=sys.stderr)
        sys.exit(1)

    print(f"\nAggregated libraries: {ok_count}/{len(entries)} succeeded.")
    if fail:
        print("Failed:")
        for line in fail:
            print(f"  - {line}")

    # 3) Enrich unique apps with review summary + install size
    appids = list(combined.keys())
    if args.debug:
        print(f"\nEnriching {len(appids)} unique apps…")

    def enrich_one(appid: str):
        review = fetch_review_summary(session, appid, args.day_range)
        size = fetch_install_size_gb(session, appid)
        return appid, review, size

    enrichments: dict[str, dict] = {}
    sizes: dict[str, Optional[float]] = {}

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(enrich_one, appid): appid for appid in appids}
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                a, review, size = fut.result()
                enrichments[appid] = review
                sizes[appid] = size
            except Exception as e:
                if args.debug:
                    print(f"[debug] enrichment failed for {appid}: {e}")
                enrichments[appid] = {"review_summary": "No reviews", "recent_percent_positive": None}
                sizes[appid] = None

    # 4) Build rows
    rows = []
    for appid, info in combined.items():
        rows.append({
            "appid": appid,
            "name": info["name"],
            "owners": len(info["owners"]),
            "combined_hours_on_record": round(info["hours"], 2),
            "review_summary": enrichments.get(appid, {}).get("review_summary"),
            "recent_percent_positive": enrichments.get(appid, {}).get("recent_percent_positive"),
            "approx_install_size_gb": sizes.get(appid),
        })

    rows.sort(key=lambda r: (-r["owners"], r["name"].lower()))

    # 5) Write CSV
    out_name = args.output or f"steam_family_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(out_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["appid", "name", "owners", "combined_hours_on_record", "review_summary", "recent_percent_positive", "approx_install_size_gb"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nExported {len(rows)} unique games to {Path(out_name).resolve()}")
    print("Columns: appid, name, owners, combined_hours_on_record, review_summary, recent_percent_positive, approx_install_size_gb")

if __name__ == "__main__":
    main()
