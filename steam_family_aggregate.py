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
from urllib.parse import urlparse, urlunparse

import requests
import xml.etree.ElementTree as ET

# ---------- Config ----------
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/118 Safari/537.36"
REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&filter=recent&num_per_page=0&day_range={day_range}"
APPDETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=en&filters=pc_requirements"
RG_GAMES_REGEX = re.compile(r"var\s+rgGames\s*=\s*(\[\s*{.*?}\s*\])\s*;", re.DOTALL)

# ---------- Helpers ----------
def http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

def line_is_blank_or_comment(line: str) -> bool:
    return not line.strip() or line.strip().startswith("#")

def parse_input_line_to_identifier(line: str) -> dict:
    """
    Accepts:
      - raw 64-bit steamid (e.g., 76561198000000000)
      - https://steamcommunity.com/profiles/<id>[/...]
      - https://steamcommunity.com/id/<vanity>[/...]
    Returns dict with either {"steamid64": "..."} or {"vanity": "..."}
    """
    s = line.strip()
    u = urlparse(s)
    if u.scheme and "steamcommunity.com" in (u.netloc or ""):
        m = re.match(r"^/(id|profiles)/([^/]+)/?", u.path or "")
        if not m:
            raise ValueError(f"Unrecognized Steam profile URL: {s}")
        kind, ident = m.group(1), m.group(2)
        if kind == "profiles":
            if not ident.isdigit():
                raise ValueError(f"profiles path must be numeric: {s}")
            return {"steamid64": ident}
        else:
            return {"vanity": ident}
    # raw numeric?
    if s.isdigit():
        return {"steamid64": s}
    raise ValueError(f"Line is not a SteamID64 or profile URL: {s}")

def resolve_steamid64_from_vanity(session: requests.Session, vanity: str, debug: bool=False) -> str:
    # public profile XML contains steamID64
    url = f"https://steamcommunity.com/id/{vanity}/?xml=1"
    r = session.get(url, timeout=25)
    r.raise_for_status()
    if debug:
        print(f"[debug] resolve vanity -> xml: {url}")
    root = ET.fromstring(r.content)
    sid64 = root.findtext(".//steamID64")
    if not sid64:
        raise RuntimeError(f"Unable to resolve vanity '{vanity}' to steamID64 (is profile reachable?)")
    return sid64

def build_profile_urls_from_steamid(steamid64: str) -> tuple[str, str]:
    base_path = f"/profiles/{steamid64}/games"
    xml_url = urlunparse(("https", "steamcommunity.com", base_path, "", "tab=all&xml=1", ""))
    html_url = urlunparse(("https", "steamcommunity.com", base_path + "/", "", "tab=all", ""))
    return xml_url, html_url

def fetch_games_xml(session: requests.Session, xml_url: str, debug: bool) -> list[dict]:
    r = session.get(xml_url, timeout=30)
    r.raise_for_status()
    if debug:
        print("[debug] XML URL:", xml_url)
        print("[debug] XML head:", r.text[:500].replace("\n", " "))
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        if debug:
            print("[debug] XML parse error:", e)
        return []
    # try multiple shapes
    nodes = (
        root.findall(".//gamesList/games/game")
        or root.findall(".//games/game")
        or root.findall(".//game")
    )
    out = []
    for g in nodes:
        def txt(tag):
            node = g.find(tag)
            return node.text.strip() if node is not None and node.text else ""
        appid = txt("appID") or txt("appId") or txt("appId64")
        name = txt("name")
        if appid and name:
            out.append({"appid": str(appid), "name": name})
    return out

def fetch_games_html_rg(session: requests.Session, html_url: str, debug: bool) -> list[dict]:
    r = session.get(html_url, timeout=30)
    r.raise_for_status()
    text = r.text
    if debug:
        print("[debug] HTML URL:", html_url)
        print("[debug] HTML head:", text[:500].replace("\n", " "))
    m = RG_GAMES_REGEX.search(text)
    if not m:
        # try looser match
        m2 = re.search(r"rgGames\s*=\s*(\[[^\]]*\"appid\"[^\]]*\])\s*;", text, re.DOTALL)
        if not m2:
            return []
        blob = m2.group(1)
    else:
        blob = m.group(1)
    try:
        arr = json.loads(blob)
    except json.JSONDecodeError:
        blob2 = re.sub(r",(\s*])", r"\1", blob)
        arr = json.loads(blob2)
    out = []
    for g in arr:
        appid = g.get("appid") or g.get("appID")
        name = g.get("name") or g.get("friendly_name")
        if appid and name:
            out.append({"appid": str(appid), "name": name})
    return out

def get_owned_games_for_steamid(session: requests.Session, steamid64: str, debug: bool=False) -> list[dict]:
    xml_url, html_url = build_profile_urls_from_steamid(steamid64)
    games = fetch_games_xml(session, xml_url, debug)
    if not games:
        if debug:
            print(f"[debug] XML empty for {steamid64}; trying HTML fallback")
        games = fetch_games_html_rg(session, html_url, debug)
    return games

def parse_storage_requirement_gb(raw_html: str) -> float | None:
    if not raw_html:
        return None
    text = re.sub("<[^>]+>", " ", raw_html)  # strip tags
    # Look for "Storage: 20 GB available space" or variants; take the max number found
    matches = re.findall(r"Storage[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(GB|MB)", text, flags=re.IGNORECASE)
    if not matches:
        # Sometimes "Disk Space: X GB"
        matches = re.findall(r"(?:Disk|Hard)\s*Space[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(GB|MB)", text, flags=re.IGNORECASE)
    if not matches:
        return None
    vals_gb = []
    for num, unit in matches:
        v = float(num)
        if unit.upper() == "MB":
            v = v / 1024.0
        vals_gb.append(v)
    return round(max(vals_gb), 2) if vals_gb else None

def fetch_enrichment_for_app(session: requests.Session, appid: str, day_range: int, debug: bool=False) -> dict:
    # 1) Recent review percent positive
    rev = {
        "recent_percent_positive": None
    }
    last_exc = None
    try:
        r = session.get(REVIEWS_URL.format(appid=appid, day_range=day_range), timeout=20)
        if r.status_code in (429, 503):
            raise requests.HTTPError(f"HTTP {r.status_code}")
        r.raise_for_status()
        data = r.json()
        q = data.get("query_summary", {}) or {}
        total = q.get("total_reviews", 0) or 0
        pos = q.get("total_positive", 0) or 0
        pct = (pos / total * 100.0) if total else None
        rev["recent_percent_positive"] = round(pct, 2) if pct is not None else None
    except Exception as e:
        last_exc = e
        if debug:
            print(f"[debug] reviews failed for {appid}: {e}")

    # 2) Approx install size from store appdetails
    size = {
        "approx_install_size_gb": None
    }
    try:
        r2 = session.get(APPDETAILS_URL.format(appid=appid), timeout=25)
        r2.raise_for_status()
        data = r2.json()
        entry = data.get(str(appid), {})
        if entry.get("success"):
            d = entry.get("data", {}) or {}
            pc = d.get("pc_requirements") or {}
            # minimum/recommended are HTML blobs; parse both
            mn = pc.get("minimum") or ""
            rc = pc.get("recommended") or ""
            size_val = parse_storage_requirement_gb(mn) or parse_storage_requirement_gb(rc)
            size["approx_install_size_gb"] = size_val
    except Exception as e:
        if debug:
            print(f"[debug] appdetails failed for {appid}: {e}")

    return {**rev, **size}

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Combine Steam libraries from a list of Steam IDs / profile URLs; output unique games with owners, duplicates, recent review % and approx install size."
    )
    ap.add_argument("--ids-file", required=True, help="Path to a text file with one SteamID64 or profile URL per line. Lines starting with # are ignored.")
    ap.add_argument("-o", "--output", default=None, help="Output CSV path (default: steam_family_combined_<timestamp>.csv)")
    ap.add_argument("--workers", type=int, default=12, help="Concurrent workers for enrichment (default: 12)")
    ap.add_argument("--day-range", type=int, default=30, help="Days for 'recent' reviews window (default: 30)")
    ap.add_argument("--debug", action="store_true", help="Print debug info")
    args = ap.parse_args()

    session = http_session()

    # 1) Read IDs
    lines = Path(args.ids_file).read_text(encoding="utf-8").splitlines()
    identifiers = []
    for i, line in enumerate(lines, 1):
        if line_is_blank_or_comment(line):
            continue
        try:
            identifiers.append(parse_input_line_to_identifier(line))
        except Exception as e:
            print(f"Warning: skipping line {i}: {e}", file=sys.stderr)

    if not identifiers:
        print("Error: no valid Steam IDs or profile URLs found in the file.", file=sys.stderr)
        sys.exit(1)

    # 2) Resolve any vanity URLs
    steamids = []
    for ident in identifiers:
        if "steamid64" in ident:
            steamids.append(ident["steamid64"])
        else:
            try:
                sid = resolve_steamid64_from_vanity(session, ident["vanity"], args.debug)
                steamids.append(sid)
            except Exception as e:
                print(f"Warning: could not resolve vanity '{ident['vanity']}': {e}", file=sys.stderr)

    steamids = list(dict.fromkeys(steamids))  # de-dup preserving order
    if args.debug:
        print("[debug] steamids:", steamids)

    # 3) Pull libraries and combine
    combined: dict[str, dict] = {}  # appid -> {"name": str, "owners": set()}
    for sid in steamids:
        try:
            games = get_owned_games_for_steamid(session, sid, args.debug)
        except Exception as e:
            print(f"Warning: failed to fetch library for {sid}: {e}", file=sys.stderr)
            continue
        if args.debug:
            print(f"[debug] {sid}: {len(games)} games")
        for g in games:
            appid = g["appid"]
            name = g["name"]
            if appid not in combined:
                combined[appid] = {"name": name, "owners": set()}
            combined[appid]["owners"].add(sid)

    if not combined:
        print("Error: no games found across provided accounts. Check privacy (Game details must be Public).", file=sys.stderr)
        sys.exit(1)

    # 4) Enrich each unique app with recent % positive + approximate install size
    appids = list(combined.keys())
    enrichments: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(fetch_enrichment_for_app, session, appid, args.day_range, args.debug): appid for appid in appids}
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                enrichments[appid] = fut.result()
            except Exception as e:
                if args.debug:
                    print(f"[debug] enrichment failed for {appid}: {e}")
                enrichments[appid] = {"recent_percent_positive": None, "approx_install_size_gb": None}

    # 5) Build rows
    rows = []
    for appid, info in combined.items():
        owners_count = len(info["owners"])
        rows.append({
            "appid": appid,
            "name": info["name"],
            "owners": owners_count,
            "duplicates": max(0, owners_count - 1),
            "recent_percent_positive": enrichments.get(appid, {}).get("recent_percent_positive"),
            "approx_install_size_gb": enrichments.get(appid, {}).get("approx_install_size_gb"),
        })

    rows.sort(key=lambda r: (-(r["owners"]), r["name"].lower()))

    # 6) Write CSV
    out_name = args.output or f"steam_family_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path = Path(out_name)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["appid", "name", "owners", "duplicates", "recent_percent_positive", "approx_install_size_gb"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} unique games to {out_path.resolve()}")
    print("Columns: appid, name, owners, duplicates, recent_percent_positive, approx_install_size_gb")

if __name__ == "__main__":
    main()
