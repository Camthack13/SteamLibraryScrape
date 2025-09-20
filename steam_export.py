#!/usr/bin/env python3
import argparse
import csv
import sys
import re
from urllib.parse import urlparse, urlunparse
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

def build_xml_url(profile_url: str) -> str:
    """
    Accepts:
      https://steamcommunity.com/id/<vanity>
      https://steamcommunity.com/profiles/<steamid64>
      (with or without trailing slash, any extra path)

    Returns:
      https://steamcommunity.com/(id|profiles)/<...>/games?tab=all&xml=1
    """
    u = urlparse(profile_url.strip())
    if not u.netloc:
        raise ValueError("Please include a full URL like https://steamcommunity.com/id/yourname")
    if "steamcommunity.com" not in u.netloc:
        raise ValueError("URL must be a steamcommunity.com profile URL.")

    # Normalize path: keep either /id/<x> or /profiles/<x>
    m = re.match(r"^/(id|profiles)/([^/]+)/?", u.path or "")
    if not m:
        raise ValueError("Profile URL should look like https://steamcommunity.com/id/<name> or /profiles/<steamid64>")

    base_path = f"/{m.group(1)}/{m.group(2)}/games"
    query = "tab=all&xml=1"
    return urlunparse(("https", "steamcommunity.com", base_path, "", query, ""))

def fetch_games_xml(xml_url: str) -> ET.Element:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SteamLibraryExport/1.0)"
    }
    r = requests.get(xml_url, headers=headers, timeout=30)
    r.raise_for_status()
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse XML: {e}") from e
    return root

def parse_games(root: ET.Element):
    # Expected structure: <gamesList><games> <game>...</game> ... </games></gamesList>
    games = []
    games_nodes = root.findall(".//gamesList/games/game")
    if not games_nodes:
        # Some profiles return <privacyState> or messages if private
        privacy = root.findtext(".//privacyState")
        if privacy and privacy.lower() != "public":
            raise RuntimeError("Profile or game details are not public. Make sure 'Game details' are set to Public in Steam privacy settings.")
        # Or simply no games found
        raise RuntimeError("No games found in XML. Is the profile correct and games list visible?")
    for g in games_nodes:
        def txt(tag): 
            node = g.find(tag)
            return node.text.strip() if node is not None and node.text else ""
        # Common tags seen in Steam XML
        appid = txt("appID") or txt("appId")  # handle possible capitalization variants
        name = txt("name")
        hours_on_record = txt("hoursOnRecord")
        hours_last2 = txt("hoursLast2Weeks")
        store_link = txt("storeLink")
        logo = txt("logo")
        # Normalize hours to floats when possible
        def parse_hours(s):
            # XML often has strings like "12.3" or "0.1"; sometimes "0.0"
            try:
                # Remove commas if any locale formatting appears
                return float(s.replace(",", "")) if s else 0.0
            except ValueError:
                return 0.0
        games.append({
            "appid": appid,
            "name": name,
            "hours_on_record": parse_hours(hours_on_record),
            "hours_last_2_weeks": parse_hours(hours_last2),
            "store_link": store_link,
            "logo": logo
        })
    return games

def default_output_name(profile_url: str) -> str:
    # Use vanity or steamid from the URL in the filename
    m = re.search(r"/(id|profiles)/([^/]+)/?", profile_url)
    who = m.group(2) if m else "steam_profile"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"steam_library_{who}_{timestamp}.csv"

def write_csv(rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["appid", "name", "hours_on_record", "hours_last_2_weeks", "store_link", "logo"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def main():
    ap = argparse.ArgumentParser(description="Export Steam library to CSV (no API key required).")
    ap.add_argument("profile_url", help="Your Steam profile URL (e.g., https://steamcommunity.com/id/yourname or /profiles/7656119...)")
    ap.add_argument("-o", "--output", help="Output CSV path (default: steam_library_<name>_<timestamp>.csv)")
    args = ap.parse_args()

    try:
        xml_url = build_xml_url(args.profile_url)
        root = fetch_games_xml(xml_url)
        games = parse_games(root)
        out_name = args.output or default_output_name(args.profile_url)
        out_path = Path(out_name)
        write_csv(games, out_path)
        print(f"Exported {len(games)} games to {out_path.resolve()}")
    except (ValueError, RuntimeError, requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
