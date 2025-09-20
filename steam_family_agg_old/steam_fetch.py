import json
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlunparse
from typing import List, Dict
from .utils import parse_hours

RG_GAMES_REGEX = re.compile(r"var\s+rgGames\s*=\s*(\[\s*{.*?}\s*\])\s*;", re.DOTALL)

def build_profile_urls(steamid64: str):
    base = f"/profiles/{steamid64}/games"
    xml_url = urlunparse(("https", "steamcommunity.com", base, "", "tab=all&xml=1", ""))
    html_url = urlunparse(("https", "steamcommunity.com", base + "/", "", "tab=all", ""))
    return xml_url, html_url

def fetch_games_xml(session, xml_url: str, logger) -> List[Dict]:
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
    logger.debug(f"XML parsed {len(games)} games from {xml_url}")
    return games

def fetch_games_html_rg(session, html_url: str, logger) -> List[Dict]:
    r = session.get(html_url, timeout=30)
    r.raise_for_status()
    text = r.text
    m = RG_GAMES_REGEX.search(text)
    if not m:
        # looser match
        m2 = re.search(r"rgGames\s*=\s*(\[[^\]]*\"appid\"[^\]]*\])\s*;", text, re.DOTALL)
        if not m2:
            logger.debug("rgGames JSON not found in HTML")
            return []
        blob = m2.group(1)
    else:
        blob = m.group(1)
    try:
        arr = json.loads(blob)
    except json.JSONDecodeError:
        blob = re.sub(r",(\s*])", r"\1", blob)
        arr = json.loads(blob)
    games = []
    for g in arr:
        appid = g.get("appid") or g.get("appID")
        name = g.get("name") or g.get("friendly_name")
        hours = parse_hours(g.get("hours_forever"))
        if appid and name:
            games.append({"appid": str(appid), "name": name, "hours_on_record": hours})
    logger.debug(f"HTML rgGames parsed {len(games)} games from {html_url}")
    return games

def get_owned_games_for_steamid(session, steamid64: str, logger) -> List[Dict]:
    xml_url, html_url = build_profile_urls(steamid64)
    try:
        games = fetch_games_xml(session, xml_url, logger)
    except Exception as e:
        logger.debug(f"XML fetch error for {steamid64}: {e}")
        games = []
    if not games:
        try:
            games = fetch_games_html_rg(session, html_url, logger)
        except Exception as e:
            logger.debug(f"HTML fetch error for {steamid64}: {e}")
            games = []
    return games
