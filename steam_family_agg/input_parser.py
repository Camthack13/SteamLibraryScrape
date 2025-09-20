from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import requests
import re
from typing import Optional, List, Dict
from .utils import UA

def parse_ids_file(path: str, logger) -> List[Dict[str, str]]:
    """
    Expects lines like: Username: 7656119...  OR Username: https://steamcommunity.com/(profiles|id)/...
    Ignores blank lines and lines starting with '#'.
    Returns: [{"label": "...", "steamid64": "..."}], de-duped by steamid64 (first label kept).
    """
    out = []
    seen = set()
    for i, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            logger.warning(f"Line {i} missing ':' — skipping: {raw!r}")
            continue
        label, ident = [p.strip() for p in line.split(":", 1)]
        if not label or not ident:
            logger.warning(f"Line {i} not 'Label: Value' — skipping: {raw!r}")
            continue
        sid64 = normalize_to_steamid64(ident, logger)
        if not sid64:
            logger.warning(f"Line {i} could not resolve SteamID — skipping: {raw!r}")
            continue
        if sid64 in seen:
            continue
        seen.add(sid64)
        out.append({"label": label, "steamid64": sid64})
    return out

def normalize_to_steamid64(val: str, logger) -> Optional[str]:
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
        # vanity -> resolve via profile XML
        try:
            resp = requests.get(f"https://steamcommunity.com/id/{ident}/?xml=1",
                                headers={"User-Agent": UA}, timeout=20)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            sid64 = root.findtext(".//steamID64")
            return sid64
        except Exception as e:
            logger.debug(f"Vanity resolve failed for {ident}: {e}")
            return None
    return None
