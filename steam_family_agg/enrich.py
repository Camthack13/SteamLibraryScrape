import re
from typing import Optional, Dict, Tuple
from datetime import datetime
from .utils import strip_html, mb_gb, tb_gb, http_get_store, http_get_api

# Reviews JSON endpoints
REVIEWS_RECENT = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&filter=recent&day_range={day_range}&num_per_page=0"
REVIEWS_OVERALL = "https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&num_per_page=0"

# Store details (type + release_date + pc_requirements)
APPDETAILS_META = "https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=en&filters=type,release_date,pc_requirements"
STORE_PAGE = "https://store.steampowered.com/app/{appid}/?l=english&cc=US"

# News (no API key) to infer "last update year"
GET_NEWS_V2 = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid={appid}&count={count}"

_TYPE_DLC_HINTS = re.compile(r"(Downloadable\s+Content|Requires\s+the\s+base\s+game|DLC\b)", re.IGNORECASE)
_TYPE_SOFTWARE_HINTS = re.compile(r"\bSoftware\b", re.IGNORECASE)
_TYPE_TOOL_HINTS = re.compile(r"\bTool\b", re.IGNORECASE)
_TYPE_VIDEO_HINTS = re.compile(r"\bVideo\b|\bMovie\b", re.IGNORECASE)
_RELEASE_BLOCK = re.compile(r'<div[^>]*class="release_date"[^>]*>.*?<div[^>]*class="date"[^>]*>\s*([^<]+)\s*<', re.IGNORECASE|re.DOTALL)

_DELISTED_HINTS = re.compile(
    r"(is\s+no\s+longer\s+available\s+on\s+the\s+Steam\s+store"
    r"|no\s+longer\s+available\s+for\s+purchase"
    r"|at\s+the\s+request\s+of\s+the\s+publisher.*no\s+longer\s+available"
    r"|this\s+item\s+is\s+currently\s+unavailable\s+on\s+Steam)",
    re.IGNORECASE
)



def fetch_review_summary(session, appid: str, day_range: int, logger) -> Dict[str, Optional[str]]:
    """Prefer recent review summary, else overall, else 'No reviews' + None percent."""
    # Recent
    try:
        r = http_get_store(session, REVIEWS_RECENT.format(appid=appid, day_range=day_range), timeout=20)
        r.raise_for_status()
        q = (r.json() or {}).get("query_summary", {}) or {}
        total = int(q.get("total_reviews") or 0)
        if total > 0:
            pos = int(q.get("total_positive") or 0)
            pct = round((pos / total) * 100.0, 2)
            return {"review_summary": q.get("review_score_desc") or "Recent reviews", "recent_percent_positive": pct}
    except Exception as e:
        logger.debug(f"Recent reviews failed for {appid}: {e}")

    # Overall
    try:
        r = http_get_store(session, REVIEWS_OVERALL.format(appid=appid), timeout=20)
        r.raise_for_status()
        q = (r.json() or {}).get("query_summary", {}) or {}
        total = int(q.get("total_reviews") or 0)
        if total > 0:
            pos = int(q.get("total_positive") or 0)
            pct = round((pos / total) * 100.0, 2)
            return {"review_summary": q.get("review_score_desc") or "Overall reviews", "recent_percent_positive": pct}
    except Exception as e:
        logger.debug(f"Overall reviews failed for {appid}: {e}")

    return {"review_summary": "No reviews", "recent_percent_positive": None}

def parse_storage_requirement_gb_from_html(html: str) -> Optional[float]:
    """Parse sys-reqs text for disk/Storage space; return the largest value found."""
    text = strip_html(html)
    patterns = [
        r"Storage[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
        r"(?:Disk|Hard)\s*Space[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
        r"free\s+space[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|GiB|MB)",
    ]
    vals = []
    for pat in patterns:
        for num, unit in re.findall(pat, text, flags=re.IGNORECASE):
            v = float(num)
            u = unit.upper()
            if u == "MB":
                v = mb_gb(v)
            elif u in ("GB", "GIB"):
                v = round(v, 2)
            elif u == "TB":
                v = tb_gb(v)
            vals.append(v)
    return max(vals) if vals else None

def _extract_year(date_str: Optional[str]) -> Optional[int]:
    """Extract a plausible 4-digit year from Valve's release_date.date strings."""
    if not date_str:
        return None
    m = re.search(r"(\d{4})", date_str)
    if not m:
        return None
    y = int(m.group(1))
    return y if 1970 <= y <= 2100 else None

def fetch_app_meta(session, appid: str, logger,
                   need_size_fallback: bool = True,
                   check_availability: bool = True) -> Dict[str, Optional[object]]:
    # returns: {"app_type": str|None, "release_year": int|None, "approx_install_size_gb": float|None, "delisted": bool|None}

    """
    Return {"app_type": str|None, "release_year": int|None, "approx_install_size_gb": float|None}
    Strategy:
      1) Try appdetails (type + release + pc_requirements).
      2) If blocked (e.g., 403), fetch store HTML and infer type/release; also try size from sys-reqs.
    """
    app_type = None
    release_year = None
    size_gb = None

    # 1) appdetails
    try:
        r = http_get_store(session, APPDETAILS_META.format(appid=appid), timeout=25)
        r.raise_for_status()
        data = r.json() or {}
        entry = data.get(str(appid), {})
        if entry.get("success"):
            d = entry.get("data", {}) or {}
            app_type = (d.get("type") or "").lower() or None

            rd = d.get("release_date") or {}
            if not rd.get("coming_soon"):
                release_year = _extract_year(rd.get("date"))

            pc = d.get("pc_requirements") or {}
            mn = pc.get("minimum") or ""
            rc = pc.get("recommended") or ""
            size_gb = parse_storage_requirement_gb_from_html(mn) or parse_storage_requirement_gb_from_html(rc)
    except Exception as e:
        logger.debug(f"appdetails meta failed for {appid}: {e}")

    delisted = None
    # ...
    if (app_type is None or release_year is None or (size_gb is None and need_size_fallback) or check_availability):
        try:
            r2 = http_get_store(session, STORE_PAGE.format(appid=appid), timeout=25)
            r2.raise_for_status()
            html = r2.text

            if app_type is None:
                app_type = _type_from_html(html)

            if release_year is None:
                m = _RELEASE_BLOCK.search(html)
                if m:
                    release_year = _extract_year(m.group(1))

            if size_gb is None and need_size_fallback:
                m2 = re.search(r'(<div[^>]+id=\\?\"game_area_sys_req\\?\".*?</div>)', html, re.DOTALL | re.IGNORECASE)
                section = m2.group(1) if m2 else html
                size_gb = parse_storage_requirement_gb_from_html(section)

            if delisted is None:
                        delisted = bool(_DELISTED_HINTS.search(html))
        except Exception as e:
            logger.debug(f"store page meta fallback failed for {appid}: {e}")

    return {
        "app_type": app_type,
        "release_year": release_year,
        "approx_install_size_gb": (round(size_gb, 2) if size_gb else None),
        "delisted": delisted,
    }


# ---------------- Last update year ----------------

_UPDATE_KEYWORDS = re.compile(r"\b(update|patch|hotfix|changelog|balance|bug\s*fix|release\s*notes)\b", re.IGNORECASE)

def fetch_last_update_year(session, appid: str, logger, count: int = 60) -> Optional[int]:
    """
    Heuristic: most recent patch/update-like news item, else most recent news.
    Returns year or None.
    """
    try:
        r = http_get_api(session, GET_NEWS_V2.format(appid=appid, count=count), timeout=20)
        r.raise_for_status()
        j = r.json() or {}
        items = ((j.get("appnews") or {}).get("newsitems") or [])
        if not items:
            return None

        best_ts = None
        fallback_ts = None
        for it in items:
            ts = int(it.get("date") or 0) or 0
            title = (it.get("title") or "")
            contents = (it.get("contents") or "")
            tags = it.get("tags") or []
            if ts > 0 and (_UPDATE_KEYWORDS.search(title) or _UPDATE_KEYWORDS.search(contents) or ("patchnotes" in [t.lower() for t in tags])):
                if (best_ts is None) or (ts > best_ts):
                    best_ts = ts
            if ts > 0 and ((fallback_ts is None) or (ts > fallback_ts)):
                fallback_ts = ts

        final_ts = best_ts if best_ts is not None else fallback_ts
        if not final_ts:
            return None
        return datetime.utcfromtimestamp(final_ts).year
    except Exception as e:
        logger.debug(f"GetNews last-update fetch failed for {appid}: {e}")
        return None

def _type_from_html(html: str) -> Optional[str]:
    # Heuristics; conservative: only mark as non-game if we have strong hints
    if _TYPE_DLC_HINTS.search(html):
        return "dlc"
    if _TYPE_SOFTWARE_HINTS.search(html):
        return "software"
    if _TYPE_TOOL_HINTS.search(html):
        return "tool"
    if _TYPE_VIDEO_HINTS.search(html):
        return "video"
    return "game"  # default if we can load the page