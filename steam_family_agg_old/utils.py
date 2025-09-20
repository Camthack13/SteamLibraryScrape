import logging
import re
from typing import Optional
import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/118 Safari/537.36"

def setup_logger(verbose: bool=False, debug: bool=False) -> logging.Logger:
    level = logging.WARNING
    if verbose:
        level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S"
    )
    return logging.getLogger("steam_family_agg")

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

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

def strip_html(html: str) -> str:
    return re.sub("<[^>]+>", " ", html or "")

def mb_gb(value: float) -> float:
    return round(value / 1024.0, 2)

def tb_gb(value: float) -> float:
    return round(value * 1024.0, 2)
