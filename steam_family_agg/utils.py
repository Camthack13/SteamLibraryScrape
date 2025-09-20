import logging, random, threading, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def setup_logger(verbose: bool=False, debug: bool=False) -> logging.Logger:
    level = logging.WARNING
    if verbose: level = logging.INFO
    if debug:   level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S"
    )
    return logging.getLogger("steam_family_agg")

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://store.steampowered.com/",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    # Age gate cookies (bypass 18+)
    cj = requests.cookies.RequestsCookieJar()
    cj.set("birthtime", "0", domain="store.steampowered.com", path="/")
    cj.set("lastagecheckage", "1-January-1970", domain="store.steampowered.com", path="/")
    cj.set("mature_content", "1", domain="store.steampowered.com", path="/")
    cj.set("wants_mature_content", "1", domain="store.steampowered.com", path="/")
    s.cookies.update(cj)

    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=(500, 502, 503, 504),
                  allowed_methods=["GET","HEAD"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# ---------- Global throttling (shared across threads) ----------
class _HostThrottle:
    def __init__(self):
        self.lock = threading.Lock()
        self.last = {"store": 0.0, "community": 0.0, "api": 0.0}
        self.min_delay = {"store": 0.6, "community": 0.25, "api": 0.2}
        self.jitter_ms = 120

    def configure(self, store: float, community: float, api: float, jitter_ms: int):
        with self.lock:
            self.min_delay.update({"store": store, "community": community, "api": api})
            self.jitter_ms = jitter_ms

    def wait(self, kind: str):
        now = time.monotonic()
        with self.lock:
            last = self.last.get(kind, 0.0)
            need = self.min_delay.get(kind, 0.0) - (now - last)
            if need > 0:
                jitter = random.uniform(0, self.jitter_ms/1000.0)
                sleep_for = need + jitter
            else:
                sleep_for = 0.0
            # set next allowed time conservatively
            self.last[kind] = time.monotonic() + (sleep_for if sleep_for>0 else 0)
        if sleep_for > 0:
            time.sleep(sleep_for)

_THROTTLE = _HostThrottle()

def set_throttle_config(store: float, community: float, api: float, jitter_ms: int):
    _THROTTLE.configure(store, community, api, jitter_ms)

def http_get_store(session: requests.Session, url: str, **kwargs):
    _THROTTLE.wait("store")
    return session.get(url, **kwargs)

def http_get_community(session: requests.Session, url: str, **kwargs):
    _THROTTLE.wait("community")
    return session.get(url, **kwargs)

def http_get_api(session: requests.Session, url: str, **kwargs):
    _THROTTLE.wait("api")
    return session.get(url, **kwargs)

# existing helpers you already had:
import re
from typing import Optional
def parse_hours(val) -> float:
    if val is None: return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip().replace(",", "")
    try: return float(s) if s else 0.0
    except ValueError: return 0.0

def strip_html(html: str) -> str:
    return re.sub("<[^>]+>", " ", html or "")

def mb_gb(value: float) -> float: return round(value / 1024.0, 2)
def tb_gb(value: float) -> float: return round(value * 1024.0, 2)
