from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

@dataclass
class Config:
    ids_path: Path
    verbose: bool
    debug: bool
    workers: int
    day_range: int
    include_reviews: bool
    include_release_size: bool
    include_last_update: bool

    # NEW: gentler-by-default execution
    safe_mode: bool = True
    min_delay_store: float = 0.6      # seconds between store.steampowered.com requests (global)
    min_delay_community: float = 0.25 # seconds between steamcommunity.com requests
    min_delay_api: float = 0.20       # seconds between api.steampowered.com requests
    jitter_ms: int = 120              # random jitter to avoid burstiness

@dataclass
class Timings:
    fetch_libraries: float = 0.0
    meta: float = 0.0
    enrichment: float = 0.0
    reviews: float = 0.0
    last_update: float = 0.0
    total: float = 0.0

@dataclass
class Coverage:
    counts: Dict[str, int] = field(default_factory=dict)
    perc: Dict[str, float] = field(default_factory=dict)
    total_rows: int = 0

@dataclass
class Results:
    rows: List[Dict]
    out_path: Path
    timings: Timings
    coverage: Coverage
    type_counts: Dict[str, int]
    included_games: int
    excluded_items: int
    failed_accounts: List[str]
    ok_accounts: int
    total_accounts: int
