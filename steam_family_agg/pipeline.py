import csv, time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import Config, Timings, Coverage, Results
from .utils import http_session, set_throttle_config
from .utils import strip_html, mb_gb, tb_gb, http_get_store, http_get_api
from .utils import parse_hours, http_get_community

from .input_parser import parse_ids_file
from .steam_fetch import get_owned_games_for_steamid
from .enrich import fetch_app_meta, fetch_review_summary, fetch_last_update_year

REMINDER_CMD = "python -m steam_family_agg.main"
#NON_GAME_TYPES = {"dlc","tool","application","software","demo","video","advertising","hardware","mod"}
CSV_COLUMNS = [
    "appid","name","owners","combined_hours_on_record",
    "review_summary","recent_percent_positive",
    "release_year","last_update_year","approx_install_size_gb",
]

def run_pipeline(cfg: Config, logger) -> Results:
    timings = Timings()
    session = http_session()

    # NEW: apply chosen throttle
    set_throttle_config(cfg.min_delay_store, cfg.min_delay_community, cfg.min_delay_api, cfg.jitter_ms)

    # --- Parse IDs ---
    logger.info(f"Reading accounts from: {cfg.ids_path}")
    entries = parse_ids_file(str(cfg.ids_path), logger)
    if not entries:
        raise SystemExit(
            "No valid lines found. Expected 'Username: SteamID64' or a profile URL.\n"
            f"Create ./ids.txt then run:\n  {REMINDER_CMD}"
        )

    total_t0 = time.perf_counter()

    # --- Stage 1: Fetch libraries ---
    logger.info(f"Found {len(entries)} unique account(s). Fetching libraries…\n")
    t0 = time.perf_counter()

    combined = {}       # appid -> {"name": str, "owners": set[str], "hours": float}
    ok_count = 0
    failed = []

    for e in entries:
        label, sid = e["label"], e["steamid64"]
        a0 = time.perf_counter()
        logger.info(f"[{label}] Fetching library for SteamID {sid} …")
        games = get_owned_games_for_steamid(session, sid, logger)
        a1 = time.perf_counter()
        logger.info(f"[{label}] {(a1 - a0):.2f}s")

        if not games:
            msg = "No games visible (check Game details privacy = Public)"
            failed.append(f"{label} ({sid}) — {msg}")
            logger.warning(f"[{label}] {msg}")
            continue

        for g in games:
            appid = g["appid"]; name = g["name"]; hrs = float(g.get("hours_on_record") or 0.0)
            if appid not in combined:
                combined[appid] = {"name": name, "owners": set(), "hours": 0.0}
            combined[appid]["owners"].add(sid)
            combined[appid]["hours"] += hrs

        ok_count += 1
        logger.info(f"[{label}] ✓ {len(games)} items")

    timings.fetch_libraries = time.perf_counter() - t0
    if not combined:
        raise SystemExit("No games found across provided accounts.")

    # --- Stage 2: Metadata (type + release + size) & filter ---
    appids = list(combined.keys())
    total_items = len(appids)
    logger.info(f"\nChecking app metadata for {total_items} items")

    t0 = time.perf_counter()
    need_size_fallback = cfg.include_release_size

    metas = {}
    targets = []

    def meta_one(appid: str):
        return appid, fetch_app_meta(session, appid, logger,
                             need_size_fallback=need_size_fallback,
                             check_availability=True)

    with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as ex:
        futs = {ex.submit(meta_one, appid): appid for appid in appids}
        done = 0
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                a, meta = fut.result()
            except Exception:
                meta = {"app_type": None, "release_year": None, "approx_install_size_gb": None}
            metas[appid] = meta


            targets.append(appid)
            done += 1
            if done % 50 == 0 or done == len(futs):
                logger.info(f"  …meta progress: {done}/{len(futs)}")

    timings.meta = time.perf_counter() - t0

    # --- Stage 3: Reviews + Last update (only for games) ---
    logger.info(f"\nEnriching {len(targets)} games (reviews + last update)…")

    reviews_secs = 0.0
    last_update_secs = 0.0
    enrichments = {}
    last_update_years = {}

    t0 = time.perf_counter()

    def enrich_one(appid: str):
        nonlocal reviews_secs, last_update_secs
        review = {"review_summary": "No reviews", "recent_percent_positive": None}
        last_up = None
        if cfg.include_reviews:
            s0 = time.perf_counter()
            review = fetch_review_summary(session, appid, cfg.day_range, logger)
            reviews_secs += (time.perf_counter() - s0)
        if cfg.include_last_update:
            s0 = time.perf_counter()
            last_up = fetch_last_update_year(session, appid, logger)
            last_update_secs += (time.perf_counter() - s0)
        return appid, review, last_up

    with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as ex:
        futs = {ex.submit(enrich_one, appid): appid for appid in targets}
        done = 0
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                a, review, last_up = fut.result()
            except Exception:
                review, last_up = {"review_summary": "No reviews", "recent_percent_positive": None}, None
            enrichments[appid] = review
            last_update_years[appid] = last_up
            done += 1
            if done % 50 == 0 or done == len(futs):
                logger.info(f"  …enrichment progress: {done}/{len(futs)}")

    timings.enrichment = time.perf_counter() - t0
    timings.reviews = reviews_secs
    timings.last_update = last_update_secs

    # --- Build rows & write CSV ---
    rows = []
    for appid in targets:
        meta = metas.get(appid, {})
        if meta.get("delisted") is True:
            excluded_delisted += 1
            continue
        info = combined[appid]
        rows.append({
            "appid": appid,
            "name": info["name"],
            "owners": len(info["owners"]),
            "combined_hours_on_record": round(info["hours"], 2),
            "review_summary": enrichments.get(appid, {}).get("review_summary") if cfg.include_reviews else None,
            "recent_percent_positive": enrichments.get(appid, {}).get("recent_percent_positive") if cfg.include_reviews else None,
            "release_year": metas.get(appid, {}).get("release_year") if cfg.include_release_size else None,
            "last_update_year": last_update_years.get(appid) if cfg.include_last_update else None,
            "approx_install_size_gb": metas.get(appid, {}).get("approx_install_size_gb") if cfg.include_release_size else None,
        })

    logger.info(f"Excluded delisted (no longer available): {excluded_delisted}")

    rows.sort(key=lambda r: (-r["owners"], r["name"].lower()))
    out_name = f"steam_family_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path = Path(out_name)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    # --- Coverage (not-blank for every column) ---
    def not_blank(v):
        if v is None: return False
        if isinstance(v, str) and not v.strip(): return False
        return True

    nb_counts = {c: 0 for c in CSV_COLUMNS}
    for row in rows:
        for c in CSV_COLUMNS:
            if not_blank(row.get(c)): nb_counts[c] += 1
    total_rows = len(rows)
    nb_perc = {c: (nb_counts[c] / total_rows * 100.0 if total_rows else 0.0) for c in CSV_COLUMNS}
    coverage = Coverage(counts=nb_counts, perc=nb_perc, total_rows=total_rows)

    timings.total = time.perf_counter() - total_t0

    return Results(
        rows=rows,
        out_path=out_path,
        timings=timings,
        coverage=coverage,
        included_games=len(targets),
        failed_accounts=failed,
        ok_accounts=ok_count,
        total_accounts=len(entries),
    )
