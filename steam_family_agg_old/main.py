#!/usr/bin/env python3
import argparse
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from .utils import setup_logger, http_session
from .input_parser import parse_ids_file
from .steam_fetch import get_owned_games_for_steamid
from .enrich import (
    fetch_review_summary,
    fetch_last_update_year,
    fetch_app_meta,  # includes type + release_year + approx_install_size_gb
)

REMINDER_CMD = "python -m steam_family_agg.main --ids-file ids.txt --workers 16 --day-range 30 --verbose"

NON_GAME_TYPES = {"dlc", "tool", "application", "software", "demo", "video", "advertising", "hardware", "mod"}

def main():
    ap = argparse.ArgumentParser(
        description=(
            "Combine Steam libraries from 'Username: SteamID/URL' lines; export unique games with owners, "
            "combined playtime, review summary, release year, last update year, and approx install size. "
            "Excludes DLC/tools/other non-games; includes timing & coverage metrics."
        )
    )
    ap.add_argument("--ids-file", default=None,
                    help="Path to text file; if omitted, tries ./ids.txt automatically.")
    ap.add_argument("-o", "--output", default=None,
                    help="Output CSV (default: steam_family_combined_<timestamp>.csv)")
    ap.add_argument("--workers", type=int, default=16, help="Concurrent workers (default: 16)")
    ap.add_argument("--day-range", type=int, default=30, help="Days for 'recent' reviews window (default: 30)")
    ap.add_argument("--verbose", action="store_true", help="Show INFO logs")
    ap.add_argument("--debug", action="store_true", help="Show DEBUG logs")

    # Speed knobs
    ap.add_argument("--skip-reviews", action="store_true", help="Skip review summary/percent")
    ap.add_argument("--skip-release-size", action="store_true", help="Skip install size fallback & still fetch type+release (needed to filter)")
    ap.add_argument("--skip-last-update", action="store_true", help="Skip last update year")

    args = ap.parse_args()

    logger = setup_logger(verbose=args.verbose, debug=args.debug)
    session = http_session()

    total_t0 = time.perf_counter()

    # Resolve ids file
    used_default = False
    if args.ids_file:
        ids_path = Path(args.ids_file)
        if not ids_path.exists():
            logger.error(f"IDs file not found: {ids_path}")
            print("\nCreate a file like this (one per line):")
            print("  Omnidude7: 76561198064184537")
            print("  LarkShark: 76561198045064545\n")
            print("Then run the latest version with:")
            print(f"  {REMINDER_CMD}")
            raise SystemExit(2)
    else:
        ids_path = Path("ids.txt")
        if ids_path.exists():
            used_default = True
            logger.info("No --ids-file provided; using ./ids.txt")
            logger.info("Reminder: to run the latest version explicitly:")
            logger.info(f"  {REMINDER_CMD}")
        else:
            logger.error("No --ids-file provided and ./ids.txt was not found.")
            print("\nCreate ./ids.txt with lines like:")
            print("  Omnidude7: 76561198064184537")
            print("  LarkShark: 76561198045064545")
            print("  5tyr: https://steamcommunity.com/id/5tyr/")
            print("\nThen run the latest version with:")
            print(f"  {REMINDER_CMD}")
            raise SystemExit(2)

    logger.info(f"Reading accounts from: {ids_path}")
    entries = parse_ids_file(str(ids_path), logger)
    if not entries:
        logger.error("No valid lines found. Expected 'Username: SteamID64' or a profile URL.")
        print(f"\nRun again with:\n  {REMINDER_CMD}")
        raise SystemExit(1)

    # ---- Stage 1: Fetch libraries per account ----
    logger.info(f"Found {len(entries)} unique account(s). Fetching libraries…\n")

    fetch_t0 = time.perf_counter()

    combined = {}  # appid -> {"name": str, "owners": set[str], "hours": float}
    ok_count = 0
    failed = []

    for e in entries:
        label, sid = e["label"], e["steamid64"]
        t0 = time.perf_counter()
        logger.info(f"[{label}] Fetching library for SteamID {sid} …")
        games = get_owned_games_for_steamid(session, sid, logger)
        t1 = time.perf_counter()
        logger.info(f"[{label}] {(t1 - t0):.2f}s")

        if not games:
            msg = "No games visible (check Game details privacy = Public)"
            failed.append(f"{label} ({sid}) — {msg}")
            logger.warning(f"[{label}] {msg}")
            continue

        for g in games:
            appid = g["appid"]
            name = g["name"]
            hrs = float(g.get("hours_on_record") or 0.0)
            if appid not in combined:
                combined[appid] = {"name": name, "owners": set(), "hours": 0.0}
            combined[appid]["owners"].add(sid)
            combined[appid]["hours"] += hrs

        ok_count += 1
        logger.info(f"[{label}] ✓ {len(games)} items")

    fetch_t1 = time.perf_counter()
    fetch_total = fetch_t1 - fetch_t0

    if not combined:
        logger.error("No games found across provided accounts.")
        print(f"\nRun again with:\n  {REMINDER_CMD}")
        raise SystemExit(1)

    # ---- Stage 2: App metadata (type + release + size) -> filter non-games ----
    appids = list(combined.keys())
    total_items = len(appids)
    logger.info(f"\nChecking app metadata for {total_items} items (filtering non-games)…")

    meta_t0 = time.perf_counter()

    # controls whether to do store-page size fallback here
    need_size_fallback = not args.skip_release_size

    def meta_one(appid: str):
        return appid, fetch_app_meta(session, appid, logger, need_size_fallback=need_size_fallback)

    metas = {}
    type_counts = {}
    good_appids = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(meta_one, appid): appid for appid in appids}
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                a, meta = fut.result()
            except Exception as e:
                meta = {"app_type": None, "release_year": None, "approx_install_size_gb": None}
            metas[appid] = meta
            t = (meta.get("app_type") or "unknown").lower()
            type_counts[t] = type_counts.get(t, 0) + 1
            # keep only games
            if t == "game":
                good_appids.append(appid)
            done += 1
            if done % 50 == 0 or done == total:
                logger.info(f"  …meta progress: {done}/{total}")

    meta_t1 = time.perf_counter()
    meta_total = meta_t1 - meta_t0

    # Exclusion stats
    excluded = sum(c for typ, c in type_counts.items() if typ in NON_GAME_TYPES or typ == "unknown")
    logger.info(f"Type breakdown: {type_counts}")
    logger.info(f"Included games: {len(good_appids)} / {total_items} (excluded {excluded} non-games)")

    if not good_appids:
        logger.error("After filtering, no 'game' items remain.")
        raise SystemExit(1)

    # ---- Stage 3: Reviews + Last update (only for games) ----
    logger.info(f"\nEnriching {len(good_appids)} games (reviews + last update)…")

    do_reviews = not args.skip_reviews
    do_last_update = not args.skip_last_update

    reviews_secs = 0.0
    last_update_secs = 0.0

    enrichments = {}            # appid -> {"review_summary": str, "recent_percent_positive": float|None}
    last_update_years = {}      # appid -> int|None

    enrich_t0 = time.perf_counter()

    def enrich_one(appid: str):
        nonlocal reviews_secs, last_update_secs
        review = {"review_summary": "No reviews", "recent_percent_positive": None}
        last_up = None
        if do_reviews:
            t0 = time.perf_counter()
            review = fetch_review_summary(session, appid, args.day_range, logger)
            reviews_secs += (time.perf_counter() - t0)
        if do_last_update:
            t0 = time.perf_counter()
            last_up = fetch_last_update_year(session, appid, logger)
            last_update_secs += (time.perf_counter() - t0)
        return appid, review, last_up

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(enrich_one, appid): appid for appid in good_appids}
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            appid = futs[fut]
            try:
                a, review, last_up = fut.result()
            except Exception as e:
                review = {"review_summary": "No reviews", "recent_percent_positive": None}
                last_up = None
            enrichments[appid] = review
            last_update_years[appid] = last_up
            done += 1
            if done % 50 == 0 or done == total:
                logger.info(f"  …enrichment progress: {done}/{total}")

    enrich_t1 = time.perf_counter()
    enrich_total = enrich_t1 - enrich_t0

    # ---- Build CSV rows (only games) ----
    rows = []
    for appid in good_appids:
        info = combined[appid]
        rows.append({
            "appid": appid,
            "name": info["name"],
            "owners": len(info["owners"]),
            "combined_hours_on_record": round(info["hours"], 2),
            "review_summary": enrichments.get(appid, {}).get("review_summary"),
            "recent_percent_positive": enrichments.get(appid, {}).get("recent_percent_positive"),
            "release_year": metas.get(appid, {}).get("release_year"),
            "last_update_year": last_update_years.get(appid),
            "approx_install_size_gb": metas.get(appid, {}).get("approx_install_size_gb"),
        })

    rows.sort(key=lambda r: (-r["owners"], r["name"].lower()))

    # ---- Write CSV ----
    out_name = args.output or f"steam_family_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path = Path(out_name)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "appid",
                "name",
                "owners",
                "combined_hours_on_record",
                "review_summary",
                "recent_percent_positive",
                "release_year",
                "last_update_year",
                "approx_install_size_gb",
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    # ---- Metrics ----
    total_runtime = time.perf_counter() - total_t0

    def not_blank(v):
        if v is None:
            return False
        if isinstance(v, str) and not v.strip():
            return False
        return True

    # not-blank coverage for every CSV column
    cols = [
        "appid",
        "name",
        "owners",
        "combined_hours_on_record",
        "review_summary",
        "recent_percent_positive",
        "release_year",
        "last_update_year",
        "approx_install_size_gb",
    ]
    nb_counts = {c: 0 for c in cols}
    for row in rows:
        for c in cols:
            if not_blank(row.get(c)):
                nb_counts[c] += 1
    total_rows = len(rows)
    nb_perc = {c: (nb_counts[c] / total_rows * 100.0 if total_rows else 0.0) for c in cols}

    # timing
    logger.info(f"\nExported {total_rows} unique GAMES → {out_path.resolve()}")
    logger.info("\n=== Timing summary ===")
    logger.info(f"Fetch libraries total: {fetch_total:.2f}s")
    logger.info(f"Meta (type+release+size) total: {meta_total:.2f}s")
    logger.info(f"Enrichment total:      {enrich_total:.2f}s")
    if not args.skip_reviews:
        logger.info(f"  Reviews JSON:        {reviews_secs:.2f}s  (~{(reviews_secs/max(1,len(good_appids))):.3f}s/app)")
    else:
        logger.info("  Reviews JSON:        skipped")
    if not args.skip_last_update:
        logger.info(f"  Last Update (news):  {last_update_secs:.2f}s  (~{(last_update_secs/max(1,len(good_appids))):.3f}s/app)")
    else:
        logger.info("  Last Update (news):  skipped")
    logger.info(f"Total runtime:         {total_runtime:.2f}s")

    # coverage
    logger.info("\n=== Not-blank coverage by column (games only) ===")
    for c in cols:
        logger.info(f"{c}: {nb_counts[c]}/{total_rows} ({nb_perc[c]:.1f}%)")

    # helpful advisories
    if nb_perc["release_year"] < 50.0:
        logger.warning(f"\nOnly {nb_perc['release_year']:.1f}% of games have release_year. "
                       f"Consider --skip-release-size to speed up.")
    if nb_perc["approx_install_size_gb"] < 50.0:
        logger.warning(f"Only {nb_perc['approx_install_size_gb']:.1f}% have approx_install_size_gb. "
                       f"Consider --skip-release-size to skip disk size retrieval.")
    if nb_perc["last_update_year"] < 50.0 and not args.skip_last_update:
        logger.warning(f"Only {nb_perc['last_update_year']:.1f}% have last_update_year. "
                       f"Consider --skip-last-update if you don't need it.")

    if used_default:
        logger.info("\nTip: you can always run the latest version explicitly with:")
        logger.info(f"  {REMINDER_CMD}")

if __name__ == "__main__":
    main()
