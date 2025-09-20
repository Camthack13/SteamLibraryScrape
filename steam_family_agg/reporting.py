from .models import Results, Config

REMINDER_CMD = "python -m steam_family_agg.main"

def print_report(res: Results, cfg: Config, used_default: bool):
    print(f"\nExported {len(res.rows)} unique GAMES → {res.out_path.resolve()}")

    # Accounts
    print(f"\nAccounts aggregated: {res.ok_accounts}/{res.total_accounts}")
    if res.failed_accounts:
        print("Failed accounts:")
        for line in res.failed_accounts:
            print(f"  - {line}")

    # Type filtering
    print(f"\nType breakdown: {res.type_counts}")
    print(f"Included games: {res.included_games} (excluded {res.excluded_items} non-games)")

    # Timing
    t = res.timings
    per = lambda s, n: (s/max(1,n))
    print("\n=== Timing summary ===")
    print(f"Fetch libraries total: {t.fetch_libraries:.2f}s")
    print(f"Meta (type+release+size) total: {t.meta:.2f}s")
    print(f"Enrichment total:      {t.enrichment:.2f}s")
    if cfg.include_reviews:
        print(f"  Reviews JSON:        {t.reviews:.2f}s  (~{per(t.reviews, res.included_games):.3f}s/app)")
    else:
        print("  Reviews JSON:        skipped")
    if cfg.include_last_update:
        print(f"  Last Update (news):  {t.last_update:.2f}s  (~{per(t.last_update, res.included_games):.3f}s/app)")
    else:
        print("  Last Update (news):  skipped")
    print(f"Total runtime:         {t.total:.2f}s")

    # Coverage
    print("\n=== Not-blank coverage by column (games only) ===")
    for col, cnt in res.coverage.counts.items():
        pct = res.coverage.perc[col]
        print(f"{col}: {cnt}/{res.coverage.total_rows} ({pct:.1f}%)")

    # Hints
    if cfg.include_release_size and res.coverage.perc.get("release_year", 0.0) < 50.0:
        print(f"\nHint: only {res.coverage.perc['release_year']:.1f}% had release_year → consider turning it OFF next run.")
    if cfg.include_release_size and res.coverage.perc.get("approx_install_size_gb", 0.0) < 50.0:
        print(f"Hint: only {res.coverage.perc['approx_install_size_gb']:.1f}% had install size → consider turning it OFF.")
    if cfg.include_last_update and res.coverage.perc.get("last_update_year", 0.0) < 50.0:
        print(f"Hint: only {res.coverage.perc['last_update_year']:.1f}% had last update year → consider turning it OFF.")

    # Reminder
    print("\nTip: to run again:")
    print(f"  {REMINDER_CMD}\n")
    if used_default:
        print("(Used ./ids.txt automatically this run.)")
