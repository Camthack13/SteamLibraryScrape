from pathlib import Path
from .models import Config

def _ask_yn(prompt: str, default: bool = True) -> bool:
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        print(prompt + suffix, end="", flush=True)
        ans = input().strip().lower()
        if ans == "": return default
        if ans in ("y","yes"): return True
        if ans in ("n","no"):  return False
        print("Please answer y or n.")

def _ask_int(prompt: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    suffix = f" [{default}] "
    while True:
        print(prompt + suffix, end="", flush=True)
        raw = input().strip()
        if raw == "": return default
        try:
            val = int(raw)
            if val < minimum: print(f"Enter >= {minimum}."); continue
            if maximum is not None and val > maximum: print(f"Enter <= {maximum}."); continue
            return val
        except ValueError:
            print("Enter a whole number.")

def _ask_path(prompt: str, default: Path | None = None) -> Path:
    if default:
        print(f"{prompt} [{default}] ", end="", flush=True)
        raw = input().strip()
        return default if raw == "" else Path(raw)
    print(prompt + " ", end="", flush=True)
    return Path(input().strip())

def interactive_setup() -> tuple[Config, bool]:
    print("\n=== Steam Family Aggregator (interactive) ===\n", flush=True)

    use_default = _ask_yn("Use ./ids.txt for account list?", True)
    if use_default:
        ids_path = Path("ids.txt")
        if not ids_path.exists():
            print("No ./ids.txt found.")
            ids_path = _ask_path("Enter path to your IDs file:")
    else:
        ids_path = _ask_path("Enter path to your IDs file:")
    while not ids_path.exists():
        print(f"File not found: {ids_path}")
        ids_path = _ask_path("Enter a valid path:")

    verbose = _ask_yn("Enable verbose logs (INFO)?", True)
    debug   = _ask_yn("Enable debug logs (very chatty)?", False)

    # NEW: Safe mode first, then fewer questions
    safe_mode = _ask_yn("Safe mode (slower, fewer workers, fewer 403s)?", True)
    if safe_mode:
        workers = 6
        day_rng = 30
        min_delay_store, min_delay_comm, min_delay_api, jitter_ms = 0.6, 0.25, 0.20, 120
    else:
        workers = _ask_int("How many parallel workers?", 12, 1, 128)
        day_rng = _ask_int("Days for 'recent' reviews window?", 30, 1, 365)
        # still be gentle by default
        min_delay_store, min_delay_comm, min_delay_api, jitter_ms = 0.35, 0.15, 0.15, 100

    inc_reviews      = _ask_yn("Include review summary and percent positive?", True)
    inc_release_size = _ask_yn("Include release year and install size?", True)
    inc_last_update  = _ask_yn("Include last update year (from news)?", True)

    print("\n--- Choices ---")
    print(f"IDs file:            {ids_path}")
    print(f"Verbose / Debug:     {verbose} / {debug}")
    print(f"Safe mode:           {safe_mode}")
    print(f"Workers:             {workers}")
    print(f"Recent day range:    {day_rng}")
    print(f"Reviews:             {'ON' if inc_reviews else 'OFF'}")
    print(f"Release+Size:        {'ON' if inc_release_size else 'OFF'}")
    print(f"Last update year:    {'ON' if inc_last_update else 'OFF'}")
    if not _ask_yn("Proceed with these settings?", True):
        raise SystemExit("Aborted by user.")

    cfg = Config(
        ids_path=ids_path,
        verbose=verbose,
        debug=debug,
        workers=workers,
        day_range=day_rng,
        include_reviews=inc_reviews,
        include_release_size=inc_release_size,
        include_last_update=inc_last_update,
        safe_mode=safe_mode,
        min_delay_store=min_delay_store,
        min_delay_community=min_delay_comm,
        min_delay_api=min_delay_api,
        jitter_ms=jitter_ms,
    )
    return cfg, use_default
