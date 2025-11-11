"""
Run the EarningsHub scraping pipeline from the command line.

Usage (examples):
    # Default sink is "db": scrape → parse → upsert to DB (no CSV artifact)
    python -m src.epsilon.app.cli --ticker MSFT --variant overview
    python -m src.epsilon.app.cli --ticker MSFT --variant full

    # Multiple tickers
    python -m src.epsilon.app.cli --ticker AAPL,MSFT --variant full

    # Force refresh cache & limit analyst refresh
    python -m src.epsilon.app.cli --ticker NVDA --variant overview --force-refresh --max-analyst-refresh 2

    # Choose sink behavior explicitly
    python -m src.epsilon.app.cli --ticker MSFT --variant full --sink db    # parse→DB only (default)
    python -m src.epsilon.app.cli --ticker MSFT --variant full --sink csv   # produce CSV only
    python -m src.epsilon.app.cli --ticker MSFT --variant full --sink both  # CSV + DB

Notes:
- "overview" will save <TICKER>_overview.html and <TICKER>_analyst.html
- "full"     will save <TICKER>_overview.html, <TICKER>_analyst.html, and <TICKER>_earnings.html
- Paths are controlled by src.epsilon.config.settings (data_dir, cache_dir, log_dir).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

from src.epsilon.config.settings import settings
from src.epsilon.app.run_pipeline import run_for_ticker

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run EarningsHub scraping pipeline.")
    ap.add_argument(
        "--ticker",
        required=True,
        help="Ticker symbol or comma-separated list (e.g., 'AAPL,MSFT').",
    )
    ap.add_argument(
        "--variant",
        choices=["overview", "full"],
        default="overview",
        help="Which variant to run: 'overview' or 'full' (overview+earnings).",
    )
    ap.add_argument(
        "--max-analyst-refresh",
        type=int,
        default=2,
        help="Max page refreshes while looking for Analyst Ratings section.",
    )
    ap.add_argument(
        "--force-refresh",
        action="store_true",
        help="Delete existing cache for requested variant(s) before scraping.",
    )
    ap.add_argument(
        "--sink",
        choices=["db", "csv", "both"],
        default="db",
        help="Where to send parsed earnings: 'db' (default, direct upsert), "
             "'csv' (write CSV artifact only), or 'both' (CSV + DB).",
    )
    # --parse-json retained for overview JSON export (optional)
    ap.add_argument(
        "--parse-json",
        action="store_true",
        help="Parse the overview HTML to JSON format after scraping.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    log.info(
        "=== run_pipeline start | variant=%s | tickers=%s | sink=%s | parse_json=%s ===",
        args.variant, args.ticker, args.sink, args.parse_json
    )

    tickers = [t.strip() for t in args.ticker.split(",") if t.strip()]
    if not tickers:
        raise SystemExit("No valid tickers provided.")
    
    if args.sink in {"csv", "both"} or args.parse_json:
        export_dir = Path(settings.data_dir) / "export"
        export_dir.mkdir(parents=True, exist_ok=True)

    failures: Dict[str, str] = {}
    for t in tickers:
        try:
            paths = run_for_ticker(
                t,
                variant=args.variant,
                force_refresh=args.force_refresh,
                max_analyst_refresh=args.max_analyst_refresh,
                parse_json=args.parse_json,
                sink=args.sink,
            )
            # Simple stdout summary per ticker
            pretty = ", ".join(f"{k}={v}" for k, v in paths.items())
            print(f"[OK] {t}: {pretty}")
        except Exception as e:
            failures[t] = str(e)
            log.exception("Failed for %s: %s", t, e)
            print(f"[FAIL] {t}: {e}")

    if failures:
        raise SystemExit(f"Failures: {failures}")


if __name__ == "__main__":
    main()
