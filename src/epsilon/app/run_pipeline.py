from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, Literal, Optional, Any

from src.epsilon.config.settings import settings
from src.epsilon.config.paths import cleanup_artifacts
from src.epsilon.services.scraper.earningshub.scraper import (
    EarningsHubModularScraper,
)
from src.epsilon.services.parser.earningshub.controller import (
    export_overview_to_json,
    export_earnings_to_csv,  # used for CSV sink and as fallback only
)

from src.epsilon.services.db.db import (
    create_database,
    create_tables_if_not_exists,
    _validate_db_settings,
)

# DB loaders
from src.epsilon.services.db.crud import (
    load_csv_earnings,        # legacy CSV path
    load_earnings_rows,       # direct rows → DB
    load_csv_text_earnings,   # CSV text in-memory → DB
)

AllowedVariant = Literal["overview", "full"]

log = logging.getLogger(__name__)


def build_earningshub_url(ticker: str) -> str:
    t = ticker.strip().upper()
    return f"https://www.earningshub.com/quote/{t}"


def _delete_cache_if_exists(cache_dir: Path, ticker: str, variants: Iterable[str]) -> None:
    for v in variants:
        p = cache_dir / f"{ticker.strip().upper()}_{v}.html"
        if p.exists():
            try:
                p.unlink()
                log.info("Deleted existing cache: %s", p)
            except Exception as e:
                log.warning("Failed deleting cache %s: %s", p, e)


def _parse_overview_json_only(ticker: str, output_dir: Path) -> Path:
    t = ticker.strip().upper()
    ov_path = Path(settings.cache_dir) / f"{t}_overview.html"
    an_path = Path(settings.cache_dir) / f"{t}_analyst.html"
    json_path = Path(output_dir) / f"{t}_eh_overview.json"

    if not ov_path.exists():
        raise FileNotFoundError(f"Overview HTML file not found: {ov_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # analyst is optional
    analyst_html: Optional[Path] = an_path if an_path.exists() else None

    log.info("Parsing overview(+analyst) HTML -> JSON: %s , %s -> %s", ov_path, analyst_html, json_path)
    out = export_overview_to_json(ov_path, analyst_html, t, json_path)
    log.info("Successfully parsed JSON: %s", out)
    return out


def _parse_earnings_csv_only(ticker: str, output_dir: Path) -> Path:
    t = ticker.strip().upper()
    html_path = Path(settings.cache_dir) / f"{t}_earnings.html"

    if not html_path.exists():
        raise FileNotFoundError(f"Earnings HTML file not found: {html_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = Path(output_dir) / f"{t}_eh_earnings.csv"

    log.info("Parsing earnings HTML -> CSV for ticker: %s", t)
    out = export_earnings_to_csv(html_path, t, out_csv)
    log.info("Successfully parsed CSV: %s", out)
    return out


def _import_csv_to_db(csv_path: Path, ticker: str) -> int:
    """
    Legacy CSV→DB path retained for compatibility (used when sink∈{'csv','both'}).
    """
    _validate_db_settings()
    create_database()
    create_tables_if_not_exists()

    t = ticker.strip().upper()
    summary = load_csv_earnings(str(csv_path), default_ticker=t)

    if isinstance(summary, dict):
        inserted = int(summary.get("inserted", 0))
        updated = int(summary.get("updated", 0))
        unchanged = int(summary.get("unchanged", 0))
        attempted = int(summary.get("attempted", inserted + updated + unchanged))

        log.info(
            "CSV import summary for %s -> %s.%s | inserted=%d, updated=%d, unchanged=%d, attempted=%d",
            t, settings.database, settings.table, inserted, updated, unchanged, attempted
        )
        return attempted
    else:
        total = int(summary)
        log.info(
            "Imported/Upserted %d rows into %s.%s for %s",
            total, settings.database, settings.table, t
        )
        return total


def _direct_parse_earnings_to_db(ticker: str) -> Dict[str, int]:
    """
    New path: earnings HTML (cache) -> parse in-memory -> upsert to DB.
    - Prefer a parser that returns rows; if unavailable, fallback to CSV text in-memory.
    """
    _validate_db_settings()
    create_database()
    create_tables_if_not_exists()

    t = ticker.strip().upper()
    html_path = Path(settings.cache_dir) / f"{t}_earnings.html"
    if not html_path.exists():
        raise FileNotFoundError(f"Earnings HTML file not found: {html_path}")

    # Try to use an in-memory rows parser if available
    try:
        # Imported lazily to avoid hard dependency if not present
        from src.epsilon.services.parser.earningshub.controller import parse_earnings_rows  # type: ignore
        html_text = html_path.read_text(encoding="utf-8", errors="replace")
        rows = list(parse_earnings_rows(html_text, ticker=t))  # expected to yield Earnings dataclasses
        summary = load_earnings_rows(rows)
        log.info(
            "Direct parse→DB summary for %s -> %s.%s | inserted=%d updated=%d unchanged=%d attempted=%d",
            t, settings.database, settings.table,
            summary.get("inserted", 0),
            summary.get("updated", 0),
            summary.get("unchanged", 0),
            summary.get("attempted", 0),
        )
        return summary  # type: ignore[return-value]
    except Exception as e:
        log.warning("parse_earnings_rows unavailable or failed (%s). Falling back to CSV-in-memory.", e)

    # Fallback: produce CSV to a temp file, read text, delete, then load to DB (no artifact left)
    tmp_dir = Path(settings.data_dir) / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_csv = tmp_dir / f"{t}_eh_earnings.tmp.csv"

    out = export_earnings_to_csv(html_path, t, tmp_csv)
    csv_text = Path(out).read_text(encoding="utf-8", errors="replace")
    try:
        Path(out).unlink(missing_ok=True)
    except Exception:
        pass

    summary = load_csv_text_earnings(csv_text, default_ticker=t)
    log.info(
        "Direct parse→DB (via CSV-text) for %s -> %s.%s | inserted=%d updated=%d unchanged=%d attempted=%d",
        t, settings.database, settings.table,
        summary.get("inserted", 0),
        summary.get("updated", 0),
        summary.get("unchanged", 0),
        summary.get("attempted", 0),
    )
    return summary  # type: ignore[return-value]


def run_for_ticker(
    ticker: str,
    *,
    variant: AllowedVariant,
    force_refresh: bool,
    max_analyst_refresh: int,
    parse_json: bool,
    sink: Literal["db", "csv", "both"] = "db",
    cleanup_before: bool = True,
    cleanup_age_hours: int = 3,
) -> Dict[str, Any]:
    """
    Orchestrates:
      1) optional cache deletion,
      2) fetching HTML(s),
      3) parsing:
         - overview → JSON (optional)
         - earnings → DB (default sink) and/or CSV (if requested)
    Returns a dict with keys like: 'overview', 'analyst', 'earnings', 'json', 'csv', 'db'.
    """
    t = ticker.strip().upper()
    url = build_earningshub_url(t)
    scraper = EarningsHubModularScraper(cache_dir=settings.cache_dir)

    if cleanup_before:
        cleanup_artifacts(max_age_hours=cleanup_age_hours)

    if force_refresh:
        needs = ["overview", "analyst"] if variant == "overview" else ["overview", "analyst", "earnings"]
        _delete_cache_if_exists(Path(settings.cache_dir), t, needs)

    results: Dict[str, Any] = scraper.fetch_variants(
        url=url,
        variants=[variant],              # "overview" or "full"
        single_session=True,             # keep one session for speed/stability
        name_prefix=t,
        max_analyst_refresh=max_analyst_refresh,
    )

    # Normalize if scraper returns only 'full' bundle
    if "full" in results:
        results.setdefault("overview", results["full"])
        results.setdefault("analyst", results["full"])
        results.setdefault("earnings", results["full"])

    # Overview JSON export (optional)
    if parse_json and "overview" in results:
        try:
            json_path = _parse_overview_json_only(t, Path(settings.data_dir) / "export")
            results["json"] = json_path
        except Exception as e:
            log.error("Failed to parse overview to JSON for %s: %s", t, e)

    # Earnings: default is direct parse→DB (no CSV artifact)
    if "earnings" in results:
        if sink in {"db", "both"}:
            try:
                summary = _direct_parse_earnings_to_db(t)
                results["db"] = summary
            except Exception as e:
                log.exception(
                    "Failed direct parse→DB for %s (host=%r, port=%r, user=%r, db=%r)",
                    t, settings.host, settings.port, settings.user, settings.database
                )

        if sink in {"csv", "both"}:
            try:
                csv_path = _parse_earnings_csv_only(t, Path(settings.data_dir) / "export")
                results["csv"] = csv_path
            except Exception as e:
                log.error("Failed to parse earnings to CSV for %s: %s", t, e)

    for k, v in results.items():
        log.info("Result: %s -> %s", k, v)

    return results
