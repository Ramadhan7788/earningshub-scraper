from __future__ import annotations
import logging
import time
from pathlib import Path
from typing import Sequence

from .settings import settings

log = logging.getLogger(__name__)

def delete_old_files(
    directory: Path,
    *,
    patterns: Sequence[str] = ("*.html", "*.json", "*.csv"),
    max_age_hours: int = 3,
    recursive: bool = False,
) -> int:
    """
    Delete files in `directory` matching `patterns` if older than `max_age_hours`.
    Returns the count of deleted files.
    """
    if not directory.exists():
        return 0

    now = time.time()
    threshold = max_age_hours * 3600
    count = 0

    globber = directory.rglob if recursive else directory.glob
    for pattern in patterns:
        for file_path in globber(pattern):
            try:
                mtime = file_path.stat().st_mtime
                age = now - mtime
                if age > threshold:
                    file_path.unlink()
                    count += 1
                    log.info("Deleted old file: %s (age %.1f hours)", file_path, age / 3600)
            except FileNotFoundError:
                # File might have been removed by another process
                continue
            except Exception as e:
                log.warning("Failed to delete file %s: %s", file_path, e)
    return count


def cleanup_artifacts(max_age_hours: int = 3) -> None:
    """
    Clean stale files across cache (HTML) and export (JSON/CSV).
    """
    cache_dir = Path(settings.cache_dir)
    export_dir = Path(settings.data_dir) / "export"

    n_cache = delete_old_files(cache_dir, patterns=("*.html",), max_age_hours=max_age_hours, recursive=False)
    n_export = delete_old_files(export_dir, patterns=("*.json", "*.csv"), max_age_hours=max_age_hours, recursive=False)

    log.info("Cleanup complete: %d HTML cache files, %d export files removed (>%dh).",
             n_cache, n_export, max_age_hours)