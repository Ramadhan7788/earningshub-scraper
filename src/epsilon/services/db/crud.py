from __future__ import annotations

import csv
from io import StringIO
from datetime import datetime, date
from typing import Any, Iterable, NamedTuple, Literal, Iterator

from src.epsilon.config.settings import settings
from src.epsilon.services.db.db import connect_db
from src.epsilon.services.db.models import Earnings

# ---------- Upsert single row ----------
class UpsertResult(NamedTuple):
    id: int
    action: Literal["inserted", "updated", "unchanged"]

# ---------- CSV helpers ----------
# ----------- Parsing helpers ----------
def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s == "":
        return None
    
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None
    
# ---------- Date parsing ----------
def _to_date(val: str) -> date:

    s = str(val).strip()
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {val!r}")

# ---------- Load from CSV ----------
def _iter_csv_dict_to_earnings(rows: Iterator[dict], default_ticker: str | None) -> Iterator["Earnings"]:
    for row in rows:
        ticker = (row.get("ticker") or default_ticker or "").strip()
        if not ticker:
            continue
        yield Earnings(
            id=None,
            ticker=ticker,
            quarter=(row.get("quarter") or None),
            report_date=_to_date(row["date"]),
            rev_est=_to_float(row.get("rev_est")),
            rev_est_unit=(row.get("rev_est_unit") or None),
            rev_act=_to_float(row.get("rev_act")),
            rev_act_unit=(row.get("rev_act_unit") or None),
            rev_pct=_to_float(row.get("rev_pct")),
            rev_status=(row.get("rev_status") or None),
            eps_est=_to_float(row.get("eps_est")),
            eps_act=_to_float(row.get("eps_act")),
            eps_pct=_to_float(row.get("eps_pct")),
            eps_status=(row.get("eps_status") or None),
        )

# ---------- Upsert single row ----------
def upsert_earnings_row(e: "Earnings") -> UpsertResult:
    table = f"`{settings.table}`"
    sql = f"""
        INSERT INTO {table}
            (ticker, quarter, report_date, rev_est, rev_est_unit, rev_act, rev_act_unit,
             rev_pct, rev_status, eps_est, eps_act, eps_pct, eps_status)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            quarter = VALUES(quarter),
            rev_est = VALUES(rev_est),
            rev_est_unit = VALUES(rev_est_unit),
            rev_act = VALUES(rev_act),
            rev_act_unit = VALUES(rev_act_unit),
            rev_pct = VALUES(rev_pct),
            rev_status = VALUES(rev_status),
            eps_est = VALUES(eps_est),
            eps_act = VALUES(eps_act),
            eps_pct = VALUES(eps_pct),
            eps_status = VALUES(eps_status)
    """
    params = (
        e.ticker, e.quarter, e.report_date,
        e.rev_est, e.rev_est_unit, e.rev_act, e.rev_act_unit,
        e.rev_pct, e.rev_status, e.eps_est, e.eps_act, e.eps_pct, e.eps_status,
    )

    conn = connect_db()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()

    # rowcount semantics: 1=inserted, 2=updated, 0=unchanged
    if cur.rowcount == 1:
        action: Literal["inserted","updated","unchanged"] = "inserted"
    elif cur.rowcount == 2:
        action = "updated"
    else:
        action = "unchanged"

    row_id = int(cur.lastrowid)
    cur.close(); conn.close()
    return UpsertResult(id=row_id, action=action)

# ---------- Load from CSV ----------
def _iter_csv_rows(csv_path: str, default_ticker: str | None) -> Iterable[tuple]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("ticker") or default_ticker or "").strip()
            if not ticker:
                continue

            quarter = (row.get("quarter") or None)
            report_date = _to_date(row["date"])

            yield (
                ticker,
                quarter,
                report_date,
                _to_float(row.get("rev_est")),
                (row.get("rev_est_unit") or None),
                _to_float(row.get("rev_act")),
                (row.get("rev_act_unit") or None),
                _to_float(row.get("rev_pct")),
                (row.get("rev_status") or None),
                _to_float(row.get("eps_est")),
                _to_float(row.get("eps_act")),
                _to_float(row.get("eps_pct")),
                (row.get("eps_status") or None),
            )

# ---------- Load from CSV text ----------
def load_csv_text_earnings(csv_text: str, default_ticker: str | None = None) -> dict:
    sio = StringIO(csv_text)
    reader = csv.DictReader(sio)
    return load_earnings_rows(_iter_csv_dict_to_earnings(reader, default_ticker))

# ---------- Load multiple rows ----------
def load_earnings_rows(rows: Iterable["Earnings"], commit_every: int = 500) -> dict:
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "attempted": 0}

    table = f"`{settings.table}`"
    sql = f"""
        INSERT INTO {table}
            (ticker, quarter, report_date, rev_est, rev_est_unit, rev_act, rev_act_unit,
             rev_pct, rev_status, eps_est, eps_act, eps_pct, eps_status)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            quarter = VALUES(quarter),
            rev_est = VALUES(rev_est),       rev_est_unit = VALUES(rev_est_unit),
            rev_act = VALUES(rev_act),       rev_act_unit = VALUES(rev_act_unit),
            rev_pct = VALUES(rev_pct),       rev_status = VALUES(rev_status),
            eps_est = VALUES(eps_est),       eps_act = VALUES(eps_act),
            eps_pct = VALUES(eps_pct),       eps_status = VALUES(eps_status)
    """

    conn = connect_db()
    cur = conn.cursor()
    try:
        batch = 0
        for e in rows:
            params = (
                e.ticker, e.quarter, e.report_date,
                e.rev_est, e.rev_est_unit, e.rev_act, e.rev_act_unit,
                e.rev_pct, e.rev_status, e.eps_est, e.eps_act, e.eps_pct, e.eps_status,
            )
            cur.execute(sql, params)
            if cur.rowcount == 1:
                stats["inserted"] += 1
            elif cur.rowcount == 2:
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
            stats["attempted"] += 1

            batch += 1
            if batch >= commit_every:
                conn.commit()
                batch = 0

        if batch:
            conn.commit()
    finally:
        cur.close()
        conn.close()

    return stats

# ---------- Load from CSV file ----------
def load_csv_earnings(csv_path: str, default_ticker: str | None = None) -> dict:
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "attempted": 0}
    for row in _iter_csv_rows(csv_path, default_ticker):
        # mapping tuple -> Earnings
        e = Earnings(
            id=None,
            ticker=row[0],
            quarter=row[1],
            report_date=row[2],
            rev_est=row[3],
            rev_est_unit=row[4],
            rev_act=row[5],
            rev_act_unit=row[6],
            rev_pct=row[7],
            rev_status=row[8],
            eps_est=row[9],
            eps_act=row[10],
            eps_pct=row[11],
            eps_status=row[12],
        )
        res = upsert_earnings_row(e)
        stats[res.action] += 1
        stats["attempted"] += 1
    return stats


# ---------- List by ticker ----------
def list_earnings_by_ticker(ticker: str, limit: int = 20) -> list[dict]:
    table = f"`{settings.table}`"
    query = f"""
        SELECT
            ticker, quarter, report_date, rev_est, rev_est_unit, rev_act, rev_act_unit,
            rev_pct, rev_status, eps_est, eps_act, eps_pct, eps_status, created_at
        FROM {table}
        WHERE ticker = %s
        ORDER BY report_date DESC
        LIMIT %s
    """
    conn = connect_db()
    cur = conn.cursor(dictionary=True)
    cur.execute(query, (ticker, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_latest_report_dates(ticker: str, limit: int = 10) -> list[dict]:
    table = f"`{settings.table}`"
    query = f"""
        SELECT 
            ticker,
            report_date
        FROM {table}
        WHERE ticker = %s
        ORDER BY report_date DESC
        LIMIT %s
    """
    
    conn = connect_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, (ticker.upper(), limit))
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()