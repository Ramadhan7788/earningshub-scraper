from __future__ import annotations
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

from .eh_helper import (
    normalize_quarter, clean_text,
    parse_est_act, determine_status,
    parse_date_time, parse_percent,
)

class EarningsHubHistoryParser:

    def __init__(self, soup: BeautifulSoup, ticker: str) -> None:
        self.soup = soup
        self.ticker = ticker

    # -------------------------- public API --------------------------
    def parse_rows(self) -> List[Dict[str, Any]]:
        table = self._find_earnings_table()
        if table is None:
            return []

        headers = self._parse_headers(table)
        if not headers:
            return []

        data_rows = []
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            row_map = self._row_to_map(headers, cells)
            parsed = self._transform_row(row_map)
            if parsed:
                data_rows.append(parsed)

        return data_rows

    # -------------------------- table helpers --------------------------
    def _find_earnings_table(self):

        t = self.soup.find("table", class_=lambda x: x and "MuiTable-root" in x)
        if t:
            return t

        for table in self.soup.find_all("table"):
            ths = table.find_all("th")
            if len(ths) >= 4:
                return table
        return None

    def _parse_headers(self, table) -> List[str]:
        headers: List[str] = []
        for th in table.find_all("th"):
            txt = th.get_text(strip=True)
            key = self._normalize_header(txt)
            headers.append(key)
        return headers

    @staticmethod
    def _normalize_header(h: str) -> str:
        h = (h or "").strip()
        h = h.lower().replace(" ", "_").replace(".", "").replace("%", "pct")
        return h

    def _row_to_map(self, headers: List[str], cells: List) -> Dict[str, Optional[str]]:
        row: Dict[str, Optional[str]] = {}
        for i, key in enumerate(headers):
            if i < len(cells):
                txt = clean_text(cells[i].get_text(strip=True))
                row[key] = txt
            else:
                row[key] = None
        return row

    # -------------------------- transformation --------------------------
    def _transform_row(self, row: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:

        # --- Quarter
        quarter_raw = row.get("earnings") or row.get("quarter") or None
        quarter = normalize_quarter(quarter_raw)

        date_raw = row.get("release_date") or row.get("date")
        date, time = parse_date_time(date_raw)

        # --- Revenue
        rev_est_raw = row.get("rev_est")
        rev_act_raw = row.get("rev_act")
        rev_parsed = parse_est_act([v for v in [rev_est_raw, rev_act_raw] if v])

        rev_est = rev_parsed["est"]["value"]
        rev_est_unit = rev_parsed["est"]["unit"]
        rev_act = rev_parsed["act"]["value"]
        rev_act_unit = rev_parsed["act"]["unit"]

        # --- EPS
        eps_est_raw = row.get("eps_est")
        eps_act_raw = row.get("eps_act")
        eps_parsed = parse_est_act([v for v in [eps_est_raw, eps_act_raw] if v])

        eps_est = eps_parsed["est"]["value"]
        eps_act = eps_parsed["act"]["value"]

        # --- Percent
        rev_pct = parse_percent(row.get("rev_surp") or row.get("rev_pct"))
        eps_pct = parse_percent(row.get("eps_surp") or row.get("eps_pct"))

        # --- Status Beat/Miss
        rev_status = determine_status(rev_pct, rev_act, rev_est)
        eps_status = determine_status(eps_pct, eps_act, eps_est)

        if not any([quarter, date, rev_est, rev_act, eps_est, eps_act]):
            return None

        return {
            "ticker": self.ticker,
            "quarter": quarter or "",
            "date": date or "",
            "rev_est": rev_est or "",
            "rev_est_unit": (rev_est_unit or ""),
            "rev_act": rev_act or "",
            "rev_act_unit": (rev_act_unit or ""),
            "rev_pct": (f"{rev_pct:.2f}" if rev_pct is not None else ""),
            "rev_status": rev_status or "",
            "eps_est": eps_est or "",
            "eps_act": eps_act or "",
            "eps_pct": (f"{eps_pct:.2f}" if eps_pct is not None else ""),
            "eps_status": eps_status or "",
        }
