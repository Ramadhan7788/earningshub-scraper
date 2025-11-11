from __future__ import annotations

import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from typing import Optional

from src.epsilon.services.output.writer import write_json, write_csv

from src.epsilon.services.parser.earningshub.overview_parser import EarningsHubOverviewParser
from src.epsilon.services.parser.earningshub.history_parser import EarningsHubHistoryParser

def load_soup(path: Path) -> BeautifulSoup:
    html = path.read_text(encoding="utf-8", errors="replace")
    return BeautifulSoup(html, "html.parser")

def parse_overview(overview_html: Path, analyst_html: Optional[Path], ticker: str) -> dict:
    ov = load_soup(overview_html)
    an = load_soup(analyst_html) if analyst_html and analyst_html.exists() else None
    return EarningsHubOverviewParser(overview_soup=ov, analyst_soup=an, ticker=ticker).parse_all()

def parse_earnings(earnings_html: Path, ticker: str) -> pd.DataFrame:
    soup = load_soup(earnings_html)
    rows = EarningsHubHistoryParser(soup, ticker).parse_rows()
    cols = [
        "ticker","quarter","date",
        "rev_est","rev_est_unit","rev_act","rev_act_unit","rev_pct","rev_status",
        "eps_est","eps_act","eps_pct","eps_status",
    ]
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame([], columns=cols)
    # Hindari index custom; biarkan default saja
    # Hindari hardcoded rename; konsistenkan nanti di layer DB jika perlu
    return (df.drop_duplicates(subset=["quarter","date"], keep="first")
              .reset_index(drop=True))

def export_overview_to_json(overview_html: Path, analyst_html: Optional[Path],
                            ticker: str, out_json: Path) -> Path:
    data = parse_overview(overview_html, analyst_html, ticker)
    return write_json(data, out_json)

def export_earnings_to_csv(earnings_html: Path, ticker: str, out_csv: Path) -> Path:
    df = parse_earnings(earnings_html, ticker)
    return write_csv(df, out_csv)