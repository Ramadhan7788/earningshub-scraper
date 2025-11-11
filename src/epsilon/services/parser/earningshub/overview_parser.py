from __future__ import annotations
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional
from dataclasses import dataclass

from .selectors import (
    COMPANY_SELECTORS, ANALYST_RATING_SELECTORS,
    LATEST_SELECTORS, UPCOMING_SELECTORS,
)

from .eh_helper import (
    clean_text, normalize_quarter, parse_date_time,
    parse_est_act, extract_percentage_from_item, determine_status,
    parse_indicator
)

@dataclass
class OverviewBlocks:
    overview_soup: BeautifulSoup
    analyst_soup: Optional[BeautifulSoup] = None

class EarningsHubOverviewParser:

    def __init__(
        self, 
        overview_soup: BeautifulSoup, 
        analyst_soup: Optional[BeautifulSoup] = None, 
        ticker: Optional[str] = None
    ) -> None:
        self.blocks = OverviewBlocks(overview_soup=overview_soup, analyst_soup=analyst_soup)
        self.ticker = ticker

    # -------- COMPANY --------
    def parse_company(self) -> Dict[str, Optional[str]]:
        soup = self.blocks.overview_soup
        info: Dict[str, Optional[str]] = {
            "ticker": self.ticker,
            "company_name": None
        }
        for container in soup.select(COMPANY_SELECTORS["container"]):
            el = container.select_one(COMPANY_SELECTORS["name"])
            if el:
                info["company_name"] = clean_text(el.get_text())
                break
        return info
    
    # -------- ANALYST RATINGS --------
    def parse_analyst_ratings(self) -> Dict[str, Optional[str]]:
        soup = self.blocks.analyst_soup or self.blocks.overview_soup  # fallback jika analyst_soup belum disuplai

        data: Dict[str, Optional[str]] = {
            "indicator": None,
            "Buy": None,
            "Hold": None,
            "Sell": None,
        }

        for container in soup.select(ANALYST_RATING_SELECTORS["container"]):
            # Indicator: "3 Months", "6 Months", dst.
            ind_block = container.select_one(ANALYST_RATING_SELECTORS["indicator_block"])
            if ind_block:
                ind_text = ind_block.get_text(separator=" ", strip=True)
                data["indicator"] = parse_indicator(ind_text)

            # Distribusi Buy/Hold/Sell
            rating_block = container.select_one(ANALYST_RATING_SELECTORS["rating_block"])
            if rating_block:
                labels = rating_block.select(ANALYST_RATING_SELECTORS["label"])
                values = rating_block.select(ANALYST_RATING_SELECTORS["value"])
                if len(labels) == len(values):
                    for lab, val in zip(labels, values):
                        k = clean_text(lab.get_text())
                        if k in data:
                            data[k] = clean_text(val.get_text())

            if any(data[k] is not None for k in ("Buy", "Hold", "Sell")):
                break

        return data

    # -------- UPCOMING --------
    def parse_upcoming(self) -> Dict[str, Any]:
        soup = self.blocks.overview_soup
        data: Dict[str, Any] = {
            "ticker": self.ticker,
            "quarter": None,
            "date": None,
            "time": None,
            "rev_est": None,
            "rev_unit": None,
            "eps_est": None,
        }

        for c in soup.select(UPCOMING_SELECTORS["container"]):
            db = c.select_one(UPCOMING_SELECTORS["date_block"])
            if db:
                q = db.select_one(UPCOMING_SELECTORS["quarter"])
                dt = db.select_one(UPCOMING_SELECTORS["date_time"])
                if q and dt:
                    data["quarter"] = normalize_quarter(q.get_text(strip=True))
                    data["date"], data["time"] = parse_date_time(dt.get_text(strip=True))

            eb = c.select_one(UPCOMING_SELECTORS["est_block"])
            if eb:
                labels = [el.get_text(strip=True).upper() for el in eb.select(UPCOMING_SELECTORS["est_label"])]
                values = [el.get_text(strip=True) for el in eb.select(UPCOMING_SELECTORS["est_value"])]

                for lab, val in zip(labels, values):
                    parsed = parse_est_act([val]) if val else {"est": {"value": None, "unit": None}, "act": {"value": None, "unit": None}}
                    est_val, est_unit = parsed["est"]["value"], parsed["est"]["unit"]
                    if lab == "REVENUE":
                        data["rev_est"] = est_val
                        data["rev_unit"] = est_unit
                    elif lab == "EPS":
                        data["eps_est"] = est_val

            if data["quarter"] or data["rev_est"] or data["eps_est"]:
                break

        return data

    # -------- LATEST --------
    def parse_latest(self) -> Dict[str, Any]:
        soup = self.blocks.overview_soup
        result: Dict[str, Any] = {
            "ticker": self.ticker,
            "quarter": None, "date": None, #"time": None,
            "rev_est": None, "rev_est_unit": None,
            "rev_act": None, "rev_act_unit": None,
            "rev_prc": None, "rev_sta": None,
            "eps_est": None, "eps_act": None,
            "eps_prc": None, "eps_sta": None,
        }

        for container in soup.select(LATEST_SELECTORS["container"]):
            # date block
            db = container.select_one(LATEST_SELECTORS["date_block"])
            if db:
                q_el = db.select_one(LATEST_SELECTORS["quarter"])
                dt_el = db.select_one(LATEST_SELECTORS["date_time"])
                if q_el and dt_el:
                    result["quarter"] = normalize_quarter(q_el.get_text(strip=True))
                    result["date"], result["time"] = parse_date_time(dt_el.get_text(strip=True))

            # metrics
            mb = container.select_one(LATEST_SELECTORS["metrics_block"])
            if not mb:
                continue

            for item in mb.select(LATEST_SELECTORS["metric_item"]):
                label = item.select_one(LATEST_SELECTORS["label"])
                est_p = item.select_one(LATEST_SELECTORS["est_value"])
                act_p = item.select_one(LATEST_SELECTORS["act_value"])
                if not label:
                    continue

                label_text = (label.get_text(strip=True) or "").lower()

                # Est/Act parsing konsisten via eh_helper.parse_est_act
                values = []
                if est_p:
                    values.append(est_p.get_text(strip=True))
                if act_p:
                    values.append(act_p.get_text(strip=True))

                parsed = parse_est_act(values) if values else {"est": {"value": None, "unit": None}, "act": {"value": None, "unit": None}}
                est_val, est_unit = parsed["est"]["value"], parsed["est"]["unit"]
                act_val, act_unit = parsed["act"]["value"], parsed["act"]["unit"]

                pct = extract_percentage_from_item(item)

                if "rev" in label_text:
                    result["rev_est"] = est_val
                    result["rev_est_unit"] = est_unit
                    result["rev_act"] = act_val
                    result["rev_act_unit"] = act_unit
                    result["rev_prc"] = pct
                    result["rev_sta"] = determine_status(pct, act_val, est_val)
                elif "eps" in label_text:
                    result["eps_est"] = est_val
                    result["eps_act"] = act_val
                    result["eps_prc"] = pct
                    result["eps_sta"] = determine_status(pct, act_val, est_val)

            return result

        return result

    # -------- ALL --------
    def parse_all(self) -> Dict[str, Any]:
        return {
            "company": self.parse_company(),
            "analyst_ratings": self.parse_analyst_ratings(),
            "upcoming": self.parse_upcoming(),
            "latest": self.parse_latest(),
        }