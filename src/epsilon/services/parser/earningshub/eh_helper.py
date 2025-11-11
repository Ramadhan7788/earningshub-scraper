from __future__ import annotations
import re
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
from bs4 import Tag
from pathlib import Path

_QUARTER_PATTERNS = [
    r"(Q[1-4]\s+\d{4})",
    r"(FY\s+\d{4})",
    r"(H[12]\s+\d{4})",
]

NO_DATA = {"-", "--", "—", "N/A", "NA", "n/a", ""}

def _parse_one_value(raw: Optional[str]) -> dict[str, Optional[str] | bool]:

    if not raw:
        return {"value": None, "unit": None, "is_est": None}

    s = raw.strip()
    if s in NO_DATA:
        return {"value": None, "unit": None, "is_est": None}

    # Flag est/actual (case-insensitive, toleran spasi/paren)
    is_est = None
    if re.search(r"\b(est|estimate)\b", s, flags=re.I):
        is_est = True
    elif re.search(r"\b(actual)\b", s, flags=re.I):
        is_est = False

    s = (
        s.replace("(est)", "")
         .replace("(actual)", "")
         .replace("est", "")
         .replace("actual", "")
         .replace("$", "")
         .replace(",", "")
         .replace("*", "")
         .strip()
    )

    m = re.match(r"^(-?[\d.]+)\s*([A-Za-z]?)$", s)
    if not m:
        return {"value": s or None, "unit": None, "is_est": is_est}

    value, unit = m.groups()
    unit = unit.upper() or None
    return {"value": value, "unit": unit, "is_est": is_est}

def _fmt_time_portable(dt: datetime) -> str:
    try:
        return dt.strftime("%#I:%M %p")
    except ValueError:
        return dt.strftime("%-I:%M %p")

def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = " ".join(s.split()).strip()
    return None if t in {"-", "--", "—"} else t

def normalize_quarter(raw: Optional[str]) -> Optional[str]:
    raw = clean_text(raw)
    if not raw:
        return None
    for pat in _QUARTER_PATTERNS:
        m = re.search(pat, raw, re.IGNORECASE)
        if m:
            return m.group(1).upper().replace("  ", " ")
    return raw

def parse_date_time(raw_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    # Input format: 
    # 1. With day and time: 'Wed, Jul 31, 2025, 4:00 PM'
    # 2. Without time: 'Jul 31, 2025'
    # Output: (mm/dd/yyyy, time) or (mm/dd/yyyy, None) if no time is present

    raw_text = clean_text(raw_text)
    if not raw_text:
        return None, None
    
    cleaned = raw_text.strip()
    for suffix in ["ESTIMATE", "EST", "ACTUAL"]:
        if cleaned.upper().endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
    
    try:
        dt = datetime.strptime(cleaned, "%a, %b %d, %Y, %I:%M %p")
        return dt.strftime("%m/%d/%Y"), _fmt_time_portable(dt)
    except ValueError:
        pass
    
    try:
        dt = datetime.strptime(cleaned, "%b %d, %Y")
        return dt.strftime("%m/%d/%Y"), None
    except ValueError:
        pass
    
    return cleaned, None
    
def parse_est_act(values: list[str]) -> dict[str, dict[str, Optional[str]]]:

    if not values:
        return {"est": {"value": None, "unit": None},
                "act": {"value": None, "unit": None}}
    
    toks = [_parse_one_value(v) for v in values[:2]]
    
    if len(toks) == 1:
        t = toks[0]
        if t["is_est"] is True:
            return {
                "est": {"value": t["value"], "unit": t["unit"]},
                "act": {"value": None, "unit": None},
            }
        
        return {
            "est": {"value": None, "unit": None},
            "act": {"value": t["value"], "unit": t["unit"]},
        }
    
    a, b = toks[0], toks[1]
    if a["is_est"] is True or b["is_est"] is True or a["is_est"] is False or b["is_est"] is False:
        est = a if a["is_est"] is True else (b if b["is_est"] is True else None)
        act = a if a["is_est"] is False else (b if b["is_est"] is False else None)

        if est is None:
            est = b if a is act else a
        if act is None:
            act = a if b is est else b
    else:
        est, act = a, b

    return {
        "est": {"value": est["value"], "unit": est["unit"]},
        "act": {"value": act["value"], "unit": act["unit"]},
    }

def determine_status(percentage: Optional[float], actual: Optional[float], estimate: Optional[float]) -> str:
    if percentage is not None:
        return "Beat" if percentage >= 0 else "Miss"
    if actual is not None and estimate is not None:
        try:
            return "Beat" if float(actual) >= float(estimate) else "Miss"
        except Exception:
            return None
    return None

def parse_indicator(raw: str | None) -> str | None:

    raw = clean_text(raw)
    if not raw:
        return None

    m = re.search(r"\b(\d{1,2})\s*(Months?|Mos?|M)\b", raw, flags=re.I)
    if m:
        n = m.group(1)
        return f"{int(n)} Months"

    m = re.search(r"\b(\d{1,2})\b", raw)
    if m:
        return f"{int(m.group(1))} Months"

    return None

def extract_percentage_from_item(item: Tag) -> Optional[float]:

    if item is None:
        return None
    ps = item.find_all("p", class_=lambda x: x and (
        "css-1oq66ka" in x or 
        "css-1ica283" in x or 
        "css-1e9w60g" in x
    ))
    for p in ps:
        t = p.get_text(strip=True)
        if "%" in t:
            try:
                return float(t.replace("%", "").replace(",", "").strip())
            except ValueError:
                continue
    return None

def drop_duplicate_history(csv_path: str, out_path: str = None) -> str:
    if out_path is None:
        out_path = csv_path
    
    df = pd.read_csv(csv_path, index_col=0)
    df = df.drop_duplicates(subset=["quarter", "date", "time"], keep="first")
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    df.index.name = "index"
    
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(out_path, index=True)
    
    return out_path

def parse_percent(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    t = s.strip().replace("%", "").replace(",", "")

    try:
        return float(t)
    except ValueError:
        return None