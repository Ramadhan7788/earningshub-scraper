from dataclasses import dataclass
from datetime import datetime , date
from typing import Optional

@dataclass
class Earnings:
    id: Optional[int]
    ticker: str
    quarter: Optional[str]
    report_date: date
    rev_est: Optional[float] = None
    rev_est_unit: Optional[str] = None
    rev_act: Optional[float] = None
    rev_act_unit: Optional[str] = None
    rev_pct: Optional[float] = None
    rev_status: Optional[str] = None
    eps_est: Optional[float] = None
    eps_act: Optional[float] = None
    eps_pct: Optional[float] = None
    eps_status: Optional[str] = None
    created_at: Optional[datetime] = None