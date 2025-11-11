import json
from pathlib import Path
import pandas as pd

def write_json(data: dict, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

def write_csv(df: pd.DataFrame, out_path: Path, *, index: bool = False) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=index)
    return out_path
