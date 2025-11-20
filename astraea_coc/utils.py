
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
import re

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def unique_path(base: Path) -> Path:
    p = base
    k = 0
    while p.exists():
        k += 1
        p = base.with_name(base.stem + f"_{k}" + base.suffix)
    return p

def save_text_unique(path_base: Path, text: str) -> Path:
    out = unique_path(path_base)
    out.write_text(text, encoding="utf-8")
    return out

def save_csv_unique(path_base: Path, df: pd.DataFrame) -> Path:
    out = unique_path(path_base)
    df.to_csv(out, index=False)
    return out

def norm_token(x):
    if not isinstance(x, str): return x
    k = x.strip().lower()
    m = {
        "yes": "Yes",
        "no": "No",
        "nonexistent": "Nonexistent",
        "non-existent": "Nonexistent",
        "does not exist": "Nonexistent",
        "n/a": "Nonexistent",
        "na": "Nonexistent",
        "not applicable": "Nonexistent",
    }
    return m.get(k, x.strip().title())

# Global boilerplate patterns for HUD forms (scrub anywhere)
BOILERPLATE_PATTERNS = [
    r"^NOFO Section.*?$",
    r"^Describe in the field below.*?$",
    r"^\(?\s*limit\s*[,\s]*\d{1,2}(?:,\d{3})?\s*characters\)?\.?\s*$",
    r"^FY\d{4}\s+CoC Application.*?$",
    r"^Applicant:.*?$",
    r"^Project:.*?$",
    r"^Page\s+\d+.*?$",
]

def scrub_boilerplate(text: str) -> str:
    for bp in BOILERPLATE_PATTERNS:
        text = re.sub(bp, "", text, flags=re.IGNORECASE | re.MULTILINE)
    return text
