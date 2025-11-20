
from __future__ import annotations
import re
from typing import Sequence

def find_first(pattern: str, pages: Sequence[tuple[int, str]], flags=re.IGNORECASE|re.MULTILINE):
    rx = re.compile(pattern, flags)
    for pno, body in pages:
        m = rx.search(body)
        if m:
            val = m.group(1).strip() if m.lastindex else m.group(0).strip()
            return {"value": val, "page": pno, "match": m.group(0).strip()}
    return None

def parse_1a_metadata(pages: Sequence[tuple[int, str]]):
    meta = {}
    meta["coc_number"] = find_first(r"CoC\s+(?:Number|ID)\s*[:\-]\s*([A-Z]{2}\-\d{3})", pages)         or find_first(r"Applicant:\s*(?:.+?)\s+([A-Z]{2}\-\d{3})\b", pages)
    meta["coc_name"]   = find_first(r"CoC\s+(?:Name|Title)\s*[:\-]\s*([^\n]+?)\s*(?:\r?\n|$)", pages)         or find_first(r"Applicant:\s*([A-Za-z0-9 ,&'()\-/]+?)\s+[A-Z]{2}\-\d{3}\b", pages)
    meta["collab_app"] = find_first(r"Collaborative Applicant(?:\s*Name)?\s*[:\-]\s*([^\n]+?)\s*(?:\r?\n|$)", pages)
    meta["designation"]= find_first(r"CoC\s+Designation\s*[:\-]\s*([A-Z]{1,3})", pages)
    meta["hmis_lead"]  = find_first(r"HMIS\s+Lead\s*[:\-]\s*([^\n]+?)\s*(?:\r?\n|$)", pages)
    meta_vals = {k: (v["value"] if v else "") for k, v in meta.items()}
    return meta_vals, meta
