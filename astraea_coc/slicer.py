
from __future__ import annotations
import re
from typing import Sequence

def slice_section_lines(pages: Sequence[tuple[int, str]], start_patterns, stop_patterns, safety_pages_ahead=3):
    def find_on_pages(pats):
        for pat in pats:
            rx = re.compile(pat, re.IGNORECASE | re.MULTILINE)
            for pno, body in pages:
                m = rx.search(body)
                if m:
                    return {"page": pno, "match": m.group(0)}
        return None

    start = find_on_pages(start_patterns)
    assert start, f"Start anchor not found. Tried: {start_patterns}"
    stop = find_on_pages(stop_patterns)

    start_page = start["page"]
    stop_page = stop["page"] if (stop and stop["page"] >= start_page) else start_page + safety_pages_ahead

    stitched = "\n".join([b for (p, b) in pages if start_page <= p <= stop_page])

    m_start = re.search(re.escape(start["match"]), stitched, re.IGNORECASE)
    block = stitched[m_start.end():] if m_start else stitched

    if stop:
        m_stop = re.search(re.escape(stop["match"].splitlines()[0]), block, re.IGNORECASE)
        if m_stop:
            block = block[:m_stop.start()]

    norm = [re.sub(r"\s+", " ", ln).strip() for ln in block.splitlines()]
    return [ln for ln in norm if ln]
