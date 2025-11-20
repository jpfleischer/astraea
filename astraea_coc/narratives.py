
from __future__ import annotations
import re
from .utils import scrub_boilerplate

def extract_narrative_after_limit(
    lines: list[str],
    start_patterns: list[str],
    stop_patterns: list[str],
    keep_paragraphs: bool = True,
) -> str:
    """
    Extract narrative text (e.g., 1C-5e/5f) that may span pages.
    Starts after '(limit ... characters)' when present, scrubs boilerplate anywhere,
    stops at the next subsection header/title, preserves paragraph breaks.
    """
    text = "\n".join(lines)

    # 1) Start (numeric or title)
    starts = [re.search(p, text, flags=re.IGNORECASE | re.MULTILINE) for p in start_patterns]
    starts = [m for m in starts if m]
    if not starts:
        return ""

    start_match = min(starts, key=lambda m: m.start())
    tail = text[start_match.end():]

    # 2) Prefer after '(limit ... characters)'
    limit_rx = re.compile(r"^\s*\(?\s*limit\s*[,\s]*\d{1,2}(?:,\d{3})?\s*characters\)?\.?\s*$",
                          re.IGNORECASE | re.MULTILINE)
    m_limit = limit_rx.search(tail)
    if m_limit:
        tail = tail[m_limit.end():]
        m_blank = re.search(r"^\s*$", tail, flags=re.MULTILINE)
        if m_blank:
            tail = tail[m_blank.end():]
    else:
        prompt_rx = re.compile(r"^Describe\s+in\s+the\s+field\s+below.*?$", re.IGNORECASE | re.MULTILINE)
        m_prompt = prompt_rx.search(tail)
        if m_prompt:
            tail2 = tail[m_prompt.end():]
            m_blank2 = re.search(r"^\s*$", tail2, flags=re.MULTILINE)
            if m_blank2:
                tail = tail2[m_blank2.end():]

    # 3) Stop at next subsection header/title
    stops = [re.search(p, tail, flags=re.IGNORECASE | re.MULTILINE) for p in stop_patterns]
    stops = [m for m in stops if m]
    if stops:
        stop_pos = min(m.start() for m in stops)
        tail = tail[:stop_pos]

    # 4) Scrub boilerplate anywhere (handles page breaks)
    tail = scrub_boilerplate(tail)

    # 5) Tidy whitespace
    if keep_paragraphs:
        tail = re.sub(r"[ \t]+", " ", tail)
        tail = re.sub(r"\n{3,}", "\n\n", tail)
        tail = tail.strip()
    else:
        tail = re.sub(r"\s+", " ", tail).strip()

    return tail
