
from __future__ import annotations
from pathlib import Path

def extract_pdf_text(pdf_path: Path) -> tuple[str, int, str]:
    pages_text, engine = [], None
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for pg in pdf.pages:
                pages_text.append(pg.extract_text() or "")
        engine = "pdfplumber"
        n_pages = len(pages_text)
    except Exception as e:
        print(f"[info] pdfplumber failed: {e}\n[info] falling back to PyPDF2…")
        import PyPDF2
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            for p in r.pages:
                pages_text.append(p.extract_text() or "")
        engine = "PyPDF2"
        n_pages = len(pages_text)

    joined = []
    for i, t in enumerate(pages_text, start=1):
        joined.append(f"\n\n=== [PAGE {i}/{n_pages}] ===\n\n{t}")
    return "".join(joined), n_pages, engine

def split_pages_by_markers(text: str):
    import re
    parts = re.split(r"\n=== \[PAGE (\d+)/(\d+)\] ===\n", text)
    if len(parts) <= 1:
        return [(1, text)]
    pages = []
    for i in range(1, len(parts), 3):
        pno = int(parts[i]); body = parts[i+2]
        pages.append((pno, body))
    return pages
