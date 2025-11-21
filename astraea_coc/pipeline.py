from __future__ import annotations
from pathlib import Path
import re as _re
import pandas as pd

from .io_extract import extract_pdf_text, split_pages_by_markers
from .meta import parse_1a_metadata
from .slicer import slice_section_lines
from .parsers import (
    parse_numbered_yesno,
)

from .build_wide import build_wide, col_order_extended
from .utils import ts, save_text_unique, save_csv_unique

from .generic_parse import parse_tables, parse_narratives
from .specs_2024 import TABLE_SPECS_2024, NARR_SPECS_2024
from .custom_blocks import (
    custom_1c7d, custom_1c7e,
    custom_1d2, custom_1d5, custom_1d9, custom_1d10a,
)



def parse_1e(pages) -> dict[str, str]:
    out: dict[str, str] = {}

    yesno_rx = _re.compile(r"\b(Yes|No)\b", _re.IGNORECASE)
    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

    def _extract_narrative_block(lines: list[str]) -> str:
        """
        Narrative extractor that avoids including the HUD prompt/bullets.

        Behavior:
        - Ignore boilerplate headers/footers.
        - After we see "Describe in the field below", we enter PROMPT mode and
          skip everything until:
            a) the "(limit 2,500 characters)" line, OR
            b) a blank line after prompt text (OCR sometimes drops the limit line).
        - Only then start capturing the real narrative.
        """
        header_skip = _re.compile(
            r"^\s*(NOFO Section|Applicant:|Project:|FY20\d{2}\s+CoC Application Page|Page\s+\d+)",
            _re.IGNORECASE,
        )
        limit_line = _re.compile(r"limit\s*2,?500\s*characters", _re.IGNORECASE)
        describe_line = _re.compile(r"Describe\s+in\s+the\s+field\s+below", _re.IGNORECASE)

        state = "seek_anchor"  # seek_anchor -> in_prompt -> in_answer
        prompt_seen = False
        buf: list[str] = []

        for ln in lines:
            if header_skip.search(ln):
                continue

            stripped = ln.strip()

            if state == "seek_anchor":
                # Wait until we hit Describe or limit (some PDFs jump straight to limit)
                if describe_line.search(ln):
                    state = "in_prompt"
                    prompt_seen = True
                    continue
                if limit_line.search(ln):
                    state = "in_answer"
                    continue
                # If no anchor yet, ignore
                continue

            if state == "in_prompt":
                # Prompt ends at limit line...
                if limit_line.search(ln):
                    state = "in_answer"
                    continue
                # ...or a blank line after we've seen prompt text.
                if not stripped and prompt_seen:
                    state = "in_answer"
                    continue
                # Otherwise still prompt text—skip it
                if stripped:
                    prompt_seen = True
                continue

            # state == "in_answer"
            if not stripped:
                buf.append("")          # keep paragraph breaks
            else:
                buf.append(ln.rstrip())

        return "\n".join(buf).strip()

    def _map_yesno_df(df, prefix: str, n_items: int):
        """
        df from parse_numbered_yesno() has columns: index, value
        """
        for i in range(1, n_items + 1):
            val = ""
            if not df.empty:
                row = df[df["index"] == i]
                if not row.empty:
                    val = str(row["value"].iloc[0]).strip()
            out[f"{prefix}{i}"] = val

    # -------------------------
    # 1E-1: two dates
    # -------------------------
    lines_1e1 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]1\.\s*Web Posting of Advance Public Notice"],
        stop_patterns=[r"^\s*1E[-–]2\."],
        safety_pages_ahead=2,
    )
    text_1e1 = "\n".join(lines_1e1)
    dates = date_rx.findall(text_1e1)
    out["val_1e_1_1"] = dates[0] if len(dates) > 0 else ""
    out["val_1e_1_2"] = dates[1] if len(dates) > 1 else ""

    # -------------------------
    # 1E-2: yes/no chart (1..6)
    # -------------------------
    lines_1e2 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]2\.\s*Project Review and Ranking Process"],
        stop_patterns=[r"^\s*1E[-–]2a\."],
        safety_pages_ahead=2,
    )
    df_1e2 = parse_numbered_yesno(lines_1e2)
    _map_yesno_df(df_1e2, prefix="val_1e_2_", n_items=6)

    # -------------------------
    # 1E-2a: three detail fields
    # -------------------------
    lines_1e2a = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]2a\.\s*Scored Project Forms"],
        stop_patterns=[r"^\s*1E[-–]2b\."],
        safety_pages_ahead=2,
    )
    text_1e2a = "\n".join(lines_1e2a)

    m1 = _re.search(
        r"maximum number of points available.*?\?\s*([0-9]+)",
        text_1e2a, flags=_re.IGNORECASE | _re.DOTALL
    )
    out["val_1e_2a_1"] = m1.group(1) if m1 else ""

    m2 = _re.search(
        r"How many renewal projects did your CoC submit.*?\?\s*([0-9]+)",
        text_1e2a, flags=_re.IGNORECASE | _re.DOTALL
    )
    out["val_1e_2a_2"] = m2.group(1) if m2 else ""

    m3 = _re.search(
        r"What renewal project type did most applicants use\?\s*([A-Za-z0-9\-/ ]+)",
        text_1e2a, flags=_re.IGNORECASE
    )
    out["val_1e_2a_3"] = m3.group(1).strip() if m3 else ""

    # -------------------------
    # 1E-2b: narrative
    # -------------------------
    lines_1e2b = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]2b\.\s*Addressing Severe Barriers"],
        stop_patterns=[r"^\s*1E[-–]3\."],
        safety_pages_ahead=3,
    )
    out["narr_1e_2b"] = _extract_narrative_block(lines_1e2b)

    # -------------------------
    # 1E-3: narrative
    # -------------------------
    lines_1e3 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]3\.\s*Advancing Racial Equity"],
        stop_patterns=[r"^\s*1E[-–]4\."],
        safety_pages_ahead=3,
    )
    out["narr_1e_3"] = _extract_narrative_block(lines_1e3)

    # -------------------------
    # 1E-4: narrative
    # -------------------------
    lines_1e4 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]4\.\s*Reallocation"],
        stop_patterns=[r"^\s*1E[-–]4a\."],
        safety_pages_ahead=3,
    )
    out["narr_1e_4"] = _extract_narrative_block(lines_1e4)

    # -------------------------
    # 1E-4a: yes/no single value
    # -------------------------
    lines_1e4a = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]4a\.\s*Reallocation Between"],
        stop_patterns=[r"^\s*1E[-–]5\."],
        safety_pages_ahead=2,
    )
    val_1e_4a = ""
    for ln in reversed(lines_1e4a):
        m = yesno_rx.search(ln)
        if m:
            val_1e_4a = m.group(1).title()
            break
    out["val_1e_4a"] = val_1e_4a

    # -------------------------
    # 1E-5: yes/no chart (1..4)
    # -------------------------
    lines_1e5 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*1E[-–]5\.\s*Projects Rejected/Reduced"],
        stop_patterns=[r"^\s*1E[-–]5a\."],
        safety_pages_ahead=2,
    )
    df_1e5 = parse_numbered_yesno(lines_1e5)
    _map_yesno_df(df_1e5, prefix="val_1e_5_", n_items=4)

    # -------------------------
    # 1E-5a..5d: narratives
    # -------------------------
    def _slice_5(letter: str, next_letter: str | None):
        start = [rf"^\s*1E[-–]5{letter}\.\s*"]
        stop = [rf"^\s*1E[-–]5{next_letter}\.\s*"] if next_letter else [r"^\s*2A[-–]1\."]
        return slice_section_lines(pages, start_patterns=start, stop_patterns=stop, safety_pages_ahead=2)

    lines_1e5a = _slice_5("a", "b")
    lines_1e5b = _slice_5("b", "c")
    lines_1e5c = _slice_5("c", "d")
    lines_1e5d = _slice_5("d", None)

    out["narr_1e_5a"] = _extract_narrative_block(lines_1e5a)
    out["narr_1e_5b"] = _extract_narrative_block(lines_1e5b)
    out["narr_1e_5c"] = _extract_narrative_block(lines_1e5c)
    out["narr_1e_5d"] = _extract_narrative_block(lines_1e5d)

    return out



def run_all(pdf_path: Path, out_dir: Path | None = None) -> dict:
    pdf_path = Path(pdf_path).resolve()
    if out_dir is None:
        out_dir = pdf_path.parent

    full_text, n_pages, engine = extract_pdf_text(pdf_path)
    txt_path = save_text_unique(
        out_dir / f"{pdf_path.stem}__text_{ts()}.txt", full_text
    )
    pages = split_pages_by_markers(full_text)

    # 1A metadata
    meta_vals, meta_debug = parse_1a_metadata(pages)

    # Spec-driven simple tables + narratives
    section_data: dict[str, object] = {}
    section_data.update(parse_tables(pages, TABLE_SPECS_2024))

    # inject meta into the tables that used to have it
    for k in ["df_1b1", "df_1c1", "df_1c2"]:  # add others if needed
        df = section_data.get(k)
        if isinstance(df, pd.DataFrame) and not df.empty:
            for mk, mv in meta_vals.items():
                df[mk] = mv
            section_data[k] = df
    section_data.update(parse_narratives(pages, NARR_SPECS_2024))

    # Custom blocks (special logic)
    section_data.update(custom_1c7d(pages))
    section_data.update(custom_1c7e(pages))
    section_data.update(custom_1d2(pages))
    section_data.update(custom_1d5(pages))
    section_data.update(custom_1d9(pages))
    section_data.update(custom_1d10a(pages))

    # 1E stays custom for now: keep your existing parse_1e(pages)
    section_data.update(parse_1e(pages))

    # Build wide
    wide_df = build_wide(meta_vals=meta_vals, **section_data)

    ordered = col_order_extended()
    extras = [c for c in wide_df.columns if c not in ordered]
    wide_df = wide_df.reindex(columns=ordered + extras)
    save_csv_unique(out_dir / f"{pdf_path.stem}__wide.csv", wide_df)


    result: dict[str, object] = {
        "txt_path": txt_path,
        "meta_vals": meta_vals,
        "wide_df": wide_df,
    }
    result.update(section_data)
    return result
