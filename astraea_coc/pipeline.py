from __future__ import annotations
from pathlib import Path
import re as _re
import pandas as pd

from .io_extract import extract_pdf_text, split_pages_by_markers
from .meta import parse_1a_metadata
from .slicer import slice_section_lines
from .parsers import (
    parse_triple_table,
    parse_numbered_yesno,
    parse_numbered_dual_tokens,
    parse_1c7_pha,
)

from .narratives import extract_narrative_after_limit
from .build_wide import build_wide, col_order_extended
from .utils import ts, save_text_unique, save_csv_unique


def parse_1b(pages, meta_vals, pdf_path: Path, out_dir: Path) -> dict:
    """
    Parse section 1B (inclusive structure + narratives).
    Returns a dict suitable for passing into build_wide().
    """
    # 1B-1 slice/parse
    start_1b1 = [
        r"^\s*1B[-–]1\.\s*Inclusive\s+Structure\s+and\s+Participation.*?$",
        r"\bInclusive Structure and Participation\b",
    ]
    stop_1b1 = [
        r"^\s*1B[-–]1a\.",
        r"^\s*1B[-–]2\.",
        r"^\s*1C[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1b1 = slice_section_lines(pages, start_1b1, stop_1b1, safety_pages_ahead=3)
    df_1b1 = parse_triple_table(lines_1b1)
    if not df_1b1.empty:
        cols = [
            "coc_number",
            "coc_name",
            "collab_app",
            "designation",
            "hmis_lead",
            "org_type_index",
            "org_type",
            "meetings",
            "voted",
            "ces",
        ]
        for k, v in meta_vals.items():
            df_1b1[k] = v
        df_1b1 = df_1b1[cols]
        save_csv_unique(out_dir / f"{pdf_path.stem}__1b1_parsed_{ts()}.csv", df_1b1)

    # 1B-1a narrative (Experience Promoting Racial Equity)
    start_1b1a_anchor = [
        r"^\s*1B[-–]1a\.\s*Experience\s+Promoting\s+Racial\s+Equity.*$",
        r"^\s*1B[-–]1a\.\s*",
    ]
    stop_1b1a_anchor = [
        r"^\s*1B[-–]1b\.",
        r"^\s*1B[-–]2\.",
        r"^\s*1C[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1b1a = slice_section_lines(
        pages, start_1b1a_anchor, stop_1b1a_anchor, safety_pages_ahead=3
    )

    start_1b1a_narr = [
        r"^NOFO\s+Section\s+III\.B\.3\.c\.",
        r"^Describe\s+in\s+the\s+field\s+below.*$",
    ]

    narr_1b1a = extract_narrative_after_limit(
        lines_1b1a,
        start_patterns=start_1b1a_narr,
        stop_patterns=stop_1b1a_anchor,
        keep_paragraphs=True,
    )

    # 1B-2 narrative (Open Invitation for New Members)
    start_1b2_anchor = [
        r"^\s*1B[-–]2\.\s*Open\s+Invitation\s+for\s+New\s+Members.*$",
        r"^\s*1B[-–]2\.\s*",
    ]
    stop_1b2_anchor = [
        r"^\s*1B[-–]3\.",
        r"^\s*1C[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1b2 = slice_section_lines(
        pages, start_1b2_anchor, stop_1b2_anchor, safety_pages_ahead=3
    )

    start_1b2_narr = [
        r"^NOFO\s+Section\s+V\.B\.1\.a\.\(2\)",
        r"^Describe\s+in\s+the\s+field\s+below.*$",
    ]
    narr_1b2 = extract_narrative_after_limit(
        lines_1b2,
        start_patterns=start_1b2_narr,
        stop_patterns=stop_1b2_anchor,
        keep_paragraphs=True,
    )

    # 1B-3 narrative (CoC’s Strategy to Solicit/Consider Opinions...)
    start_1b3_anchor = [
        r"^\s*1B[-–]3\.\s*CoC['’]s\s+Strategy\s+to\s+Solicit/Consider\s+Opinions.*$",
        r"^\s*1B[-–]3\.\s*",
    ]
    stop_1b3_anchor = [
        r"^\s*1B[-–]4\.",
        r"^\s*1C[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1b3 = slice_section_lines(
        pages, start_1b3_anchor, stop_1b3_anchor, safety_pages_ahead=3
    )

    start_1b3_narr = [
        r"^NOFO\s+Section\s+V\.B\.1\.a\.\(3\)",
        r"^Describe\s+in\s+the\s+field\s+below.*$",
    ]
    narr_1b3 = extract_narrative_after_limit(
        lines_1b3,
        start_patterns=start_1b3_narr,
        stop_patterns=stop_1b3_anchor,
        keep_paragraphs=True,
    )

    # 1B-4 narrative (Public Notification for Proposals from Orgs Not Previously Awarded...)
    start_1b4_anchor = [
        r"^\s*1B[-–]4\.\s*Public\s+Notification\s+for\s+Proposals\s+from\s+Organizations\s+Not\s+Previously\s+Awarded.*$",
        r"^\s*1B[-–]4\.\s*",
    ]
    stop_1b4_anchor = [
        r"^\s*1B[-–]5\.",
        r"^\s*1C\.\s*Coordination\s+and\s+Engagement.*$",
        r"^\s*1C[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]

    lines_1b4 = slice_section_lines(
        pages, start_1b4_anchor, stop_1b4_anchor, safety_pages_ahead=3
    )

    start_1b4_narr = [
        r"^NOFO\s+Section\s+V\.B\.1\.a\.\(4\)",
        r"^Describe\s+in\s+the\s+field\s+below.*$",
    ]
    narr_1b4 = extract_narrative_after_limit(
        lines_1b4,
        start_patterns=start_1b4_narr,
        stop_patterns=stop_1b4_anchor,
        keep_paragraphs=True,
    )

    return {
        "df_1b1": df_1b1,
        "narr_1b1a": narr_1b1a,
        "narr_1b2": narr_1b2,
        "narr_1b3": narr_1b3,
        "narr_1b4": narr_1b4,
    }


def parse_1c(pages, meta_vals, pdf_path: Path, out_dir: Path) -> dict:
    """
    Parse section 1C (coordination & engagement, tables + narratives).
    """
    # 1C-1
    start_1c1 = [r"^\s*1C[-–]1\.\s?.*$", r"\b1C[-–]1\.\b"]
    stop_1c1 = [r"^\s*1C[-–]2\.", r"^\s*1D[-–]1\.", r"^\s*2A[-–]1\."]
    lines_1c1 = slice_section_lines(pages, start_1c1, stop_1c1, safety_pages_ahead=3)
    df_1c1 = parse_numbered_yesno(lines_1c1)
    if not df_1c1.empty:
        for k, v in meta_vals.items():
            df_1c1[k] = v
        df_1c1 = df_1c1[
            [
                "coc_number",
                "coc_name",
                "collab_app",
                "designation",
                "hmis_lead",
                "index",
                "label",
                "value",
            ]
        ]
        save_csv_unique(out_dir / f"{pdf_path.stem}__1c1_parsed_{ts()}.csv", df_1c1)

    # 1C-2
    start_1c2 = [r"^\s*1C[-–]2\.\s?.*$", r"\b1C[-–]2\.\b"]
    stop_1c2 = [r"^\s*1C[-–]3\.", r"^\s*1D[-–]1\.", r"^\s*2A[-–]1\."]
    lines_1c2 = slice_section_lines(pages, start_1c2, stop_1c2, safety_pages_ahead=2)
    df_1c2 = parse_numbered_yesno(lines_1c2)
    if not df_1c2.empty:
        for k, v in meta_vals.items():
            df_1c2[k] = v
        df_1c2 = df_1c2[
            [
                "coc_number",
                "coc_name",
                "collab_app",
                "designation",
                "hmis_lead",
                "index",
                "label",
                "value",
            ]
        ]
        save_csv_unique(out_dir / f"{pdf_path.stem}__1c2_parsed_{ts()}.csv", df_1c2)

    # 1C-3
    start_1c3 = [r"^\s*1C[-–]3\.\s?.*$", r"\b1C[-–]3\.\b"]
    stop_1c3 = [r"^\s*1C[-–]4\.", r"^\s*1D[-–]1\.", r"^\s*2A[-–]1\."]
    lines_1c3 = slice_section_lines(pages, start_1c3, stop_1c3, safety_pages_ahead=2)
    df_1c3 = parse_numbered_yesno(lines_1c3)

    # 1C-4
    start_1c4 = [
        r"^\s*1C[-–]4\.\s?.*$",
        r"\b1C[-–]4\.\b",
    ]
    stop_1c4 = [
        r"^\s*1C[-–]5\.",
        r"^\s*1C\.\s*Coordination\s+and\s+Engagement",
        r"^\s*1C[-–]1\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]

    lines_1c4 = slice_section_lines(pages, start_1c4, stop_1c4, safety_pages_ahead=3)
    df_1c4_basic = parse_numbered_yesno(lines_1c4)
    df_1c4c = parse_numbered_dual_tokens(lines_1c4, suffixes=("mou", "oth"))

    # 1C-4a narrative – formal partnerships
    start_1c4a_narr = [
        r"^Describe\s+in\s+the\s+field\s+below\s+the\s+formal\s+partnerships\s+your\s+CoC\s+has\s+with\s+at\s+least\s+one\s+of\s+the\s+entities.*$",
    ]
    stop_1c4a_narr = [
        r"^\s*(?:1C|C)[-–]4b\.",
        r"^\s*1C[-–]5\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_1c4a = extract_narrative_after_limit(
        lines_1c4,
        start_patterns=start_1c4a_narr,
        stop_patterns=stop_1c4a_narr,
        keep_paragraphs=True,
    )

    # 1C-4b narrative – informing about educational services
    start_1c4b_narr = [
        r"^\s*(?:1C|C)[-–]4b\.\s*Informing\s+Individuals\s+and\s+Families.*$",
        r"^Describe\s+in\s+the\s+field\s+below\s+written\s+policies\s+and\s+procedures\s+your\s+CoC\s+uses\s+to\s+inform\s+individuals.*$",
    ]
    stop_1c4b_narr = [
        r"^\s*(?:1C|C)[-–]4c\.",
        r"^\s*1C[-–]5\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_1c4b = extract_narrative_after_limit(
        lines_1c4,
        start_patterns=start_1c4b_narr,
        stop_patterns=stop_1c4b_narr,
        keep_paragraphs=True,
    )

    # 1C-5
    start_1c5 = [r"^\s*1C[-–]5\.\s?.*$", r"\b1C[-–]5\.\b"]
    stop_1c5 = [r"^\s*1C[-–]6\.", r"^\s*1D[-–]1\.", r"^\s*2A[-–]1\."]
    lines_1c5 = slice_section_lines(pages, start_1c5, stop_1c5, safety_pages_ahead=3)
    df_1c5_basic = parse_numbered_yesno(lines_1c5)
    df_1c5c = parse_numbered_dual_tokens(lines_1c5, suffixes=("proj", "ces"))

    # 1C-5a narrative – collaborating with federally funded programs / VSPs
    start_1c5a_narr = [
        r"^\s*1C[-–]?5a\.\s*Collaborating\s+with\s+Federally\s+Funded\s+Programs.*$",
        r"^Describe\s+in\s+the\s+field\s+below\s+how\s+your\s+CoC\s+regularly\s+collaborates.*$",
    ]
    stop_1c5a_narr = [
        r"^\s*1C[-–]?5b\.",
        r"^\s*1C[-–]?5c\.",
        r"^\s*1C[-–]6\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_1c5a = extract_narrative_after_limit(
        lines_1c5,
        start_patterns=start_1c5a_narr,
        stop_patterns=stop_1c5a_narr,
        keep_paragraphs=True,
    )

    # 1C-5b narrative – safety & confidentiality in CE
    start_1c5b_narr = [
        r"^\s*1C[-–]?5b\.\s*Implemented\s+Safety\s+Planning.*$",
        r"^Describe\s+in\s+the\s+field\s+below\s+how\s+your\s+CoC['’]s\s+coordinated\s+entry\s+addresses\s+the\s+needs\s+of\s+DV\s+survivors.*$",
    ]
    stop_1c5b_narr = [
        r"^\s*1C[-–]?5c\.",
        r"^\s*1C[-–]6\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_1c5b = extract_narrative_after_limit(
        lines_1c5,
        start_patterns=start_1c5b_narr,
        stop_patterns=stop_1c5b_narr,
        keep_paragraphs=True,
    )

    # 1C-5d narrative – VAWA emergency transfer plan
    start_1c5d_narr = [
        r"^\s*1C[-–]?5d\.\s*Implemented\s+VAWA-Required\s+Written\s+Emergency\s+Transfer\s+Plan.*$",
        r"^Describe\s+in\s+the\s+field\s+below:.*$",
    ]
    stop_1c5d_narr = [
        r"^\s*1C[-–]?5e\.\s",
        r"^Facilitating\s+Safe\s+Access\s+to\s+Housing\s+and\s+Services\s+for\s+Survivors",
        r"^\s*1C[-–]6\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_1c5d = extract_narrative_after_limit(
        lines_1c5,
        start_patterns=start_1c5d_narr,
        stop_patterns=stop_1c5d_narr,
        keep_paragraphs=True,
    )

    # narratives 5e / 5f
    start_5e = [
        r"^\s*1C[-–]5e\.\s",
        r"^Facilitating\s+Safe\s+Access\s+to\s+Housing\s+and\s+Services\s+for\s+Survivors",
    ]
    stop_after_5e = [
        r"^\s*1C[-–]5f\.\s",
        r"^Identifying\s+and\s+Removing\s+Barriers\s+for\s+Survivors",
        r"^\s*1C[-–]6\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    start_5f = [
        r"^\s*1C[-–]5f\.\s",
        r"^Identifying\s+and\s+Removing\s+Barriers\s+for\s+Survivors",
    ]
    stop_after_5f = [
        r"^\s*1C[-–]6\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    narr_5e = extract_narrative_after_limit(
        lines_1c5, start_5e, stop_after_5e, keep_paragraphs=True
    )
    narr_5f = extract_narrative_after_limit(
        lines_1c5, start_5f, stop_after_5f, keep_paragraphs=True
    )

    # --- 1C-6. LGBTQ+ anti-discrimination yes/no + 1C-6a narrative ---

    # 1C-6 table (three yes/no items)
    start_1c6 = [
        r"^\s*1C[-–]6\.\s*Addressing\s+the\s+Needs\s+of\s+Lesbian,\s*Gay,\s*Bisexual,\s*Transgender\s+and\s+Queer\+.*$",
        r"^\s*1C[-–]6\.\s*",
    ]
    stop_1c6 = [
        r"^\s*1C[-–]6a\.",
        r"^\s*1C[-–]7\.",
        r"^\s*[1I]D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c6 = slice_section_lines(pages, start_1c6, stop_1c6, safety_pages_ahead=2)
    df_1c6 = parse_numbered_yesno(lines_1c6)

    # 1C-6a narrative
    start_1c6a = [
        r"^\s*1C[-–]6a\.\s*Anti-Discrimination\s+Policy.*$",
        r"^\s*1C[-–]6a\.\s*",
    ]
    stop_1c6a = [
        r"^\s*1C[-–]7\.",
        r"^\s*[1I]D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c6a = slice_section_lines(pages, start_1c6a, stop_1c6a, safety_pages_ahead=3)
    narr_1c6a = extract_narrative_after_limit(
        lines_1c6a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1c6a,
        keep_paragraphs=True,
    )

    # --- 1C-7 – PHA table ---
    start_1c7 = [
        r"^\s*1C[-–]7\.\s*Public\s+Housing\s+Agencies\s+within\s+Your\s+CoC",
        r"^\s*1C[-–]7\.\s*",
    ]
    stop_1c7 = [
        r"^\s*1C[-–]7a\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7 = slice_section_lines(pages, start_1c7, stop_1c7, safety_pages_ahead=2)
    df_1c7 = parse_1c7_pha(lines_1c7)
    if not df_1c7.empty:
        cols_1c7 = ["pha_name", "ph_hhm", "ph_limit_hhm", "psh"]
        save_csv_unique(out_dir / f"{pdf_path.stem}__1c7_parsed_{ts()}.csv", df_1c7[cols_1c7])

    # --- 1C-7a. Homeless admission preferences w/ PHAs ---
    start_1c7a = [
        r"^\s*1C[-–]7a\.\s*Written\s+Policies\s+on\s+Homeless\s+Admission\s+Preferences.*$",
        r"^\s*1C[-–]7a\.\s*",
    ]
    stop_1c7a = [
        r"^\s*1C[-–]7b\.",
        r"^\s*1C[-–]7c\.",
        r"^\s*[1I]D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7a = slice_section_lines(pages, start_1c7a, stop_1c7a, safety_pages_ahead=3)
    narr_1c7a = extract_narrative_after_limit(
        lines_1c7a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1c7a,
        keep_paragraphs=True,
    )

    # --- 1C-7b. Moving On Strategy with Affordable Housing Providers (Yes/No chart) ---
    start_1c7b = [
        r"^\s*1C[-–]7b\.\s*Moving\s+On\s+Strategy\s+with\s+Affordable\s+Housing\s+Providers.*$",
        r"^\s*1C[-–]7b\.\s*",
    ]
    stop_1c7b = [
        r"^\s*1C[-–]7c\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7b = slice_section_lines(pages, start_1c7b, stop_1c7b, safety_pages_ahead=2)
    df_1c7b = parse_numbered_yesno(lines_1c7b)

    # --- 1C-7c. Include Units from PHA Administered Programs in CoC CE (Yes/No chart) ---
    start_1c7c = [
        r"^\s*1C[-–]7c\.\s*Include\s+Units\s+from\s+PHA\s+Administered\s+Programs.*$",
        r"^\s*1C[-–]7c\.\s*",
    ]
    stop_1c7c = [
        r"^\s*1C[-–]7d\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7c = slice_section_lines(pages, start_1c7c, stop_1c7c, safety_pages_ahead=2)
    df_1c7c = parse_numbered_yesno(lines_1c7c)

    # --- 1C-7d. Joint CoC–PHA applications ---
    start_1c7d = [
        r"^\s*1C[-–]7d\.\s*Submitting\s+CoC\s+and\s+PHA\s+Joint\s+Applications.*$",
        r"^\s*1C[-–]7d\.\s*",
    ]
    stop_1c7d = [
        r"^\s*1C[-–]7e\.",
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7d = slice_section_lines(pages, start_1c7d, stop_1c7d, safety_pages_ahead=2)

    # 1C-7d(1): Yes/No to "Did your CoC coordinate with a PHA(s)...?"
    df_1c7d_yn = parse_numbered_yesno(lines_1c7d)
    val_1c7d_1 = ""
    if not df_1c7d_yn.empty:
        row = df_1c7d_yn[df_1c7d_yn["index"] == 1]
        if not row.empty:
            val_1c7d_1 = row["value"].iloc[0]

    # 1C-7d(2): program type (free text after "Program Funding Source / 2. Enter ...")
    narr_1c7d_2 = ""
    start_rx = _re.compile(
        r"^2\.\s*Enter\s+the\s+type\s+of\s+competitive\s+project\s+your\s+CoC\s+coordinated.*$",
        _re.IGNORECASE,
    )
    next_section_rx = _re.compile(
        r"^\s*1C[-–]7e\.|^\s*1D[-–]1\.|^\s*2A[-–]1\.|^\s*NOFO\s+Section",
        _re.IGNORECASE,
    )

    start_idx = None
    for i, ln in enumerate(lines_1c7d):
        if start_rx.search(ln):
            start_idx = i
            break

    if start_idx is not None:
        content_lines: list[str] = []
        for ln in lines_1c7d[start_idx + 1 :]:
            if next_section_rx.search(ln):
                break
            if not ln.strip():
                continue
            content_lines.append(ln)
        narr_1c7d_2 = "\n".join(content_lines).strip()

    # If they answered No to Q1, there is no competitive project,
    # so force the free-text field to be blank.
    if val_1c7d_1 != "Yes":
        narr_1c7d_2 = "Empty"

    # --- 1C-7e. Coordinating with PHA(s) to Apply for or Implement HCV/EHV ---
    start_1c7e = [
        r"^\s*1C[-–]7e\.\s*Coordinating\s+with\s+PHA\(s\)\s+to\s+Apply\s+for\s+or\s+Implement\s+HCV.*$",
        r"^\s*1C[-–]7e\.\s*",
    ]
    stop_1c7e = [
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7e = slice_section_lines(pages, start_1c7e, stop_1c7e, safety_pages_ahead=2)

    val_1c7e = ""
    yesno_rx = _re.compile(r"\b(Yes|No|Nonexistent)\b", _re.IGNORECASE)

    # Scan from bottom up and grab the last Yes/No/Nonexistent token
    for ln in reversed(lines_1c7e):
        m = yesno_rx.search(ln)
        if m:
            val_1c7e = m.group(1).title()
            break

    return {
        "df_1c1": df_1c1,
        "df_1c2": df_1c2,
        "df_1c3": df_1c3,
        "df_1c4_basic": df_1c4_basic,
        "df_1c4c": df_1c4c,
        "df_1c5_basic": df_1c5_basic,
        "df_1c5c": df_1c5c,
        "df_1c6": df_1c6,
        "df_1c7": df_1c7,
        "df_1c7b": df_1c7b,
        "df_1c7c": df_1c7c,
        "narr_1c4a": narr_1c4a,
        "narr_1c4b": narr_1c4b,
        "narr_1c5a": narr_1c5a,
        "narr_1c5b": narr_1c5b,
        "narr_1c5d": narr_1c5d,
        "narr_5e": narr_5e,
        "narr_5f": narr_5f,
        "narr_1c6a": narr_1c6a,
        "narr_1c7a": narr_1c7a,
        "val_1c7d_1": val_1c7d_1,
        "narr_1c7d_2": narr_1c7d_2,
        "val_1c7e": val_1c7e,
    }


def parse_1d(pages, meta_vals, pdf_path: Path, out_dir: Path) -> dict:
    """
    Parse section 1D (public systems, Housing First, racial equity, etc.).
    """
    # --- 1D-1. Preventing People Transitioning from Public Systems from Experiencing Homelessness ---
    start_1d1 = [
        r"^\s*1D[-–]1\.\s*Preventing\s+People\s+Transitioning\s+from\s+Public\s+Systems\s+from\s+Experiencing\s+Homelessness.*$",
        r"^\s*1D[-–]1\.\s*",
    ]
    stop_1d1 = [
        r"^\s*1D[-–]2\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d1 = slice_section_lines(pages, start_1d1, stop_1d1, safety_pages_ahead=2)
    df_1d1 = parse_numbered_yesno(lines_1d1)

    # --- 1D-2. Housing First – Lowering Barriers to Entry ---
    start_1d2 = [
        r"^\s*1D[-–]2\.\s*Housing\s+First[-–]\s*Lowering\s+Barriers\s+to\s+Entry.*$",
        r"^\s*1D[-–]2\.\s*",
    ]
    stop_1d2 = [
        r"^\s*1D[-–]2a\.",
        r"^\s*1D[-–]3\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d2 = slice_section_lines(pages, start_1d2, stop_1d2, safety_pages_ahead=2)

    def _grab_answer_after_question(lines: list[str], question_rx: "_re.Pattern", max_lookahead: int = 10) -> str:
        """
        Find the first number-like token (e.g., '13', '100%') on the lines
        immediately following a line matching question_rx.
        """
        for idx, ln in enumerate(lines):
            if question_rx.search(ln):
                # look ahead a few lines for the numeric answer
                for ln2 in lines[idx + 1 : idx + 1 + max_lookahead]:
                    # stop if we hit another numbered sub-question (1., 2., 3.)
                    if _re.match(r"^\s*[123]\.\s", ln2):
                        break
                    # stop if we accidentally wander into the next section
                    if _re.match(r"^\s*[1I]D[-–]2a\.", ln2) or _re.match(r"^\s*[1I]D[-–]3\.", ln2):
                        break

                    m = _re.search(r"\b(\d+%?)\b", ln2)
                    if not m:
                        continue
                    tok = m.group(1)

                    # Skip years (e.g., 2024) unless they are explicitly percentages
                    if not tok.endswith("%"):
                        try:
                            num = int(tok)
                        except ValueError:
                            continue
                        if 1900 <= num <= 2100:
                            continue

                    return tok
                break
        return ""

    # Match the exact HUD prompt wording as anchors
    q1_rx = _re.compile(r"^\s*1\.\s*Enter\s+the\s+total\s+number", _re.IGNORECASE)
    q2_rx = _re.compile(r"^\s*2\.\s*Enter\s+the\s+total\s+number", _re.IGNORECASE)
    q3_rx = _re.compile(r"^\s*3\.\s*This\s+number\s+is\s+a\s+calculation", _re.IGNORECASE)

    val_1d2_1 = _grab_answer_after_question(lines_1d2, q1_rx)
    val_1d2_2 = _grab_answer_after_question(lines_1d2, q2_rx)
    val_1d2_3 = _grab_answer_after_question(lines_1d2, q3_rx)

    # --- fallback: if for some reason we didn't get all 3, use the generic numeric scan ---
    if not (val_1d2_1 and val_1d2_2 and val_1d2_3):
        nums_1d2: list[str] = []
        skip_enums = {"1", "2", "3"}

        for ln in lines_1d2:
            # find all number-ish tokens, e.g. "13", "100%"
            for tok in _re.findall(r"\b\d+%?\b", ln):
                if tok in skip_enums:
                    continue  # skip the question numbers 1, 2, 3

                # strip '%' for numeric checks
                try:
                    num = int(tok.rstrip("%"))
                except ValueError:
                    continue

                # if it looks like a year (1900–2100) and is NOT a percentage, skip it
                if not tok.endswith("%") and 1900 <= num <= 2100:
                    continue

                nums_1d2.append(tok)
                if len(nums_1d2) == 3:
                    break
            if len(nums_1d2) == 3:
                break

        val_1d2_1 = val_1d2_1 or (nums_1d2[0] if len(nums_1d2) >= 1 else "")
        val_1d2_2 = val_1d2_2 or (nums_1d2[1] if len(nums_1d2) >= 2 else "")
        val_1d2_3 = val_1d2_3 or (nums_1d2[2] if len(nums_1d2) >= 3 else "")

    # ensure 1D-2(3) is stored as a percentage string
    if val_1d2_3 and not val_1d2_3.endswith("%"):
        val_1d2_3 = f"{val_1d2_3}%"


    # --- 1D-2a. Housing First evaluation narrative ---
    start_1d2a = [
        r"^\s*[1I]D[-–]2a\.\s*Project\s+Evaluation\s+for\s+Housing\s+First\s+Compliance.*$",
        r"^\s*[1I]D[-–]2a\.\s*",
    ]
    stop_1d2a = [
        r"^\s*[1I]D[-–]3\.",
        r"^\s*[1I]D[-–]4\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d2a = slice_section_lines(pages, start_1d2a, stop_1d2a, safety_pages_ahead=3)
    narr_1d2a = extract_narrative_after_limit(
        lines_1d2a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d2a,
        keep_paragraphs=True,
    )

    # --- 1D-3. Street outreach narrative ---
    start_1d3 = [
        r"^\s*[1I]D[-–]3\.\s*Street\s+Outreach.*$",
        r"^\s*[1I]D[-–]3\.\s*",
    ]
    stop_1d3 = [
        r"^\s*[1I]D[-–]4\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d3 = slice_section_lines(pages, start_1d3, stop_1d3, safety_pages_ahead=3)
    narr_1d3 = extract_narrative_after_limit(
        lines_1d3,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d3,
        keep_paragraphs=True,
    )

    # --- 1D-4. Strategies to Prevent Criminalization of Homelessness ---
    start_1d4 = [
        r"^\s*[1I]D[-–]4\.\s*Strategies\s+to\s+Prevent\s+Criminalization\s+of\s+Homelessness.*$",
        r"^\s*[1I]D[-–]4\.\s*",
    ]
    stop_1d4 = [
        r"^\s*[1I]D[-–]5\.",
        r"^\s*[1I]D[-–]6\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d4 = slice_section_lines(pages, start_1d4, stop_1d4, safety_pages_ahead=2)

    # Two Yes/No columns:
    #   - engaged/educated legislators and policymakers
    #   - implemented laws/policies/practices
    df_1d4 = parse_numbered_dual_tokens(
        lines_1d4,
        suffixes=("engaged", "implemented"),
    )

    # --- 1D-5. Rapid Rehousing – RRH Beds (HIC or Longitudinal HMIS Data) ---
    start_1d5 = [
        r"^\s*[1I]D[-–]5\.\s*Rapid\s+Rehousing[-–]\s*RRH\s+Beds\s+as\s+Reported\s+in\s+the\s+Housing\s+Inventory\s+Count.*$",
        r"^\s*[1I]D[-–]5\.\s*",
    ]
    stop_1d5 = [
        r"^\s*[1I]D[-–]6\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d5 = slice_section_lines(pages, start_1d5, stop_1d5, safety_pages_ahead=2)

    val_1d5_source = ""
    val_1d5_2023 = ""
    val_1d5_2024 = ""

    # Look for a row like:
    #   "HIC 215 206"
    #   "Longitudinal HMIS Data 215 206"
    # but skip header lines where the two numbers are years (2023, 2024).
    rx_1d5 = _re.compile(
        r"\b(HIC|Longitudinal\s+HMIS\s+Data)\b\s+(\d+)\s+(\d+)",
        _re.IGNORECASE,
    )

    for ln in lines_1d5:
        m = rx_1d5.search(ln)
        if not m:
            continue

        src_raw = m.group(1)
        n1_str = m.group(2)
        n2_str = m.group(3)
        n1 = int(n1_str)
        n2 = int(n2_str)

        # If both numbers look like years (e.g., 2023 2024), treat as header and skip
        if 1900 <= n1 <= 2100 and 1900 <= n2 <= 2100:
            continue

        # Otherwise this is the actual data row
        if src_raw.lower().startswith("hic"):
            val_1d5_source = "HIC"
        else:
            val_1d5_source = "Longitudinal HMIS Data"

        val_1d5_2023 = n1_str
        val_1d5_2024 = n2_str
        break

    # --- 1D-6. Mainstream Benefits – CoC Annual Training of Project Staff (Yes/No chart) ---
    start_1d6 = [
        r"^\s*[1I]D[-–]6\.\s*Mainstream\s+Benefits[-–]\s*CoC\s+Annual\s+Training\s+of\s+Project\s+Staff.*$",
        r"^\s*[1I]D[-–]6\.\s*",
    ]
    stop_1d6 = [
        r"^\s*[1I]D[-–]6a\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d6 = slice_section_lines(pages, start_1d6, stop_1d6, safety_pages_ahead=2)
    df_1d6 = parse_numbered_yesno(lines_1d6)

    # --- 1D-6a. Mainstream benefits / SOAR narrative ---
    start_1d6a = [
        r"^\s*[1I]D[-–]6a\.\s*Information\s+and\s+Training\s+on\s+Mainstream\s+Benefits.*$",
        r"^\s*[1I]D[-–]6a\.\s*",
    ]
    stop_1d6a = [
        r"^\s*[1I]D[-–]7\.",
        r"^\s*ID[-–]7\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d6a = slice_section_lines(pages, start_1d6a, stop_1d6a, safety_pages_ahead=3)
    narr_1d6a = extract_narrative_after_limit(
        lines_1d6a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d6a,
        keep_paragraphs=True,
    )

    # --- Fallback: if extract_narrative_after_limit didn't find anything (OCR weirdness) ---
    if not narr_1d6a.strip():
        header_6a_rx = _re.compile(r"^\s*[1I]D[-–]6a\.", _re.IGNORECASE)
        skip_6a_rx = _re.compile(
            r"^\s*(NOFO Section|Applicant:|Project:|FY20\d{2}\s+CoC Application Page)",
            _re.IGNORECASE,
        )
        describe_rx = _re.compile(r"^Describe\s+in\s+the\s+field\s+below", _re.IGNORECASE)

        started = False
        narr_lines_6a: list[str] = []

        for ln in lines_1d6a:
            # Drop the header and generic boilerplate
            if header_6a_rx.search(ln):
                continue
            if skip_6a_rx.search(ln):
                continue
            # This is the “Describe in the field below…” prompt – use it as an anchor,
            # but don't keep it in the narrative.
            if describe_rx.search(ln):
                started = True
                continue

            if not started:
                # Still before the narrative anchor – ignore
                continue

            # After we've started, keep non-empty lines as part of the narrative
            if not ln.strip():
                # allow blank lines as paragraph breaks
                narr_lines_6a.append("")
                continue

            narr_lines_6a.append(ln.rstrip())

        narr_1d6a = "\n".join(narr_lines_6a).strip()


    # --- 1D-7. Public health infectious disease response ---
    start_1d7 = [
        r"^\s*[1I]D[-–]7\.\s*Partnerships\s+with\s+Public\s+Health\s+Agencies.*$",
        r"^\s*ID[-–]7\.\s*",
    ]
    stop_1d7 = [
        r"^\s*[1I]D[-–]7a\.",
        r"^\s*ID[-–]7a\.",
        r"^\s*[1I]D[-–]8\.",
        r"^\s*ID[-–]8\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d7 = slice_section_lines(pages, start_1d7, stop_1d7, safety_pages_ahead=3)
    narr_1d7 = extract_narrative_after_limit(
        lines_1d7,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d7,
        keep_paragraphs=True,
    )

    # --- 1D-7a. Sharing public health info with providers ---
    start_1d7a = [
        r"^\s*[1I]D[-–]7a\.\s*Collaboration\s+With\s+Public\s+Health\s+Agencies\s+on\s+Infectious\s+Diseases.*$",
        r"^\s*ID[-–]7a\.\s*",
    ]
    stop_1d7a = [
        r"^\s*[1I]D[-–]8\.",
        r"^\s*ID[-–]8\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d7a = slice_section_lines(pages, start_1d7a, stop_1d7a, safety_pages_ahead=3)
    narr_1d7a = extract_narrative_after_limit(
        lines_1d7a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d7a,
        keep_paragraphs=True,
    )

    # --- 1D-8. CE standard processes ---
    start_1d8 = [
        r"^\s*[1I]D[-–]8\.\s*Coordinated\s+Entry\s+Standard\s+Processes.*$",
        r"^\s*ID[-–]8\.\s*",
    ]
    stop_1d8 = [
        r"^\s*[1I]D[-–]8a\.",
        r"^\s*ID[-–]8a\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d8 = slice_section_lines(pages, start_1d8, stop_1d8, safety_pages_ahead=3)
    narr_1d8 = extract_narrative_after_limit(
        lines_1d8,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d8,
        keep_paragraphs=True,
    )

    # --- 1D-8a. CE participant-centered approach ---
    start_1d8a = [
        r"^\s*[1I]D[-–]8a\.\s*Coordinated\s+Entry[-–]Program\s+Participant-Centered\s+Approach.*$",
        r"^\s*ID[-–]8a\.\s*",
    ]
    stop_1d8a = [
        r"^\s*[1I]D[-–]8b\.",
        r"^\s*ID[-–]8b\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d8a = slice_section_lines(pages, start_1d8a, stop_1d8a, safety_pages_ahead=3)
    narr_1d8a = extract_narrative_after_limit(
        lines_1d8a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d8a,
        keep_paragraphs=True,
    )

    # --- 1D-8b. CE rights / remedies / reporting ---
    start_1d8b = [
        r"^\s*[1I]D[-–]8b\.\s*Coordinated\s+Entry[-–]Informing\s+Program\s+Participants.*$",
        r"^\s*ID[-–]8b\.\s*",
    ]
    # NEW: stop when 1D-9 starts (so 1D-8b doesn’t eat 1D-9)
    stop_1d8b = [
        r"^\s*[1I]D[-–]9\.",   # 1D-9 header
        r"^\s*2A[-–]1\.",      # old safety stop
    ]
    lines_1d8b = slice_section_lines(pages, start_1d8b, stop_1d8b, safety_pages_ahead=3)
    narr_1d8b = extract_narrative_after_limit(
        lines_1d8b,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d8b,
        keep_paragraphs=True,
    )

    # --- 1D-9. Advancing Racial Equity in Homelessness – Conducting Assessment ---
    # 1D-9 main block (Q1 yes/no, Q2 date)
    start_1d9 = [
        r"^\s*[1I]D[-–]9\.\s*Advancing\s+Racial\s+Equity\s+in\s+Homelessness.*$",
        r"^\s*[1I]D[-–]9\.\s*",
    ]
    stop_1d9 = [
        r"^\s*[1I]D[-–]9a\.",
        r"^\s*[1I]D[-–]10\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d9 = slice_section_lines(pages, start_1d9, stop_1d9, safety_pages_ahead=2)

    val_1d9_1 = ""  # Has your CoC conducted a racial disparities assessment in last 3 years? (Yes/No)
    val_1d9_2 = ""  # Date assessment was completed (mm/dd/yyyy)

    # Q1 – yes/no
    yesno_rx = _re.compile(r"\b(Yes|No)\b", _re.IGNORECASE)
    for ln in lines_1d9:
        if "Has your CoC conducted a racial disparities assessment" not in ln:
            continue
        m = yesno_rx.search(ln)
        if not m:
            # occasionally Yes/No ends up on following line; look ahead a few lines
            idx = lines_1d9.index(ln)
            for ln2 in lines_1d9[idx + 1 : idx + 4]:
                m2 = yesno_rx.search(ln2)
                if m2:
                    m = m2
                    break
        if m:
            val_1d9_1 = m.group(1).title()
            break

    # Q2 – date
    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
    for ln in lines_1d9:
        m = date_rx.search(ln)
        if m:
            val_1d9_2 = m.group(0)
            break

    # --- 1D-9a. Clarifying Racial Equity Goals (narrative) ---
    start_1d9a = [
        r"^\s*[1I]D[-–]9a\.\s*",
    ]
    stop_1d9a = [
        r"^\s*[1I]D[-–]9b\.",
        r"^\s*[1I]D[-–]10\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d9a = slice_section_lines(pages, start_1d9a, stop_1d9a, safety_pages_ahead=3)
    narr_1d9a = extract_narrative_after_limit(
        lines_1d9a,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d9a,
        keep_paragraphs=True,
    )

    # --- 1D-9b. Strategy priorities (Yes/No chart for 1..11) ---
    start_1d9b = [
        r"^\s*[1I]D[-–]9b\.\s*",
    ]
    stop_1d9b = [
        r"^\s*[1I]D[-–]9c\.",
        r"^\s*[1I]D[-–]10\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d9b = slice_section_lines(pages, start_1d9b, stop_1d9b, safety_pages_ahead=4)
    df_1d9b = parse_numbered_yesno(lines_1d9b)

    # --- 1D-9c. Leading with Lived Experience (narrative) ---
    start_1d9c = [
        r"^\s*[1I]D[-–]9c\.\s*",
    ]
    stop_1d9c = [
        r"^\s*[1I]D[-–]9d\.",
        r"^\s*[1I]D[-–]10\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d9c = slice_section_lines(pages, start_1d9c, stop_1d9c, safety_pages_ahead=3)
    narr_1d9c = extract_narrative_after_limit(
        lines_1d9c,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d9c,
        keep_paragraphs=True,
    )

    # --- 1D-9d. Addressing Identified Disparities (narrative) ---
    start_1d9d = [
        r"^\s*[1I]D[-–]9d\.\s*",
    ]
    stop_1d9d = [
        r"^\s*[1I]D[-–]10\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d9d = slice_section_lines(pages, start_1d9d, stop_1d9d, safety_pages_ahead=3)
    narr_1d9d = extract_narrative_after_limit(
        lines_1d9d,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d9d,
        keep_paragraphs=True,
    )

    # --- 1D-10. Incorporating Persons with Lived Experience (narrative) ---
    start_1d10 = [
        r"^\s*[1I]D[-–]10\.\s*",
    ]
    stop_1d10 = [
        r"^\s*[1I]D[-–]10a\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d10 = slice_section_lines(pages, start_1d10, stop_1d10, safety_pages_ahead=3)

    narr_lines_1d10: list[str] = []
    header_1d10_rx = _re.compile(r"^\s*[1I]D[-–]10\.", _re.IGNORECASE)
    skip_1d10_rx = _re.compile(
        r"^\s*(NOFO Section|Applicant:|Project:|FY20\d{2}\s+CoC Application Page)",
        _re.IGNORECASE,
    )

    for ln in lines_1d10:
        if header_1d10_rx.search(ln):
            continue
        if skip_1d10_rx.search(ln):
            continue
        if not ln.strip():
            continue
        narr_lines_1d10.append(ln.rstrip())
    narr_1d10 = "\n".join(narr_lines_1d10).strip()

    # --- 1D-10a. Roles of persons with lived experience (numeric table) ---
    start_1d10a = [
        r"^\s*[1I]D[-–]10a\.\s*",
    ]
    stop_1d10a = [
        r"^\s*[1I]D[-–]10b\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d10a = slice_section_lines(pages, start_1d10a, stop_1d10a, safety_pages_ahead=2)

    vals_1d10a_years = ["", "", "", ""]
    vals_1d10a_unshel = ["", "", "", ""]
    # lines look like: "1. … 4 0"
    row_10a_rx = _re.compile(r"^\s*(\d+)\.\s.*?(\d+)\s+(\d+)\s*$")

    for ln in lines_1d10a:
        m = row_10a_rx.match(ln.strip())
        if not m:
            continue
        idx = int(m.group(1))
        if 1 <= idx <= 4:
            vals_1d10a_years[idx - 1] = m.group(2)
            vals_1d10a_unshel[idx - 1] = m.group(3)

    val_1d10a_1_years, val_1d10a_2_years, val_1d10a_3_years, val_1d10a_4_years = vals_1d10a_years
    (
        val_1d10a_1_unsheltered,
        val_1d10a_2_unsheltered,
        val_1d10a_3_unsheltered,
        val_1d10a_4_unsheltered,
    ) = vals_1d10a_unshel

    # --- 1D-10b. Equipping Persons with Lived Experience (narrative) ---
    start_1d10b = [
        r"^\s*[1I]D[-–]10b\.\s*",
    ]
    stop_1d10b = [
        r"^\s*[1I]D[-–]10c\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d10b = slice_section_lines(pages, start_1d10b, stop_1d10b, safety_pages_ahead=2)
    narr_1d10b = extract_narrative_after_limit(
        lines_1d10b,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d10b,
        keep_paragraphs=True,
    )

    # --- 1D-10c. Compensation for persons with lived experience (narrative) ---
    start_1d10c = [
        r"^\s*[1I]D[-–]10c\.\s*",
    ]
    stop_1d10c = [
        r"^\s*[1I]D[-–]11\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d10c = slice_section_lines(pages, start_1d10c, stop_1d10c, safety_pages_ahead=2)
    narr_1d10c = extract_narrative_after_limit(
        lines_1d10c,
        start_patterns=[r"^Describe\s+in\s+the\s+field\s+below.*$"],
        stop_patterns=stop_1d10c,
        keep_paragraphs=True,
    )

    # 1D-11 – no question text in this CoC pdf, so leave blank placeholder
    val_1d11 = ""

    return {
        "df_1d1": df_1d1,
        "df_1d4": df_1d4,
        "df_1d6": df_1d6,
        "val_1d2_1": val_1d2_1,
        "val_1d2_2": val_1d2_2,
        "val_1d2_3": val_1d2_3,
        "narr_1d2a": narr_1d2a,
        "narr_1d3": narr_1d3,
        "val_1d5_source": val_1d5_source,
        "val_1d5_2023": val_1d5_2023,
        "val_1d5_2024": val_1d5_2024,
        "narr_1d6a": narr_1d6a,
        "narr_1d7": narr_1d7,
        "narr_1d7a": narr_1d7a,
        "narr_1d8": narr_1d8,
        "narr_1d8a": narr_1d8a,
        "narr_1d8b": narr_1d8b,
        "val_1d9_1": val_1d9_1,
        "val_1d9_2": val_1d9_2,
        "narr_1d9a": narr_1d9a,
        "df_1d9b": df_1d9b,
        "narr_1d9c": narr_1d9c,
        "narr_1d9d": narr_1d9d,
        "narr_1d10": narr_1d10,
        "val_1d10a_1_years": val_1d10a_1_years,
        "val_1d10a_1_unsheltered": val_1d10a_1_unsheltered,
        "val_1d10a_2_years": val_1d10a_2_years,
        "val_1d10a_2_unsheltered": val_1d10a_2_unsheltered,
        "val_1d10a_3_years": val_1d10a_3_years,
        "val_1d10a_3_unsheltered": val_1d10a_3_unsheltered,
        "val_1d10a_4_years": val_1d10a_4_years,
        "val_1d10a_4_unsheltered": val_1d10a_4_unsheltered,
        "narr_1d10b": narr_1d10b,
        "narr_1d10c": narr_1d10c,
        "val_1d11": val_1d11,
    }


def run_all(pdf_path: Path, out_dir: Path | None = None) -> dict:
    """
    End-to-end parse for 1A, 1B-1, 1C-1..7, and all of 1D.
    Returns a dict with DataFrames and paths; writes CSV/TXT if out_dir is provided.
    """
    pdf_path = Path(pdf_path).resolve()
    if out_dir is None:
        out_dir = pdf_path.parent

    # Extract full text
    full_text, n_pages, engine = extract_pdf_text(pdf_path)
    txt_path = save_text_unique(
        out_dir / f"{pdf_path.stem}__text_{ts()}.txt", full_text
    )

    pages = split_pages_by_markers(full_text)

    # 1A
    meta_vals, meta_debug = parse_1a_metadata(pages)

    # 1B, 1C, 1D
    section_data: dict[str, object] = {}
    section_data.update(parse_1b(pages, meta_vals, pdf_path, out_dir))
    section_data.update(parse_1c(pages, meta_vals, pdf_path, out_dir))
    section_data.update(parse_1d(pages, meta_vals, pdf_path, out_dir))

    # Build wide table
    wide_df = build_wide(
        meta_vals=meta_vals,
        **section_data,
    )

    # Order columns and save
    cols = [c for c in col_order_extended() if c in wide_df.columns]
    wide_df = wide_df.reindex(columns=cols)
    save_csv_unique(out_dir / f"{pdf_path.stem}__1a_1b_1c_wide.csv", wide_df)

    # Collect outputs
    result: dict[str, object] = {
        "txt_path": txt_path,
        "meta_vals": meta_vals,
        "wide_df": wide_df,
    }
    result.update(section_data)
    return result
