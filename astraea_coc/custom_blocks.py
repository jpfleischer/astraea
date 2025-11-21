# custom_blocks.py
from __future__ import annotations
import re as _re

from .slicer import slice_section_lines
from .parsers import parse_numbered_yesno
from .parsers import parse_2a5_bed_coverage


Pages = list[tuple[int, str]]


def custom_1c7d(pages: Pages) -> dict[str, str]:
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

    df_yn = parse_numbered_yesno(lines_1c7d)
    val_1c7d_1 = ""
    if not df_yn.empty:
        row = df_yn[df_yn["index"] == 1]
        if not row.empty:
            val_1c7d_1 = row["value"].iloc[0]

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

    if val_1c7d_1 != "Yes":
        narr_1c7d_2 = "Empty"

    return {
        "val_1c7d_1": val_1c7d_1,
        "narr_1c7d_2": narr_1c7d_2,
    }


def custom_1c7e(pages: Pages) -> dict[str, str]:
    start_1c7e = [
        r"^\s*1C[-–]7e\.\s*Coordinating\s+with\s+PHA\(s\)\s+to\s+Apply\s+for\s+or\s+Implement\s+HCV.*$",
        r"^\s*1C[-–]7e\.\s*",
    ]
    stop_1c7e = [
        r"^\s*1D[-–]1\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1c7e = slice_section_lines(pages, start_1c7e, stop_1c7e, safety_pages_ahead=2)

    yesno_rx = _re.compile(r"\b(Yes|No|Nonexistent)\b", _re.IGNORECASE)
    val_1c7e = ""
    for ln in reversed(lines_1c7e):
        m = yesno_rx.search(ln)
        if m:
            val_1c7e = m.group(1).title()
            break

    return {"val_1c7e": val_1c7e}


def custom_1d2(pages: Pages) -> dict[str, str]:
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

    def _grab_answer_after_question(lines, question_rx, max_lookahead=10):
        for idx, ln in enumerate(lines):
            if question_rx.search(ln):
                for ln2 in lines[idx + 1 : idx + 1 + max_lookahead]:
                    if _re.match(r"^\s*[123]\.\s", ln2):
                        break
                    if _re.match(r"^\s*[1I]D[-–]2a\.", ln2) or _re.match(r"^\s*[1I]D[-–]3\.", ln2):
                        break
                    m = _re.search(r"\b(\d+%?)\b", ln2)
                    if not m:
                        continue
                    tok = m.group(1)
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

    q1_rx = _re.compile(r"^\s*1\.\s*Enter\s+the\s+total\s+number", _re.IGNORECASE)
    q2_rx = _re.compile(r"^\s*2\.\s*Enter\s+the\s+total\s+number", _re.IGNORECASE)
    q3_rx = _re.compile(r"^\s*3\.\s*This\s+number\s+is\s+a\s+calculation", _re.IGNORECASE)

    val_1d2_1 = _grab_answer_after_question(lines_1d2, q1_rx)
    val_1d2_2 = _grab_answer_after_question(lines_1d2, q2_rx)
    val_1d2_3 = _grab_answer_after_question(lines_1d2, q3_rx)

    if not (val_1d2_1 and val_1d2_2 and val_1d2_3):
        nums = []
        skip_enums = {"1","2","3"}
        for ln in lines_1d2:
            for tok in _re.findall(r"\b\d+%?\b", ln):
                if tok in skip_enums:
                    continue
                try:
                    num = int(tok.rstrip("%"))
                except ValueError:
                    continue
                if not tok.endswith("%") and 1900 <= num <= 2100:
                    continue
                nums.append(tok)
                if len(nums) == 3:
                    break
            if len(nums) == 3:
                break
        val_1d2_1 = val_1d2_1 or (nums[0] if len(nums) >= 1 else "")
        val_1d2_2 = val_1d2_2 or (nums[1] if len(nums) >= 2 else "")
        val_1d2_3 = val_1d2_3 or (nums[2] if len(nums) >= 3 else "")

    if val_1d2_3 and not val_1d2_3.endswith("%"):
        val_1d2_3 = f"{val_1d2_3}%"

    return {
        "val_1d2_1": val_1d2_1,
        "val_1d2_2": val_1d2_2,
        "val_1d2_3": val_1d2_3,
    }


def custom_1d5(pages: Pages) -> dict[str, str]:
    start_1d5 = [
        r"^\s*[1I]D[-–]5\.\s*Rapid\s+Rehousing[-–]\s*RRH\s+Beds\s+as\s+Reported\s+in\s+the\s+Housing\s+Inventory\s+Count.*$",
        r"^\s*[1I]D[-–]5\.\s*",
    ]
    stop_1d5 = [
        r"^\s*[1I]D[-–]6\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d5 = slice_section_lines(pages, start_1d5, stop_1d5, safety_pages_ahead=2)

    rx_1d5 = _re.compile(
        r"\b(HIC|Longitudinal\s+HMIS\s+Data)\b\s+(\d+)\s+(\d+)",
        _re.IGNORECASE,
    )
    val_1d5_source = val_1d5_2023 = val_1d5_2024 = ""

    for ln in lines_1d5:
        m = rx_1d5.search(ln)
        if not m:
            continue
        src_raw, n1_str, n2_str = m.group(1), m.group(2), m.group(3)
        n1, n2 = int(n1_str), int(n2_str)
        if 1900 <= n1 <= 2100 and 1900 <= n2 <= 2100:
            continue
        val_1d5_source = "HIC" if src_raw.lower().startswith("hic") else "Longitudinal HMIS Data"
        val_1d5_2023 = n1_str
        val_1d5_2024 = n2_str
        break

    return {
        "val_1d5_source": val_1d5_source,
        "val_1d5_2023": val_1d5_2023,
        "val_1d5_2024": val_1d5_2024,
    }


def custom_1d9(pages: Pages) -> dict[str, str]:
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

    val_1d9_1 = val_1d9_2 = ""
    yesno_rx = _re.compile(r"\b(Yes|No)\b", _re.IGNORECASE)
    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

    for ln in lines_1d9:
        if "Has your CoC conducted a racial disparities assessment" not in ln:
            continue
        m = yesno_rx.search(ln)
        if m:
            val_1d9_1 = m.group(1).title()
            break

    for ln in lines_1d9:
        m = date_rx.search(ln)
        if m:
            val_1d9_2 = m.group(0)
            break

    return {"val_1d9_1": val_1d9_1, "val_1d9_2": val_1d9_2}


def custom_1d10a(pages: Pages) -> dict[str, str]:
    start_1d10a = [r"^\s*[1I]D[-–]10a\.\s*"]
    stop_1d10a = [
        r"^\s*[1I]D[-–]10b\.",
        r"^\s*2A[-–]1\.",
    ]
    lines_1d10a = slice_section_lines(pages, start_1d10a, stop_1d10a, safety_pages_ahead=2)

    vals_years = ["", "", "", ""]
    vals_unshel = ["", "", "", ""]
    row_rx = _re.compile(r"^\s*(\d+)\.\s.*?(\d+)\s+(\d+)\s*$")

    for ln in lines_1d10a:
        m = row_rx.match(ln.strip())
        if not m:
            continue
        idx = int(m.group(1))
        if 1 <= idx <= 4:
            vals_years[idx - 1] = m.group(2)
            vals_unshel[idx - 1] = m.group(3)

    return {
        "val_1d10a_1_years": vals_years[0],
        "val_1d10a_1_unsheltered": vals_unshel[0],
        "val_1d10a_2_years": vals_years[1],
        "val_1d10a_2_unsheltered": vals_unshel[1],
        "val_1d10a_3_years": vals_years[2],
        "val_1d10a_3_unsheltered": vals_unshel[2],
        "val_1d10a_4_years": vals_years[3],
        "val_1d10a_4_unsheltered": vals_unshel[3],
    }


def custom_2a_basic(pages) -> dict[str, str]:
    out: dict[str, str] = {}

    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

    boiler_rx = _re.compile(
        r"^\s*("
        r"2A\b|2A[-–]\d+\.|"
        r"Homeless Management Information System|HMIS Implementation|"
        r"HUD publishes resources|Resources include|"
        r"Not Scored|For Information Only|"
        r"NOFO Section|24 CFR|Navigational Guide|"
        r"You must enter|You must provide|"
        r"Applicant:|Project:|FY20\d{2}\s+CoC Application Page|Page\s+\d+"
        r")",
        _re.IGNORECASE,
    )

    # NEW: prompt prefixes that may share a line with the answer
    prompt_strip_rxes = [
        _re.compile(r"^\s*Enter the name of the HMIS Vendor your CoC is currently using\.\s*",
                    _re.IGNORECASE),
        _re.compile(r"^\s*Select from dropdown menu your CoC’s HMIS coverage area\.\s*",
                    _re.IGNORECASE),
        _re.compile(r"^\s*Enter the date your CoC submitted its 2024 HIC data into HDX\.\s*",
                    _re.IGNORECASE),
    ]

    def _strip_prompt_prefix(s: str) -> str:
        for rx in prompt_strip_rxes:
            s2 = rx.sub("", s).strip()
            if s2 != s:
                s = s2
        return s

    def _extract_free_text(lines: list[str]) -> str:
        keep: list[str] = []
        for ln in lines:
            raw = ln.strip()
            if not raw:
                continue

            s = _strip_prompt_prefix(raw)

            # NEW: strip leading list markers / bullets
            s = _re.sub(r"^\s*\d+[\.\)]\s*", "", s)
            s = _re.sub(r"^\s*[\-\u2022•]+\s*", "", s)
            if _re.fullmatch(r"[.\)\-]+", s):
                continue

            # if line is still pure boilerplate, skip
            if boiler_rx.search(s):
                continue

            if s:
                keep.append(s)

        if not keep:
            return "Empty"

        text = " ".join(keep)
        text = _re.sub(r"\s+,", ",", text)
        text = _re.sub(r"\s+", " ", text).strip()
        return text


    def _pick_answer_date(lines: list[str]) -> str:
        candidates: list[str] = []
        for ln in lines:
            raw = ln.strip()
            if not raw:
                continue

            s = _strip_prompt_prefix(raw)

            if boiler_rx.search(s):
                continue

            ds = date_rx.findall(s)
            if not ds:
                continue

            stripped = s.strip()
            if _re.fullmatch(r"\d{2}/\d{2}/\d{4}", stripped):
                candidates.extend(ds)
            elif stripped.endswith(ds[-1]):
                candidates.extend(ds)

        return candidates[-1] if candidates else "Empty"

    # 2A-1 vendor
    lines_2a1 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]1\.\s*HMIS Vendor"],
        stop_patterns=[r"^\s*2A[-–]2\."],
        safety_pages_ahead=2,
    )
    out["val_2a_1"] = _extract_free_text(lines_2a1)

    # 2A-2 coverage area
    lines_2a2 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]2\.\s*HMIS Implementation Coverage Area"],
        stop_patterns=[r"^\s*2A[-–]3\."],
        safety_pages_ahead=2,
    )
    out["val_2a_2"] = _extract_free_text(lines_2a2)

    # 2A-3 HIC submission date
    lines_2a3 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]3\.\s*HIC Data Submission in HDX"],
        stop_patterns=[r"^\s*2A[-–]4\."],
        safety_pages_ahead=2,
    )
    out["val_2a_3"] = _pick_answer_date(lines_2a3)

    return out



def custom_2a4(pages: Pages) -> dict[str, str]:
    """
    2A-4 is a narrative block.
    Use the same prompt-skipping logic as 1E narratives.
    """
    lines_2a4 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]4\.\s*Comparable Databases for DV Providers"],
        stop_patterns=[r"^\s*2A[-–]5\."],
        safety_pages_ahead=3,
    )

    header_skip = _re.compile(
        r"^\s*(NOFO Section|Applicant:|Project:|FY20\d{2}\s+CoC Application Page|Page\s+\d+)",
        _re.IGNORECASE,
    )
    limit_line = _re.compile(r"limit\s*2,?500\s*characters", _re.IGNORECASE)
    describe_line = _re.compile(r"Describe\s+in\s+the\s+field\s+below", _re.IGNORECASE)

    state = "seek_anchor"
    prompt_seen = False
    buf: list[str] = []

    for ln in lines_2a4:
        if header_skip.search(ln):
            continue

        stripped = ln.strip()

        if state == "seek_anchor":
            if describe_line.search(ln):
                state = "in_prompt"
                prompt_seen = True
                continue
            if limit_line.search(ln):
                state = "in_answer"
                continue
            continue

        if state == "in_prompt":
            if limit_line.search(ln):
                state = "in_answer"
                continue
            if not stripped and prompt_seen:
                state = "in_answer"
                continue
            if stripped:
                prompt_seen = True
            continue

        # in_answer
        if not stripped:
            buf.append("")
        else:
            buf.append(ln.rstrip())

    text = "\n".join(buf).strip()

    # Clean the OCR hanging quote/parens you saw
    text = _re.sub(r'^\s*"\)\s*', "", text)
    text = _re.sub(r'\s*"\s*$', "", text).strip()

    if not text:
        text = "Empty"

    return {"narr_2a_4": text}


def custom_2a5(pages: Pages) -> dict[str, str]:
    """
    2A-5 is a 6-row numeric table. We parse it to a DF, then
    emit scalar keys that auto-map into wide columns:
      val_2a_5_{i}_non_vsp
      val_2a_5_{i}_vsp
      val_2a_5_{i}_hmis
      val_2a_5_{i}_coverage
    """
    lines_2a5 = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]5\.\s*Bed Coverage Rate"],
        stop_patterns=[r"^\s*2A[-–]5a\."],
        safety_pages_ahead=2,
    )

    df = parse_2a5_bed_coverage(lines_2a5)

    out: dict[str, str] = {}

    # default empties for all 6 rows
    for i in range(1, 7):
        out[f"val_2a_5_{i}_non_vsp"] = "Empty"
        out[f"val_2a_5_{i}_vsp"]     = "Empty"
        out[f"val_2a_5_{i}_hmis"]    = "Empty"
        out[f"val_2a_5_{i}_coverage"]= "Empty"

    if df is None or df.empty:
        return out

    for _, row in df.iterrows():
        i = int(row["index"])
        if not (1 <= i <= 6):
            continue
        out[f"val_2a_5_{i}_non_vsp"]  = str(row.get("adj_total_non_vsp_beds", "")).strip() or "Empty"
        out[f"val_2a_5_{i}_vsp"]      = str(row.get("adj_total_vsp_beds", "")).strip() or "Empty"
        out[f"val_2a_5_{i}_hmis"]     = str(row.get("total_hmis_plus_vsp_beds", "")).strip() or "Empty"
        out[f"val_2a_5_{i}_coverage"] = str(row.get("coverage_rate", "")).strip() or "Empty"

    return out


def custom_2a5a(pages: Pages) -> dict[str, str]:
    """
    2A-5a is a narrative block.
    """
    lines_2a5a = slice_section_lines(
        pages,
        start_patterns=[r"^\s*2A[-–]5a\.\s*Partial Credit for Bed Coverage Rates"],
        stop_patterns=[r"^\s*2A[-–]6\."],
        safety_pages_ahead=3,
    )

    # reuse same narrative stripper as 2a4
    header_skip = _re.compile(
        r"^\s*(NOFO Section|Applicant:|Project:|FY20\d{2}\s+CoC Application Page|Page\s+\d+)",
        _re.IGNORECASE,
    )
    limit_line = _re.compile(r"limit\s*2,?500\s*characters", _re.IGNORECASE)
    describe_line = _re.compile(r"Describe\s+in\s+the\s+field\s+below", _re.IGNORECASE)

    state = "seek_anchor"
    prompt_seen = False
    buf: list[str] = []

    for ln in lines_2a5a:
        if header_skip.search(ln):
            continue
        stripped = ln.strip()

        if state == "seek_anchor":
            if describe_line.search(ln):
                state = "in_prompt"
                prompt_seen = True
                continue
            if limit_line.search(ln):
                state = "in_answer"
                continue
            continue

        if state == "in_prompt":
            if limit_line.search(ln):
                state = "in_answer"
                continue
            if not stripped and prompt_seen:
                state = "in_answer"
                continue
            if stripped:
                prompt_seen = True
            continue

        if not stripped:
            buf.append("")
        else:
            buf.append(ln.rstrip())

    text = "\n".join(buf).strip()
    if not text:
        text = "Empty"

    return {"narr_2a_5a": text}


def custom_2a6(pages: Pages) -> dict[str, str]:
    start_2a6 = [
        r"^\s*2A[-–]6\.\s*Longitudinal System Analysis",
        r"^\s*2A[-–]6\.\s*",
    ]
    stop_2a6 = [
        r"^\s*2B[-–]1\.",
        r"^\s*2B\.",
        r"^\s*2C[-–]1\.",
    ]
    lines_2a6 = slice_section_lines(pages, start_2a6, stop_2a6, safety_pages_ahead=2)

    yesno_rx = _re.compile(r"\b(Yes|No)\b", _re.IGNORECASE)
    val = "Empty"
    for ln in reversed(lines_2a6):
        m = yesno_rx.search(ln)
        if m:
            val = m.group(1).title()
            break

    return {"val_2a_6": val}


def custom_2b1(pages: Pages) -> dict[str, str]:
    start_2b1 = [
        r"^\s*2B[-–]1\.\s*PIT Count Date",
        r"^\s*2B[-–]1\.\s*",
    ]
    stop_2b1 = [
        r"^\s*2B[-–]2\.",
        r"^\s*2B[-–]3\.",
        r"^\s*2C[-–]1\.",
    ]
    lines_2b1 = slice_section_lines(pages, start_2b1, stop_2b1, safety_pages_ahead=2)

    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
    dates = date_rx.findall("\n".join(lines_2b1))
    val = dates[-1] if dates else "Empty"

    return {"val_2b_1": val}


def custom_2b2(pages: Pages) -> dict[str, str]:
    start_2b2 = [
        r"^\s*2B[-–]2\.\s*PIT Count Data",
        r"^\s*2B[-–]2\.\s*",
    ]
    stop_2b2 = [
        r"^\s*2B[-–]3\.",
        r"^\s*2C[-–]1\.",
    ]
    lines_2b2 = slice_section_lines(pages, start_2b2, stop_2b2, safety_pages_ahead=2)

    date_rx = _re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
    dates = date_rx.findall("\n".join(lines_2b2))
    val = dates[-1] if dates else "Empty"

    return {"val_2b_2": val}
