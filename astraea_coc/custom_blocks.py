# custom_blocks.py
from __future__ import annotations
from pathlib import Path
import re as _re

from .slicer import slice_section_lines
from .parsers import parse_numbered_yesno
from .utils import norm_token


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
