# generic_parse.py
from __future__ import annotations
from pathlib import Path
from typing import Sequence

from .specs import TableSpec, NarrSpec
from .slicer import slice_section_lines
from .narratives import extract_narrative_after_limit


Pages = Sequence[tuple[int, str]]


def parse_tables(pages: Pages, table_specs: Sequence[TableSpec]) -> dict:
    """
    Run all TableSpecs and return dict {spec.key: df}.
    Table parser signature: parser(lines) -> pd.DataFrame
    """
    out: dict[str, object] = {}
    for s in table_specs:
        lines = slice_section_lines(
            pages,
            start_patterns=s.start,
            stop_patterns=s.stop,
            safety_pages_ahead=s.safety_pages_ahead,
        )
        df = s.parser(lines)
        if s.post:
            df = s.post(df)
        out[s.key] = df
    return out


def parse_narratives(pages: Pages, narr_specs: Sequence[NarrSpec]) -> dict:
    """
    Run all NarrSpecs and return dict {spec.key: str}.
    """
    out: dict[str, str] = {}
    for s in narr_specs:
        lines = slice_section_lines(
            pages,
            start_patterns=s.anchor_start,
            stop_patterns=s.anchor_stop,
            safety_pages_ahead=s.safety_pages_ahead,
        )
        out[s.key] = extract_narrative_after_limit(
            lines,
            start_patterns=s.narr_start,
            stop_patterns=s.narr_stop,
            keep_paragraphs=True,
        )
    return out
