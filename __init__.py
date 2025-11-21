
"""
astraea_coc — CoC PDF → structured text/CSV parsers
"""
from .io_extract import extract_pdf_text, split_pages_by_markers
from .meta import parse_1a_metadata, find_first
from .slicer import slice_section_lines
from .parsers import parse_triple_table, parse_numbered_yesno, parse_numbered_dual_tokens
from .narratives import extract_narrative_after_limit
from .build_wide import build_wide, col_order_extended
from .utils import ts, unique_path, save_text_unique, save_csv_unique, norm_token, scrub_boilerplate

__all__ = [
    "extract_pdf_text", "split_pages_by_markers",
    "parse_1a_metadata", "find_first",
    "slice_section_lines",
    "parse_triple_table", "parse_numbered_yesno", "parse_numbered_dual_tokens",
    "extract_narrative_after_limit",
    "build_wide", "col_order_extended",
    "ts", "unique_path", "save_text_unique", "save_csv_unique", "norm_token", "scrub_boilerplate",
]
