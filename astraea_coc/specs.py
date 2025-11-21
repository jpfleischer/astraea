# specs.py
from dataclasses import dataclass
from typing import Callable, Sequence, Optional

@dataclass(frozen=True)
class TableSpec:
    key: str                       # df_1c1, df_1d9b, ...
    start: Sequence[str]
    stop: Sequence[str]
    parser: Callable               # parse_numbered_yesno, parse_triple_table, ...
    safety_pages_ahead: int = 3
    post: Optional[Callable] = None  # optional df cleanup

@dataclass(frozen=True)
class NarrSpec:
    key: str                      # narr_1c4a, narr_1d7, ...
    anchor_start: Sequence[str]
    anchor_stop: Sequence[str]
    narr_start: Sequence[str]     # prompts / NOFO anchors
    narr_stop: Sequence[str]
    safety_pages_ahead: int = 3
