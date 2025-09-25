
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class LeadRow:
    product: str
    codes: Tuple[str, ...]              # Buz inventory codes (normalized, upper, stripped)
    lead_text: str                      # Original text from sheet
    upper_bound_weeks: float            # Parsed numeric upper-bound in weeks (business weeks)


@dataclass(frozen=True)
class CutoffRow:
    product: str
    codes: Tuple[str, ...]
    cutoff_date: date                   # Normalized to a date


@dataclass
class MergedItem:
    product: str
    codes: Tuple[str, ...]
    lead_text: str
    cutoff_date: Optional[date]
