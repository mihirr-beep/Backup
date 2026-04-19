from __future__ import annotations


import re
from dataclasses import dataclass
from typing import List, Optional, Tuple


_INDEX_RE = re.compile(r"\[(\d+)\]")


def extract_indices(afd_key: str) -> List[int]:
    """Return all numeric list indices from an AFD key.

    Example:
      'measuredNonNodalLesions[0].issues[2].question' -> [0, 2]
    """

    return [int(m.group(1)) for m in _INDEX_RE.finditer(afd_key)]


def canonicalize_key(afd_key: str) -> str:
    """
    Normalize indices so that any '[<digits>]' becomes '[]'.
    Also normalizes 'ADF_' and 'AFD_' to 'AXX_' to handle naming swaps.
    """
    if not afd_key:
        return afd_key
    
    # 1. Normalize numbers
    k = _INDEX_RE.sub("[]", afd_key)
    
    # 2. Normalize ADF/AFD swap (case insensitive)
    # We use AXX_ as a placeholder to ensure both map to the same target.
    k = re.sub(r'^ADF_', 'AXX_', k, flags=re.IGNORECASE)
    k = re.sub(r'^AFD_', 'AXX_', k, flags=re.IGNORECASE)
    
    return k


def build_row_index(indices: List[int]) -> Tuple[str, str]:
    """Build ROW_INDEX and PARENT_ROW_INDEX from a list of indices.

    ROW_INDEX is a '-' joined, zero-padded (3 digits) representation.

    Examples:
      [] -> ('000', 'na')
      [0] -> ('000', 'na')
      [0, 2] -> ('000-002', '000')
    """

    if not indices:
        return "000", "na"

    parts = [str(i).zfill(3) for i in indices]
    row_index = "-".join(parts)
    parent = "na" if len(parts) == 1 else "-".join(parts[:-1])
    return row_index, parent


