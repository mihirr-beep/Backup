# from __future__ import annotations

# import re
# from dataclasses import dataclass
# from typing import List, Optional, Tuple

# def parse_path_structure(afd_key: str) -> List[dict]:
#     """
#     Parses the key into structural tokens (name + index) using fast string splitting.
#     """
#     if not afd_key:
#         return []
        
#     tokens = []
#     parts = afd_key.split('.')

#     for part in parts:
#         # Check if this part has an index like "name[123]"
#         bracket_start = part.rfind('[')
        
#         if bracket_start != -1 and part.endswith(']'):
#             name = part[:bracket_start]
#             idx_str = part[bracket_start + 1 : -1]
#             idx = int(idx_str) if idx_str.isdigit() else None
#             tokens.append({"name": name, "index": idx})
#         else:
#             tokens.append({"name": part, "index": None})
            
#     return tokens

# def extract_indices(afd_key: str) -> List[int]:
#     """
#     Fast index extractor — no regex, minimal string operations.
#     """
#     if not afd_key:
#         return []

#     indices: List[int] = []
#     parts = afd_key.split('.')

#     for part in parts:
#         if '[' not in part or ']' not in part: continue
#         bracket_start = part.rfind('[')
#         if bracket_start == -1 or part[-1] != ']': continue
#         idx_str = part[bracket_start + 1 : -1]
#         if idx_str.isdigit():
#             indices.append(int(idx_str))

#     return indices

# def canonicalize_key(afd_key: str) -> str:
#     """
#     OPTIMIZED: Rebuilds the template string without object overhead.
#     """
#     if not afd_key:
#         return afd_key
    
#     parts = afd_key.split('.')
#     clean_parts = []
#     for part in parts:
#         start = part.find('[')
#         if start != -1:
#             clean_parts.append(part[:start] + "[]")
#         else:
#             clean_parts.append(part)
            
#     k = ".".join(clean_parts)
#     # Keep prefix normalization
#     k = re.sub(r'^ADF_', 'AXX_', k, flags=re.IGNORECASE)
#     k = re.sub(r'^AFD_', 'AXX_', k, flags=re.IGNORECASE)
#     return k


# def build_client_structural_indices(struct: List[dict]) -> List[dict]:
#     """
#     Transforms the path structure into client-specific row and parent indices.
#     row_index = path_level (depth)
#     parent_row_index = node_index (the value inside [])
#     """
#     results = []
#     for level, node in enumerate(struct, start=1):
#         results.append({
#             "path_level": level,
#             "node_name": node['name'],
#             "node_index": node['index'] if node['index'] is not None else "null",
#             "full_path": node['name'] # Or rebuild the full string up to this point
#         })
#     return results


from __future__ import annotations
import re
from functools import lru_cache
from typing import List, Dict, Optional

# Using lru_cache for Memoization - makes repetitive key parsing near-instant
@lru_cache(maxsize=1024)
def parse_path_structure(afd_key: str) -> List[dict]:
    """
    Parses the key into structural tokens using fast string operations.
    Memorized to prevent re-parsing identical keys across thousands of rows.
    """
    if not afd_key:
        return []
        
    tokens = []
    # Native split is fast, but we only do it once per unique key due to cache
    parts = afd_key.split('.')

    for part in parts:
        bracket_start = part.rfind('[')
        
        # Ensure it has '[' and ends with ']'
        if bracket_start != -1 and part.endswith(']'):
            name = part[:bracket_start]
            idx_str = part[bracket_start + 1 : -1]
            # Fast digit check
            idx = int(idx_str) if idx_str.isdigit() else None
            tokens.append({"name": name, "index": idx})
        else:
            tokens.append({"name": part, "index": None})
            
    return tokens

def extract_indices(afd_key: str) -> List[int]:
    """
    Uses the memoized parser to extract indices without redundant string work.
    """
    struct = parse_path_structure(afd_key)
    return [node["index"] for node in struct if node["index"] is not None]

@lru_cache(maxsize=1024)
def canonicalize_key(afd_key: str) -> str:
    """
    REFINED: Uses the structural tokens to build the template string.
    This ensures canonicalization and parsing are always in sync.
    """
    if not afd_key:
        return afd_key
    
    struct = parse_path_structure(afd_key)
    clean_parts = []
    for node in struct:
        if node["index"] is not None:
            clean_parts.append(f"{node['name']}[]")
        else:
            clean_parts.append(node["name"])
            
    k = ".".join(clean_parts)
    # Prefix normalization (kept RegEx here as it's only for the prefix)
    k = re.sub(r'^ADF_', 'AXX_', k, flags=re.IGNORECASE)
    k = re.sub(r'^AFD_', 'AXX_', k, flags=re.IGNORECASE)
    return k

def build_client_structural_indices(afd_key: str) -> List[dict]:
    """
    Transforms the path structure into client-specific rows.
    Fixes the 'full_path' flaw to match the client's example table.
    """
    struct = parse_path_structure(afd_key)
    results = []
    for level, node in enumerate(struct, start=1):
        results.append({
            "path_level": level,
            "node_name": node['name'],
            "node_index": node['index'] if node['index'] is not None else "null",
            # Logic for client screenshot: show full path only on level 1
            "full_path": afd_key if level == 1 else "..." 
        })
    return results