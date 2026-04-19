
"""
Generate CRF Observation Data DIRECTLY FROM ORACLE DATABASE (no Excel).
Outputs Crf_Observation_Data.xlsx

Fully dynamic – handles dotted SECTION_NAME hierarchy correctly.
Fixed:
- Dotted sections: recursive path building
- Checkbox fields: adds inner [0] for array-like keys (issues[], reasons[], etc.)
"""

# import pandas as pd
# from pathlib import Path
# from typing import Optional, Dict
# import os
# from dotenv import load_dotenv
# import oracledb
# import sqlalchemy

# # Load environment variables
# load_dotenv()

# # ────────────────────────────────────────────────
# # Database Configuration (use .env file)
# # ────────────────────────────────────────────────

# DB_CONFIG = {
#     'user': os.getenv('DB_USER', 'system'),
#     'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
#     'host': os.getenv('DB_HOST', 'localhost'),
#     'port': os.getenv('DB_PORT', '1521'),
#     'service': os.getenv('DB_SERVICE', 'xepdb1'),
# }

# # Optional: path to Oracle Instant Client if using thick mode
# ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')

# if ORACLE_CLIENT_LIB:
#     print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
#     oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)

# class CRFDataGenerator:
#     """Generates Crf_Observation_Data from Oracle database only."""

#     def __init__(self):
#         self.conn = None
#         self.crf_sections_df = None
#         self.std_sections_df = None
#         self.std_obs_df = None
#         self.sections_map: Dict[str, dict] = {}

#     def connect(self):
#         """Establish Oracle connection."""
#         print("Connecting to Oracle database...")
#         dsn = oracledb.makedsn(
#             host=DB_CONFIG['host'],
#             port=int(DB_CONFIG['port']),
#             service_name=DB_CONFIG['service']
#         )
#         self.conn = oracledb.connect(
#             user=DB_CONFIG['user'],
#             password=DB_CONFIG['password'],
#             dsn=dsn
#         )
#         print("Database connection established.")

#     def load_data_from_db(self):
#         """Load all metadata directly from Oracle tables."""
#         if self.conn is None:
#             self.connect()

#         print("Loading metadata from database...")

#         engine = sqlalchemy.create_engine(
#             f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
#             f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/?service_name={DB_CONFIG['service']}"
#         )

#         with engine.connect() as conn:
#             # CRF_SECTIONS
#             self.crf_sections_df = pd.read_sql(
#                 "SELECT CRF_NAME, SEQUENCE, SECTION_NAME FROM CRF_SECTIONS ORDER BY CRF_NAME, SEQUENCE",
#                 conn
#             )

#             # STD_SECTIONS
#             self.std_sections_df = pd.read_sql(
#                 "SELECT SECTION_NAME, SECTION_DESCRIPTION, SECTION_KEY, MULTIPLE_RECORDS_FLAG FROM STD_SECTIONS",
#                 conn
#             )

#             # STD_SECTION_OBSERVATIONS
#             self.std_obs_df = pd.read_sql(
#                 "SELECT SECTION_NAME, SEQUENCE, OBSERVATION_CODE, KEY, CHECKBOX FROM STD_SECTION_OBSERVATIONS ORDER BY SECTION_NAME, SEQUENCE",
#                 conn
#             )

#         # Normalize column names to uppercase
#         for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
#             df.columns = df.columns.str.strip().str.upper()

#         # Build lookup map
#         self.sections_map = self.std_sections_df.set_index('SECTION_NAME').to_dict('index')

#         # Diagnostic info
#         dotted = [n for n in self.sections_map if '.' in n]
#         print(f"Detected {len(dotted)} dotted/hierarchical section names: {dotted[:5]}{'...' if len(dotted)>5 else ''}")
#         print(f"CRF sections loaded: {len(self.crf_sections_df)}")
#         print(f"Standard sections loaded: {len(self.std_sections_df)}")
#         print(f"Observations loaded: {len(self.std_obs_df)}")

#     def get_section_key(self, section_name: str) -> Optional[str]:
#         return self.sections_map.get(section_name, {}).get('SECTION_KEY')

#     def is_multiple(self, section_name: str) -> bool:
#         return self.sections_map.get(section_name, {}).get('MULTIPLE_RECORDS_FLAG') == 'Y'

#     def build_base_pattern(self, section_name: str) -> str:
#         """Build hierarchical path with correct dotted name support."""
#         if not section_name or pd.isna(section_name):
#             return ""

#         levels = section_name.split('.')
#         path_parts = []
#         current_parts = []

#         for level in levels:
#             current_parts.append(level)
#             full_name = '.'.join(current_parts)
#             key = self.get_section_key(full_name)
#             if key:
#                 part = str(key)
#                 if self.is_multiple(full_name):
#                     part += '[]'  # placeholder for later replacement
#                 path_parts.append(part)

#         return ".".join(path_parts)

#     def generate_observations(self) -> pd.DataFrame:
#         print("Generating observation mappings (using [0] for repeatable sections)...")

#         results = []

#         for _, crf_row in self.crf_sections_df.iterrows():
#             crf_name = crf_row.get('CRF_NAME')
#             section_name = crf_row.get('SECTION_NAME')

#             if pd.isna(section_name) or not section_name.strip():
#                 continue

#             obs_rows = self.std_obs_df[self.std_obs_df['SECTION_NAME'] == section_name]

#             for _, obs in obs_rows.iterrows():
#                 obs_code = obs.get('OBSERVATION_CODE')
#                 obs_key   = obs.get('KEY')
#                 is_checkbox = obs.get('CHECKBOX', 'N') == 'Y'

#                 base = self.build_base_pattern(section_name)

#                 # Build final base (section path + observation key)
#                 final_base = base
#                 if base and pd.notna(obs_key) and obs_key.strip():
#                     final_base = f"{base}.{obs_key.strip()}"

#                 # ────────────────────────────────────────────────
#                 # Decide final key(s) to store
#                 # ────────────────────────────────────────────────
#                 if '[]' in final_base:
#                     # Has at least one repeatable ancestor → use [0] representative
#                     concrete_path = final_base.replace('[]', '[0]')

#                     if is_checkbox:
#                         inner = f"{concrete_path}[0]"
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': f"{obs_code}_QUESTION",
#                             'AFD_PATTERN_KEY': f"{inner}.question",
#                             'IS_CHECKBOX': 'Y',
#                             'ACTIVE': 'Y'
#                         })
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': f"{obs_code}_VALUE",
#                             'AFD_PATTERN_KEY': f"{inner}.value",
#                             'IS_CHECKBOX': 'Y',
#                             'ACTIVE': 'Y'
#                         })
#                     else:
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': obs_code,
#                             'AFD_PATTERN_KEY': concrete_path,
#                             'IS_CHECKBOX': 'N',
#                             'ACTIVE': 'Y'
#                         })

#                 else:
#                     # No repeaters in path
#                     if is_checkbox:
#                         # Still needs inner [0] for checkbox structure
#                         inner = f"{final_base}[0]" if final_base else "[0]"
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': f"{obs_code}_QUESTION",
#                             'AFD_PATTERN_KEY': f"{inner}.question",
#                             'IS_CHECKBOX': 'Y',
#                             'ACTIVE': 'Y'
#                         })
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': f"{obs_code}_VALUE",
#                             'AFD_PATTERN_KEY': f"{inner}.value",
#                             'IS_CHECKBOX': 'Y',
#                             'ACTIVE': 'Y'
#                         })
#                     else:
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': obs_code,
#                             'AFD_PATTERN_KEY': final_base,
#                             'IS_CHECKBOX': 'N',
#                             'ACTIVE': 'Y'
#                         })

#         df = pd.DataFrame(results, columns=[
#             'CRF_NAME', 'SECTION_NAME', 'OBSERVATION_CODE',
#             'AFD_PATTERN_KEY', 'IS_CHECKBOX', 'ACTIVE'
#         ])

#         print(f"Generated {len(df):,} mapping rows")
#         return df

#     def save_to_excel(self, df: pd.DataFrame, filename="Crf_Observation_Data.xlsx"):
#         output_dir = Path(__file__).parent / "data"
#         output_dir.mkdir(parents=True, exist_ok=True)

#         output_path = output_dir / filename
#         print(f"Saving to -> {output_path}")
#         df.to_excel(output_path, index=False)
#         print("File saved successfully.")

#     def run(self):
#         self.load_data_from_db()
#         df = self.generate_observations()
#         self.save_to_excel(df)
#         # No arrow char here in current file, but usually good to check
#         print("\nSample (first 15 rows):")
#         print(df.head(15).to_string(index=False))

#         # Optional: show some hierarchical examples
#         print("\nSample hierarchical keys:")
#         hierarchical = df[df['AFD_PATTERN_KEY'].str.contains(r'\[\d+\]')].head(8)
#         if not hierarchical.empty:
#             print(hierarchical[['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY']].to_string(index=False))


# def main():
#     gen = CRFDataGenerator()
#     try:
#         gen.run()
#     except Exception as e:
#         print(f"Error occurred: {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if gen.conn:
#             gen.conn.close()
#             print("Database connection closed.")


# if __name__ == "__main__":
#     main()
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
import re
import os
from dotenv import load_dotenv
import oracledb
import sqlalchemy

# Load environment variables
load_dotenv()

# ────────────────────────────────────────────────
# Database Configuration (use .env file)
# ────────────────────────────────────────────────

DB_CONFIG = {
    'user':     os.getenv('DB_USER',     'system'),
    'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
    'host':     os.getenv('DB_HOST',     'localhost'),
    'port':     os.getenv('DB_PORT',     '1521'),
    'service':  os.getenv('DB_SERVICE',  'xe'),
}

# Optional: path to Oracle Instant Client if using thick mode
ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')

if ORACLE_CLIENT_LIB:
    print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)


class CRFDataGenerator:
    """
    Generates Crf_Observation_Data from Oracle database — fully dynamic hierarchy.

    Key design decisions:
    - Every repeatable level gets [<index>] with the ACTUAL node index value
      inside the bracket — never empty [] or hardcoded [0].
    - Section-level index  → 0-based position of section within its CRF (from SEQUENCE)
    - Observation-level index → 0-based position of observation within its section (from SEQUENCE)
    - Checkbox array index  → same as observation index, applied to the checkbox node
    - No MAX_ARRAY limit — one row per observation, index derived from data.
    - Dotted observation keys (e.g. overlay.imageRecID) are checked for
      repeatability so nested array markers are never missed.
    - Checkbox observations generate both _QUESTION and _VALUE sub-fields.

    Example output (AFD_PATTERN_KEY):
      imageQualityAssessments.imageQualityAssessmentList[0].issues[1].question
      targetNodalLesions[2].lesionReasonNotEvaluableIssues[4].value
      previousNewUnequivocalLesions[11].overlay.imageRecID
    """

    def __init__(self):
        self.conn   = None
        self.engine = None

        self.crf_sections_df: pd.DataFrame = None
        self.std_sections_df: pd.DataFrame = None
        self.std_obs_df:      pd.DataFrame = None
        self.sections_map:    Dict[str, dict] = {}

    # ─────────────────────────────────────────────
    # Connection helpers
    # ─────────────────────────────────────────────

    def connect(self):
        """Establish Oracle connection (thin mode by default)."""
        print("Connecting to Oracle database...")
        dsn = oracledb.makedsn(
            host=DB_CONFIG['host'],
            port=int(DB_CONFIG['port']),
            service_name=DB_CONFIG['service']
        )
        self.conn = oracledb.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dsn=dsn
        )
        self.engine = sqlalchemy.create_engine(
            f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
            f"?service_name={DB_CONFIG['service']}"
        )
        print("Database connection established.")

    # ─────────────────────────────────────────────
    # Data loading
    # ─────────────────────────────────────────────

    def load_data_from_db(self):
        """Load all metadata directly from Oracle tables."""
        if self.conn is None:
            self.connect()

        print("Loading metadata from database...")

        with self.engine.connect() as conn:
            self.crf_sections_df = pd.read_sql(
                """
                SELECT CRF_NAME, SEQUENCE, SECTION_NAME
                FROM   CRF_SECTIONS
                ORDER  BY CRF_NAME, SEQUENCE
                """,
                conn
            )

            self.std_sections_df = pd.read_sql(
                """
                SELECT SECTION_NAME, SECTION_DESCRIPTION,
                       SECTION_KEY, MULTIPLE_RECORDS_FLAG
                FROM   STD_SECTIONS
                """,
                conn
            )

            self.std_obs_df = pd.read_sql(
                """
                SELECT SECTION_NAME, SEQUENCE, OBSERVATION_CODE, KEY, CHECKBOX
                FROM   STD_SECTION_OBSERVATIONS
                ORDER  BY SECTION_NAME, SEQUENCE
                """,
                conn
            )

        # Normalize column names
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.columns = df.columns.str.strip().str.upper()

        # Replace literal 'NULL' strings with actual NaN
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.replace('NULL', pd.NA, inplace=True)

        # Build lookup map: SECTION_NAME → {SECTION_KEY, MULTIPLE_RECORDS_FLAG, ...}
        self.sections_map = (
            self.std_sections_df
            .set_index('SECTION_NAME')
            .to_dict('index')
        )

        # Diagnostics
        dotted = [n for n in self.sections_map if '.' in str(n)]
        print(f"Detected {len(dotted)} dotted/hierarchical section names: "
              f"{dotted[:5]}{'...' if len(dotted) > 5 else ''}")
        print(f"CRF sections loaded      : {len(self.crf_sections_df)}")
        print(f"Standard sections loaded : {len(self.std_sections_df)}")
        print(f"Observations loaded      : {len(self.std_obs_df)}")

    # ─────────────────────────────────────────────
    # Lookup helpers
    # ─────────────────────────────────────────────

    def get_section_key(self, section_name: str) -> Optional[str]:
        """Return SECTION_KEY for a given section name, or None."""
        entry = self.sections_map.get(section_name, {})
        key   = entry.get('SECTION_KEY')
        if key is None or (isinstance(key, float) and pd.isna(key)):
            return None
        return str(key).strip() or None

    def is_multiple(self, section_name: str) -> bool:
        """Return True if the section has MULTIPLE_RECORDS_FLAG = 'Y'."""
        return self.sections_map.get(section_name, {}).get('MULTIPLE_RECORDS_FLAG') == 'Y'

    # ─────────────────────────────────────────────
    # Core pattern builders — ACTUAL index inside brackets
    # ─────────────────────────────────────────────

    def build_section_pattern(self, section_name: str, section_indices: List[int] = None) -> str:
        """
        Walk every dot-separated level of `section_name`.
        For each level that has a known SECTION_KEY, append that key.
        If the level is repeatable (MULTIPLE_RECORDS_FLAG = Y), pop the next
        index from `section_indices` and place it inside the bracket.

        This supports unlimited nesting depth — each repeatable level
        gets its OWN independent index from the list.

        Parameters
        ----------
        section_name    : e.g. 'LEVEL1.LEVEL2.LEVEL3.LEVEL4.LEVEL5'
        section_indices : list of 0-based indices, one per repeatable level
                          e.g. [2, 5, 0, 3, 1] for 5 repeatable levels
                          Falls back to [0] if not provided.

        Examples
        --------
        section_name='IMAGE_QUALITY', section_indices=[0]
        → key='imageQualityAssessments.imageQualityAssessmentList', multiple=Y
        → consumes index 0
        → returns 'imageQualityAssessments.imageQualityAssessmentList[0]'

        section_name='RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS.lesion', section_indices=[11, 3]
        → 'RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS' → multiple=Y → consumes index 11
              → 'previousNewUnequivocalLesions[11]'
        → 'RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS.lesion' → multiple=Y → consumes index 3
              → 'lesion[3]'
        → returns 'previousNewUnequivocalLesions[11].lesion[3]'

        section_name='L1.L2.L3.L4.L5', section_indices=[2, 5, 0, 3, 1]
        → each repeatable level gets its own index: L1[2].L2[5].L3[0].L4[3].L5[1]
        """
        if not section_name or pd.isna(section_name):
            return ""

        if section_indices is None:
            section_indices = [0]

        # Use a list iterator so each repeatable level pops its own index
        index_iter  = iter(section_indices)
        levels      = str(section_name).split('.')
        path_parts  = []
        accumulated = []

        for level in levels:
            accumulated.append(level)
            full_name = '.'.join(accumulated)
            key = self.get_section_key(full_name)

            if key:
                segment = key
                if self.is_multiple(full_name):
                    # Each repeatable level consumes its OWN index from the list
                    idx = next(index_iter, 0)   # default to 0 if list runs out
                    segment += f'[{idx}]'
                path_parts.append(segment)

        return '.'.join(path_parts)

    def build_obs_key_pattern(self, obs_key: str, obs_indices: List[int] = None) -> str:
        """
        Build the observation-level key pattern, placing the ACTUAL obs_index
        inside brackets wherever a sub-component is a known repeatable section.

        For obs_key tokens that are NOT in sections_map, they are kept as-is
        (preserving any existing bracket notation like overlays[0].imageRecID).

        Parameters
        ----------
        obs_key   : e.g. 'issues', 'overlay.imageRecID', 'lesionReasonNotEvaluableIssues'
        obs_index : 0-based position of this observation within its section

        Examples
        --------
        obs_key='issues', obs_index=3
        → 'issues' is NOT a known section → kept as-is → 'issues'
          (bracket applied by caller for checkbox: issues[3].question)

        obs_key='overlay.imageRecID', obs_index=5
        → 'overlay'     not a known section → kept as 'overlay'
        → 'imageRecID'  not a known section → kept as 'imageRecID'
        → returns 'overlay.imageRecID'

        obs_key='newMeasuredNodalLesions.count', obs_index=2
        → 'newMeasuredNodalLesions' IS a known multiple section
              → 'newMeasuredNodalLesions[2]'
        → '.count' appended as-is
        → returns 'newMeasuredNodalLesions[2].count'

        obs_key='a.b.c.d.e', obs_indices=[1, 2, 3, 4, 5]
        → each repeatable token gets its own index independently
        """
        if not obs_key or pd.isna(obs_key):
            return ""

        if obs_indices is None:
            obs_indices = [0]

        obs_key     = str(obs_key).strip()
        parts       = obs_key.split('.')
        result      = []
        accumulated = []
        index_iter  = iter(obs_indices)   # each repeatable token pops its own index

        for part in parts:
            # Strip any existing bracket notation before lookup
            clean = part.split('[')[0]
            accumulated.append(clean)
            joined = '.'.join(accumulated)

            # Check if this sub-path is a known section
            key = self.get_section_key(joined)
            if key:
                segment = key
                if self.is_multiple(joined):
                    idx = next(index_iter, 0)   # own index per repeatable token
                    segment += f'[{idx}]'
                result.append(segment)
            else:
                # Not a registered section — keep the original token as-is
                result.append(part)

        return '.'.join(result)

    # ─────────────────────────────────────────────
    # Main generation logic
    # ─────────────────────────────────────────────

    def _count_repeatable_levels(self, section_name: str) -> int:
        """Count how many dot-levels in section_name have MULTIPLE_RECORDS_FLAG=Y."""
        levels      = str(section_name).split('.')
        accumulated = []
        count       = 0
        for level in levels:
            accumulated.append(level)
            if self.is_multiple('.'.join(accumulated)):
                count += 1
        return count

    def _count_repeatable_obs_tokens(self, obs_key: str) -> int:
        """Count how many dot-tokens in obs_key are known repeatable sections."""
        if not obs_key or pd.isna(obs_key):
            return 0
        parts       = str(obs_key).strip().split('.')
        accumulated = []
        count       = 0
        for part in parts:
            clean = part.split('[')[0]
            accumulated.append(clean)
            if self.is_multiple('.'.join(accumulated)):
                count += 1
        return count

    def _build_index_maps(self):
        """
        Pre-compute 0-based index maps so every section and observation
        has a deterministic integer index based on its SEQUENCE position.

        Returns
        -------
        crf_section_indices : {crf_name: {section_name: 0-based-index}}
        obs_indices         : {section_name: {obs_code: 0-based-index}}
        """
        # Section index: 0-based position within each CRF, ordered by SEQUENCE
        crf_section_indices: Dict[str, Dict[str, int]] = {}
        for crf_name, group in self.crf_sections_df.groupby('CRF_NAME'):
            sorted_sections = group.sort_values('SEQUENCE')
            crf_section_indices[crf_name] = {
                str(row['SECTION_NAME']).strip(): idx
                for idx, (_, row) in enumerate(sorted_sections.iterrows())
            }

        # Observation index: 0-based position within each section, ordered by SEQUENCE
        obs_indices: Dict[str, Dict[str, int]] = {}
        for section_name, group in self.std_obs_df.groupby('SECTION_NAME'):
            sorted_obs = group.sort_values('SEQUENCE')
            obs_indices[str(section_name).strip()] = {
                str(row['OBSERVATION_CODE']).strip(): idx
                for idx, (_, row) in enumerate(sorted_obs.iterrows())
            }

        return crf_section_indices, obs_indices

    def generate_observations(self) -> pd.DataFrame:
        """
        For every CRF section × observation combination produce one row.

        Output columns
        --------------
        CRF_NAME        : name of the CRF form
        SECTION_NAME    : name of the section
        SECTION_INDEX   : 0-based position of the section in its CRF
        OBSERVATION_CODE: observation identifier
        OBS_INDEX       : 0-based position of the observation in its section
        AFD_PATTERN_KEY : full dot-notation path with ACTUAL [index] values
                          at every repeatable level (supports unlimited depth)
        IS_CHECKBOX     : Y / N
        ACTIVE          : Y

        Checkbox observations generate TWO rows:
          <OBS_CODE>_QUESTION  →  ....<obs_key>[<obs_index>].question
          <OBS_CODE>_VALUE     →  ....<obs_key>[<obs_index>].value

        Multi-level index strategy
        --------------------------
        section_indices : one entry per repeatable level in section_name
                          e.g. section with 3 repeatable levels → [idx, idx, idx]
                          each level gets the SAME section_index value by default,
                          but the list structure allows future per-level overrides.
        obs_indices     : one entry per repeatable token in obs_key
                          similarly structured as a list for full depth support.
        """
        print("Generating observation mappings (actual index values inside brackets, unlimited depth)...")

        crf_section_indices, obs_indices_map = self._build_index_maps()
        results: List[dict] = []

        for _, crf_row in self.crf_sections_df.iterrows():
            crf_name     = crf_row.get('CRF_NAME')
            section_name = crf_row.get('SECTION_NAME')

            if pd.isna(section_name) or not str(section_name).strip():
                continue

            section_name  = str(section_name).strip()
            section_index = crf_section_indices.get(crf_name, {}).get(section_name, 0)

            # Build index LIST for section — one entry per repeatable level in section_name
            # All levels share the same section_index value (can be extended per-level if needed)
            n_sec_levels    = self._count_repeatable_levels(section_name)
            section_indices = [section_index] * max(n_sec_levels, 1)

            # Section-level pattern — each repeatable level gets its own index from the list
            section_pattern = self.build_section_pattern(section_name, section_indices)

            # All observations for this section, in sequence order
            obs_rows = self.std_obs_df[
                self.std_obs_df['SECTION_NAME'] == section_name
            ].sort_values('SEQUENCE')

            for _, obs in obs_rows.iterrows():
                obs_code    = str(obs.get('OBSERVATION_CODE', '')).strip()
                obs_key_raw = obs.get('KEY', '')
                is_checkbox = str(obs.get('CHECKBOX', 'N')).strip() == 'Y'
                obs_index   = obs_indices_map.get(section_name, {}).get(obs_code, 0)

                if pd.isna(obs_key_raw) or not str(obs_key_raw).strip():
                    # No KEY defined — record with section pattern only
                    results.append(self._build_row(
                        crf_name, section_name, section_index,
                        obs_code, obs_index,
                        pattern=section_pattern,
                        is_checkbox=False
                    ))
                    continue

                obs_key_str = str(obs_key_raw).strip()

                # Build index LIST for obs_key — one entry per repeatable token in the key
                n_obs_levels = self._count_repeatable_obs_tokens(obs_key_str)
                obs_indices  = [obs_index] * max(n_obs_levels, 1)

                # Observation key pattern — each repeatable token gets its own index
                obs_pattern  = self.build_obs_key_pattern(obs_key_str, obs_indices)
                full_pattern = (
                    f"{section_pattern}.{obs_pattern}"
                    if section_pattern else obs_pattern
                )

                if is_checkbox:
                    # Checkbox node is itself an array → append [obs_index]
                    # Then expose .question and .value sub-fields
                    checkbox_base = f"{full_pattern}[{obs_index}]"
                    for suffix in ('question', 'value'):
                        results.append(self._build_row(
                            crf_name, section_name, section_index,
                            f"{obs_code}_{suffix.upper()}", obs_index,
                            pattern=f"{checkbox_base}.{suffix}",
                            is_checkbox=True
                        ))
                else:
                    results.append(self._build_row(
                        crf_name, section_name, section_index,
                        obs_code, obs_index,
                        pattern=full_pattern,
                        is_checkbox=False
                    ))

        columns = [
            'CRF_NAME',
            'SECTION_NAME',
            'SECTION_INDEX',
            'OBSERVATION_CODE',
            'OBS_INDEX',
            'AFD_PATTERN_KEY',
            'IS_CHECKBOX',
            'ACTIVE',
        ]

        df = pd.DataFrame(results, columns=columns)
        print(f"Generated {len(df):,} mapping rows")
        return df

    def _build_row(
        self,
        crf_name:      str,
        section_name:  str,
        section_index: int,
        obs_code:      str,
        obs_index:     int,
        pattern:       str,
        is_checkbox:   bool,
    ) -> dict:
        """Helper — produce a single output row dict."""
        return {
            'CRF_NAME':         crf_name,
            'SECTION_NAME':     section_name,
            'SECTION_INDEX':    section_index,
            'OBSERVATION_CODE': obs_code,
            'OBS_INDEX':        obs_index,
            'AFD_PATTERN_KEY':  pattern,   # actual [index] already embedded
            'IS_CHECKBOX':      'Y' if is_checkbox else 'N',
            'ACTIVE':           'Y',
        }

    # ─────────────────────────────────────────────
    # Output
    # ─────────────────────────────────────────────

    def save_to_excel(self, df: pd.DataFrame, filename: str = "Crf_Observation_Data_dynamic.xlsx"):
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / filename
        print(f"Saving to → {output_path}")
        df.to_excel(output_path, index=False)
        print("File saved successfully.")

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run(self):
        self.load_data_from_db()
        df = self.generate_observations()
        self.save_to_excel(df)

        # ── Summary diagnostics ──
        print("\n── Sample output (first 15 rows) ──")
        print(
            df.head(15)[
                ['CRF_NAME', 'SECTION_NAME', 'SECTION_INDEX',
                 'OBSERVATION_CODE', 'OBS_INDEX', 'AFD_PATTERN_KEY']
            ].to_string(index=False)
        )

        print("\n── Rows with [n] array indices ──")
        indexed = df[df['AFD_PATTERN_KEY'].str.contains(r'\[\d+\]', na=False)]
        print(f"Total rows with [n] : {len(indexed):,}")
        if not indexed.empty:
            print(
                indexed.head(12)[
                    ['SECTION_NAME', 'OBSERVATION_CODE',
                     'SECTION_INDEX', 'OBS_INDEX', 'AFD_PATTERN_KEY']
                ].to_string(index=False)
            )

        print("\n── Checkbox rows (both _QUESTION and _VALUE) ──")
        checkbox_rows = df[df['IS_CHECKBOX'] == 'Y']
        print(f"Total checkbox rows : {len(checkbox_rows):,}")
        if not checkbox_rows.empty:
            print(
                checkbox_rows.head(12)[
                    ['SECTION_NAME', 'OBSERVATION_CODE', 'OBS_INDEX', 'AFD_PATTERN_KEY']
                ].to_string(index=False)
            )

        print("\n── Deepest keys (most [n] markers) ──")
        df['_depth'] = df['AFD_PATTERN_KEY'].apply(
            lambda k: len(re.findall(r'\[\d+\]', str(k)))
        )
        deep = df.nlargest(10, '_depth')[
            ['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY', '_depth']
        ]
        print(deep.to_string(index=False))
        df.drop(columns=['_depth'], inplace=True)


# ─────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────

def main():
    gen = CRFDataGenerator()
    try:
        gen.run()
    except Exception as exc:
        print(f"\nError: {exc}")
        import traceback
        traceback.print_exc()
    finally:
        if gen.conn:
            gen.conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()