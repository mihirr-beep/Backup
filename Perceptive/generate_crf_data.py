
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
# #     main()
# import pandas as pd
# from pathlib import Path
# from typing import Optional, Dict, List
# import re
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
#     'user':     os.getenv('DB_USER',     'system'),
#     'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
#     'host':     os.getenv('DB_HOST',     'localhost'),
#     'port':     os.getenv('DB_PORT',     '1521'),
#     'service':  os.getenv('DB_SERVICE',  'xepdb1'),
# }

# # Optional: path to Oracle Instant Client if using thick mode
# ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')

# if ORACLE_CLIENT_LIB:
#     print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
#     oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)


# class CRFDataGenerator:
#     """
#     Generates Crf_Observation_Data from Oracle database — fully dynamic hierarchy.

#     Key design decisions:
#     - Every repeatable level gets [<index>] with the ACTUAL node index value
#       inside the bracket — never empty [] or hardcoded [0].
#     - Section-level index  → 0-based position of section within its CRF (from SEQUENCE)
#     - Observation-level index → 0-based position of observation within its section (from SEQUENCE)
#     - Checkbox array index  → same as observation index, applied to the checkbox node
#     - No MAX_ARRAY limit — one row per observation, index derived from data.
#     - Dotted observation keys (e.g. overlay.imageRecID) are checked for
#       repeatability so nested array markers are never missed.
#     - Checkbox observations generate both _QUESTION and _VALUE sub-fields.

#     Example output (AFD_PATTERN_KEY):
#       imageQualityAssessments.imageQualityAssessmentList[0].issues[1].question
#       targetNodalLesions[2].lesionReasonNotEvaluableIssues[4].value
#       previousNewUnequivocalLesions[11].overlay.imageRecID
#     """

#     def __init__(self):
#         self.conn   = None
#         self.engine = None

#         self.crf_sections_df: pd.DataFrame = None
#         self.std_sections_df: pd.DataFrame = None
#         self.std_obs_df:      pd.DataFrame = None
#         self.sections_map:    Dict[str, dict] = {}

#     # ─────────────────────────────────────────────
#     # Connection helpers
#     # ─────────────────────────────────────────────

#     def connect(self):
#         """Establish Oracle connection (thin mode by default)."""
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
#         self.engine = sqlalchemy.create_engine(
#             f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
#             f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
#             f"?service_name={DB_CONFIG['service']}"
#         )
#         print("Database connection established.")

#     # ─────────────────────────────────────────────
#     # Data loading
#     # ─────────────────────────────────────────────

#     def load_data_from_db(self):
#         """Load all metadata directly from Oracle tables."""
#         if self.conn is None:
#             self.connect()

#         print("Loading metadata from database...")

#         with self.engine.connect() as conn:
#             self.crf_sections_df = pd.read_sql(
#                 """
#                 SELECT CRF_NAME, SEQUENCE, SECTION_NAME
#                 FROM   CRF_SECTIONS
#                 ORDER  BY CRF_NAME, SEQUENCE
#                 """,
#                 conn
#             )

#             self.std_sections_df = pd.read_sql(
#                 """
#                 SELECT SECTION_NAME, SECTION_DESCRIPTION,
#                        SECTION_KEY, MULTIPLE_RECORDS_FLAG
#                 FROM   STD_SECTIONS
#                 """,
#                 conn
#             )

#             self.std_obs_df = pd.read_sql(
#                 """
#                 SELECT SECTION_NAME, SEQUENCE, OBSERVATION_CODE, KEY, CHECKBOX
#                 FROM   STD_SECTION_OBSERVATIONS
#                 ORDER  BY SECTION_NAME, SEQUENCE
#                 """,
#                 conn
#             )

#         # Normalize column names
#         for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
#             df.columns = df.columns.str.strip().str.upper()

#         # Replace literal 'NULL' strings with actual NaN
#         for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
#             df.replace('NULL', pd.NA, inplace=True)

#         # Build lookup map: SECTION_NAME → {SECTION_KEY, MULTIPLE_RECORDS_FLAG, ...}
#         self.sections_map = (
#             self.std_sections_df
#             .set_index('SECTION_NAME')
#             .to_dict('index')
#         )

#         # Diagnostics
#         dotted = [n for n in self.sections_map if '.' in str(n)]
#         print(f"Detected {len(dotted)} dotted/hierarchical section names: "
#               f"{dotted[:5]}{'...' if len(dotted) > 5 else ''}")
#         print(f"CRF sections loaded      : {len(self.crf_sections_df)}")
#         print(f"Standard sections loaded : {len(self.std_sections_df)}")
#         print(f"Observations loaded      : {len(self.std_obs_df)}")

#     # ─────────────────────────────────────────────
#     # Lookup helpers
#     # ─────────────────────────────────────────────

#     def get_section_key(self, section_name: str) -> Optional[str]:
#         """Return SECTION_KEY for a given section name, or None."""
#         entry = self.sections_map.get(section_name, {})
#         key   = entry.get('SECTION_KEY')
#         if key is None or (isinstance(key, float) and pd.isna(key)):
#             return None
#         return str(key).strip() or None

#     def is_multiple(self, section_name: str) -> bool:
#         """Return True if the section has MULTIPLE_RECORDS_FLAG = 'Y'."""
#         return self.sections_map.get(section_name, {}).get('MULTIPLE_RECORDS_FLAG') == 'Y'

#     # ─────────────────────────────────────────────
#     # Core pattern builders — ACTUAL index inside brackets
#     # ─────────────────────────────────────────────

#     def build_section_pattern(self, section_name: str, section_index: int = 0) -> str:
#         """
#         Walk every dot-separated level of `section_name`.
#         For each level that has a known SECTION_KEY, append that key.
#         If the level is repeatable (MULTIPLE_RECORDS_FLAG = Y), append
#         [<section_index>] — the ACTUAL index value inside the bracket.

#         Parameters
#         ----------
#         section_name  : e.g. 'RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS.lesion'
#         section_index : 0-based position of this section in the CRF

#         Example
#         -------
#         section_name='IMAGE_QUALITY', section_index=0
#         → key='imageQualityAssessments.imageQualityAssessmentList', multiple=Y
#         → returns 'imageQualityAssessments.imageQualityAssessmentList[0]'

#         section_name='RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS.lesion', section_index=11
#         → 'RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS' → key='previousNewUnequivocalLesions', multiple=Y
#               → 'previousNewUnequivocalLesions[11]'
#         → 'RECIST11_PREVNEW_UNEQUIVOCAL_LESIONS.lesion' → key='lesion', multiple=Y
#               → 'lesion[11]'
#         → returns 'previousNewUnequivocalLesions[11].lesion[11]'
#         """
#         if not section_name or pd.isna(section_name):
#             return ""

#         levels      = str(section_name).split('.')
#         path_parts  = []
#         accumulated = []

#         for level in levels:
#             accumulated.append(level)
#             full_name = '.'.join(accumulated)
#             key = self.get_section_key(full_name)

#             if key:
#                 segment = key
#                 if self.is_multiple(full_name):
#                     segment += f'[{section_index}]'   # ← ACTUAL index value inside bracket
#                 path_parts.append(segment)

#         return '.'.join(path_parts)

#     def build_obs_key_pattern(self, obs_key: str, obs_index: int = 0) -> str:
#         """
#         Build the observation-level key pattern, placing the ACTUAL obs_index
#         inside brackets wherever a sub-component is a known repeatable section.

#         For obs_key tokens that are NOT in sections_map, they are kept as-is
#         (preserving any existing bracket notation like overlays[0].imageRecID).

#         Parameters
#         ----------
#         obs_key   : e.g. 'issues', 'overlay.imageRecID', 'lesionReasonNotEvaluableIssues'
#         obs_index : 0-based position of this observation within its section

#         Examples
#         --------
#         obs_key='issues', obs_index=3
#         → 'issues' is NOT a known section → kept as-is → 'issues'
#           (bracket applied by caller for checkbox: issues[3].question)

#         obs_key='overlay.imageRecID', obs_index=5
#         → 'overlay'     not a known section → kept as 'overlay'
#         → 'imageRecID'  not a known section → kept as 'imageRecID'
#         → returns 'overlay.imageRecID'

#         obs_key='newMeasuredNodalLesions.count', obs_index=2
#         → 'newMeasuredNodalLesions' IS a known multiple section
#               → 'newMeasuredNodalLesions[2]'
#         → '.count' appended as-is
#         → returns 'newMeasuredNodalLesions[2].count'
#         """
#         if not obs_key or pd.isna(obs_key):
#             return ""

#         obs_key = str(obs_key).strip()
#         parts   = obs_key.split('.')
#         result  = []
#         accumulated = []

#         for part in parts:
#             # Strip any existing bracket notation before lookup
#             clean = part.split('[')[0]
#             accumulated.append(clean)
#             joined = '.'.join(accumulated)

#             # Check if this sub-path is a known section
#             key = self.get_section_key(joined)
#             if key:
#                 segment = key
#                 if self.is_multiple(joined):
#                     segment += f'[{obs_index}]'   # ← ACTUAL index value inside bracket
#                 result.append(segment)
#             else:
#                 # Not a registered section — keep the original token as-is
#                 result.append(part)

#         return '.'.join(result)

#     # ─────────────────────────────────────────────
#     # Main generation logic
#     # ─────────────────────────────────────────────

#     def _build_index_maps(self):
#         """
#         Pre-compute 0-based index maps so every section and observation
#         has a deterministic integer index based on its SEQUENCE position.

#         Returns
#         -------
#         crf_section_indices : {crf_name: {section_name: 0-based-index}}
#         obs_indices         : {section_name: {obs_code: 0-based-index}}
#         """
#         # Section index: 0-based position within each CRF, ordered by SEQUENCE
#         crf_section_indices: Dict[str, Dict[str, int]] = {}
#         for crf_name, group in self.crf_sections_df.groupby('CRF_NAME'):
#             sorted_sections = group.sort_values('SEQUENCE')
#             crf_section_indices[crf_name] = {
#                 str(row['SECTION_NAME']).strip(): idx
#                 for idx, (_, row) in enumerate(sorted_sections.iterrows())
#             }

#         # Observation index: 0-based position within each section, ordered by SEQUENCE
#         obs_indices: Dict[str, Dict[str, int]] = {}
#         for section_name, group in self.std_obs_df.groupby('SECTION_NAME'):
#             sorted_obs = group.sort_values('SEQUENCE')
#             obs_indices[str(section_name).strip()] = {
#                 str(row['OBSERVATION_CODE']).strip(): idx
#                 for idx, (_, row) in enumerate(sorted_obs.iterrows())
#             }

#         return crf_section_indices, obs_indices

#     def generate_observations(self) -> pd.DataFrame:
#         """
#         For every CRF section × observation combination produce one row.

#         Output columns
#         --------------
#         CRF_NAME        : name of the CRF form
#         SECTION_NAME    : name of the section
#         SECTION_INDEX   : 0-based position of the section in its CRF
#         OBSERVATION_CODE: observation identifier
#         OBS_INDEX       : 0-based position of the observation in its section
#         AFD_PATTERN_KEY : full dot-notation path with ACTUAL [index] values
#                           at every repeatable level
#         IS_CHECKBOX     : Y / N
#         ACTIVE          : Y

#         Checkbox observations generate TWO rows:
#           <OBS_CODE>_QUESTION  →  ....<obs_key>[<obs_index>].question
#           <OBS_CODE>_VALUE     →  ....<obs_key>[<obs_index>].value
#         """
#         print("Generating observation mappings (actual index values inside brackets)...")

#         crf_section_indices, obs_indices = self._build_index_maps()
#         results: List[dict] = []

#         for _, crf_row in self.crf_sections_df.iterrows():
#             crf_name     = crf_row.get('CRF_NAME')
#             section_name = crf_row.get('SECTION_NAME')

#             if pd.isna(section_name) or not str(section_name).strip():
#                 continue

#             section_name  = str(section_name).strip()
#             section_index = crf_section_indices.get(crf_name, {}).get(section_name, 0)

#             # Section-level pattern with ACTUAL section index inside brackets
#             section_pattern = self.build_section_pattern(section_name, section_index)

#             # All observations for this section, in sequence order
#             obs_rows = self.std_obs_df[
#                 self.std_obs_df['SECTION_NAME'] == section_name
#             ].sort_values('SEQUENCE')

#             for _, obs in obs_rows.iterrows():
#                 obs_code    = str(obs.get('OBSERVATION_CODE', '')).strip()
#                 obs_key_raw = obs.get('KEY', '')
#                 is_checkbox = str(obs.get('CHECKBOX', 'N')).strip() == 'Y'
#                 obs_index   = obs_indices.get(section_name, {}).get(obs_code, 0)

#                 if pd.isna(obs_key_raw) or not str(obs_key_raw).strip():
#                     # No KEY defined — record with section pattern only
#                     results.append(self._build_row(
#                         crf_name, section_name, section_index,
#                         obs_code, obs_index,
#                         pattern=section_pattern,
#                         is_checkbox=False
#                     ))
#                     continue

#                 # Observation key pattern with ACTUAL obs index inside brackets
#                 obs_pattern  = self.build_obs_key_pattern(str(obs_key_raw).strip(), obs_index)
#                 full_pattern = (
#                     f"{section_pattern}.{obs_pattern}"
#                     if section_pattern else obs_pattern
#                 )

#                 if is_checkbox:
#                     # Checkbox node is itself an array → append [obs_index]
#                     # Then expose .question and .value sub-fields
#                     checkbox_base = f"{full_pattern}[{obs_index}]"
#                     for suffix in ('question', 'value'):
#                         results.append(self._build_row(
#                             crf_name, section_name, section_index,
#                             f"{obs_code}_{suffix.upper()}", obs_index,
#                             pattern=f"{checkbox_base}.{suffix}",
#                             is_checkbox=True
#                         ))
#                 else:
#                     results.append(self._build_row(
#                         crf_name, section_name, section_index,
#                         obs_code, obs_index,
#                         pattern=full_pattern,
#                         is_checkbox=False
#                     ))

#         columns = [
#             'CRF_NAME',
#             'SECTION_NAME',
#             'SECTION_INDEX',
#             'OBSERVATION_CODE',
#             'OBS_INDEX',
#             'AFD_PATTERN_KEY',
#             'IS_CHECKBOX',
#             'ACTIVE',
#         ]

#         df = pd.DataFrame(results, columns=columns)
#         print(f"Generated {len(df):,} mapping rows")
#         return df

#     def _build_row(
#         self,
#         crf_name:      str,
#         section_name:  str,
#         section_index: int,
#         obs_code:      str,
#         obs_index:     int,
#         pattern:       str,
#         is_checkbox:   bool,
#     ) -> dict:
#         """Helper — produce a single output row dict."""
#         return {
#             'CRF_NAME':         crf_name,
#             'SECTION_NAME':     section_name,
#             'SECTION_INDEX':    section_index,
#             'OBSERVATION_CODE': obs_code,
#             'OBS_INDEX':        obs_index,
#             'AFD_PATTERN_KEY':  pattern,   # actual [index] already embedded
#             'IS_CHECKBOX':      'Y' if is_checkbox else 'N',
#             'ACTIVE':           'Y',
#         }

#     # ─────────────────────────────────────────────
#     # Output
#     # ─────────────────────────────────────────────

#     def save_to_excel(self, df: pd.DataFrame, filename: str = "Crf_Observation_Data_dynamic.xlsx"):
#         output_dir = Path(__file__).parent / "data"
#         output_dir.mkdir(parents=True, exist_ok=True)
#         output_path = output_dir / filename
#         print(f"Saving to → {output_path}")
#         df.to_excel(output_path, index=False)
#         print("File saved successfully.")

#     # ─────────────────────────────────────────────
#     # Entry point
#     # ─────────────────────────────────────────────

#     def run(self):
#         self.load_data_from_db()
#         df = self.generate_observations()
#         self.save_to_excel(df)

#         # ── Summary diagnostics ──
#         print("\n── Sample output (first 15 rows) ──")
#         print(
#             df.head(15)[
#                 ['CRF_NAME', 'SECTION_NAME', 'SECTION_INDEX',
#                  'OBSERVATION_CODE', 'OBS_INDEX', 'AFD_PATTERN_KEY']
#             ].to_string(index=False)
#         )

#         print("\n── Rows with [n] array indices ──")
#         indexed = df[df['AFD_PATTERN_KEY'].str.contains(r'\[\d+\]', na=False)]
#         print(f"Total rows with [n] : {len(indexed):,}")
#         if not indexed.empty:
#             print(
#                 indexed.head(12)[
#                     ['SECTION_NAME', 'OBSERVATION_CODE',
#                      'SECTION_INDEX', 'OBS_INDEX', 'AFD_PATTERN_KEY']
#                 ].to_string(index=False)
#             )

#         print("\n── Checkbox rows (both _QUESTION and _VALUE) ──")
#         checkbox_rows = df[df['IS_CHECKBOX'] == 'Y']
#         print(f"Total checkbox rows : {len(checkbox_rows):,}")
#         if not checkbox_rows.empty:
#             print(
#                 checkbox_rows.head(12)[
#                     ['SECTION_NAME', 'OBSERVATION_CODE', 'OBS_INDEX', 'AFD_PATTERN_KEY']
#                 ].to_string(index=False)
#             )

#         print("\n── Deepest keys (most [n] markers) ──")
#         df['_depth'] = df['AFD_PATTERN_KEY'].apply(
#             lambda k: len(re.findall(r'\[\d+\]', str(k)))
#         )
#         deep = df.nlargest(10, '_depth')[
#             ['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY', '_depth']
#         ]
#         print(deep.to_string(index=False))
#         df.drop(columns=['_depth'], inplace=True)


# # ─────────────────────────────────────────────────────
# # Main
# # ─────────────────────────────────────────────────────

# def main():
#     gen = CRFDataGenerator()
#     try:
#         gen.run()
#     except Exception as exc:
#         print(f"\nError: {exc}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if gen.conn:
#             gen.conn.close()
#             print("\nDatabase connection closed.")


# if __name__ == "__main__":
#     main()

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
import re
import os
import oracledb
import sqlalchemy

# ────────────────────────────────────────────────
# Database Configuration (Hardcoded for 'system')
# ────────────────────────────────────────────────
DB_USER     = 'system'
DB_PASSWORD = 'SYSTEM'  # Change this if your system password is different
DB_HOST     = 'localhost'
DB_PORT     = '1521'
DB_SERVICE  = 'xe'

# Optional: path to Oracle Instant Client if using thick mode
ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')
if ORACLE_CLIENT_LIB:
    print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)

class CRFDataGenerator:
    def __init__(self):
        self.conn   = None
        self.engine = None
        self.crf_sections_df: pd.DataFrame = None
        self.std_sections_df: pd.DataFrame = None
        self.std_obs_df:      pd.DataFrame = None
        self.sections_map:    Dict[str, dict] = {}

    def connect(self):
        print(f"Connecting to Oracle database as user '{DB_USER}'...")
        dsn = oracledb.makedsn(host=DB_HOST, port=int(DB_PORT), service_name=DB_SERVICE)
        self.conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
        
        self.engine = sqlalchemy.create_engine(
            f"oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/?service_name={DB_SERVICE}"
        )
        print("Database connection established.")

    def load_data_from_db(self):
        if self.conn is None:
            self.connect()

        print("Loading metadata from database...")
        with self.engine.connect() as conn:
            # Notice the double quotes around table and column names to prevent ORA-00942
            self.crf_sections_df = pd.read_sql(
                'SELECT "CRF_NAME", "SEQUENCE", "SECTION_NAME" FROM "CRF_SECTIONS" ORDER BY "CRF_NAME", "SEQUENCE"', conn
            )
            self.std_sections_df = pd.read_sql(
                'SELECT "SECTION_NAME", "SECTION_DESCRIPTION", "SECTION_KEY", "MULTIPLE_RECORDS_FLAG" FROM "STD_SECTIONS"', conn
            )
            self.std_obs_df = pd.read_sql(
                'SELECT "SECTION_NAME", "SEQUENCE", "OBSERVATION_CODE", "KEY", "CHECKBOX" FROM "STD_SECTION_OBSERVATIONS" ORDER BY "SECTION_NAME", "SEQUENCE"', conn
            )

        # Normalize column names for pandas usage
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.columns = df.columns.str.strip().str.upper()
            df.replace('NULL', pd.NA, inplace=True)

        self.sections_map = self.std_sections_df.set_index('SECTION_NAME').to_dict('index')
        print(f"Loaded {len(self.crf_sections_df)} CRF Sections, {len(self.std_sections_df)} Std Sections, {len(self.std_obs_df)} Observations.")

    def get_section_key(self, section_name: str) -> Optional[str]:
        entry = self.sections_map.get(section_name, {})
        key   = entry.get('SECTION_KEY')
        if key is None or (isinstance(key, float) and pd.isna(key)): return None
        return str(key).strip() or None

    def is_multiple(self, section_name: str) -> bool:
        return self.sections_map.get(section_name, {}).get('MULTIPLE_RECORDS_FLAG') == 'Y'

    def build_section_pattern(self, section_name: str) -> str:
        """Builds path with [*] wildcard for repeatable sections."""
        if not section_name or pd.isna(section_name): return ""
        
        levels = str(section_name).split('.')
        path_parts, accumulated = [], []

        for level in levels:
            accumulated.append(level)
            full_name = '.'.join(accumulated)
            key = self.get_section_key(full_name)

            if key:
                segment = key + ('[*]' if self.is_multiple(full_name) else '')
                path_parts.append(segment)

        return '.'.join(path_parts)

    def build_obs_key_pattern(self, obs_key: str) -> str:
        """Applies [*] wildcards to observation keys."""
        if not obs_key or pd.isna(obs_key): return ""
        
        parts = str(obs_key).strip().split('.')
        result, accumulated = [], []

        for part in parts:
            clean = part.split('[')[0]
            accumulated.append(clean)
            joined = '.'.join(accumulated)
            key = self.get_section_key(joined)

            if key:
                segment = key + ('[*]' if self.is_multiple(joined) else '')
                result.append(segment)
            else:
                # Fix hardcoded metadata arrays like overlays[0] to overlays[*]
                result.append(part.replace('[0]', '[*]'))

        return '.'.join(result)

    def generate_templates(self) -> pd.DataFrame:
        """Generates the base [*] templates from metadata."""
        print("Generating [*] wildcard templates...")
        results: List[dict] = []

        for _, crf_row in self.crf_sections_df.iterrows():
            crf_name     = crf_row.get('CRF_NAME')
            section_name = crf_row.get('SECTION_NAME')

            if pd.isna(section_name) or not str(section_name).strip(): continue

            section_name = str(section_name).strip()
            section_pattern = self.build_section_pattern(section_name)
            obs_rows = self.std_obs_df[self.std_obs_df['SECTION_NAME'] == section_name]

            for _, obs in obs_rows.iterrows():
                obs_code    = str(obs.get('OBSERVATION_CODE', '')).strip()
                obs_key_raw = obs.get('KEY', '')
                is_checkbox = str(obs.get('CHECKBOX', 'N')).strip() == 'Y'

                if pd.isna(obs_key_raw) or not str(obs_key_raw).strip():
                    results.append(self._build_row(crf_name, section_name, obs_code, section_pattern, False))
                    continue

                obs_pattern  = self.build_obs_key_pattern(str(obs_key_raw).strip())
                full_pattern = f"{section_pattern}.{obs_pattern}" if section_pattern else obs_pattern

                if is_checkbox:
                    checkbox_base = full_pattern if full_pattern.endswith(']') else f"{full_pattern}[*]"
                    results.append(self._build_row(crf_name, section_name, f"{obs_code}_QUESTION", f"{checkbox_base}.question", True))
                    results.append(self._build_row(crf_name, section_name, f"{obs_code}_VALUE", f"{checkbox_base}.value", True))
                else:
                    results.append(self._build_row(crf_name, section_name, obs_code, full_pattern, False))

        return pd.DataFrame(results)

    def resolve_actual_data(self, templates_df: pd.DataFrame) -> pd.DataFrame:
        """Scans ANALYSIS_FORM_DATA and replaces [*] with real patient indexes."""
        print("Scanning ANALYSIS_FORM_DATA to extract real indices...")
        
        try:
            with self.engine.connect() as conn:
                # Double quotes around table and column!
                actual_keys_df = pd.read_sql('SELECT DISTINCT "KEY" FROM "ANALYSIS_FORM_DATA" WHERE "KEY" IS NOT NULL', conn)
            actual_keys_df.columns = actual_keys_df.columns.str.upper()
            actual_keys = actual_keys_df['KEY'].tolist()
        except Exception as e:
            print(f"Warning: Could not read ANALYSIS_FORM_DATA. Make sure the table exists. Error: {e}")
            actual_keys = []
        
        resolved_rows = []

        for _, row in templates_df.iterrows():
            pattern = str(row['AFD_PATTERN_KEY'])

            if '[*]' in pattern:
                # Convert [*] into a Regex to find digits: \[(\d+)\]
                # Example: measuredNonNodalLesions\[(\d+)\]\.overlay\.length
                regex_str = re.escape(pattern).replace(r'\[\*\]', r'\[(\d+)\]')
                regex = re.compile('^' + regex_str + '$')

                found_match = False
                for actual_key in actual_keys:
                    match = regex.match(actual_key)
                    if match:
                        found_match = True
                        new_row = row.copy()
                        new_row['AFD_PATTERN_KEY'] = actual_key  # Inject the EXACT EAV Key from the DB
                        new_row['ACTUAL_INDEX'] = "_".join(match.groups()) # Captures '0', '1', etc.
                        resolved_rows.append(new_row)
                
                # If no patient data exists for this template yet, keep it so we don't lose the mapping
                if not found_match:
                    new_row = row.copy()
                    new_row['ACTUAL_INDEX'] = 'NO_DATA_YET'
                    resolved_rows.append(new_row)
            else:
                # No wildcards, just standard static keys (e.g. overallAssessment.value)
                new_row = row.copy()
                new_row['ACTUAL_INDEX'] = 'N/A'
                resolved_rows.append(new_row)

        resolved_df = pd.DataFrame(resolved_rows)
        matched_count = len(resolved_df[~resolved_df['ACTUAL_INDEX'].isin(['NO_DATA_YET', 'N/A'])])
        print(f"Successfully matched {matched_count} actual EAV keys in the database.")
        
        return resolved_df

    def _build_row(self, crf_name: str, section_name: str, obs_code: str, pattern: str, is_checkbox: bool) -> dict:
        return {
            'CRF_NAME':         crf_name,
            'SECTION_NAME':     section_name,
            'OBSERVATION_CODE': obs_code,
            'AFD_PATTERN_KEY':  pattern,
            'IS_CHECKBOX':      'Y' if is_checkbox else 'N',
        }

    def save_to_excel(self, df: pd.DataFrame, filename: str = "Crf_Observation_Data.xlsx"):
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / filename
        df.to_excel(output_path, index=False)
        print(f"File successfully saved to -> {output_path}")

    def run(self):
        # 1. Connect and load Metadata
        self.load_data_from_db()
        
        # 2. Build the wildcard templates based on the metadata rules
        templates_df = self.generate_templates()
        
        # 3. Match templates to the actual patient data and fill the real indices
        final_df = self.resolve_actual_data(templates_df)
        
        # 4. Save to Excel
        self.save_to_excel(final_df)
        
        # Display a quick preview in the console
        print("\n── Preview of Matched EAV Keys ──")
        preview = final_df[~final_df['ACTUAL_INDEX'].isin(['NO_DATA_YET', 'N/A'])].head(15)
        preview.to_csv('output1.csv')
        if not preview.empty:
            print(preview[['OBSERVATION_CODE', 'ACTUAL_INDEX', 'AFD_PATTERN_KEY']].to_string(index=False))
        else:
            print("No matching actual data found in ANALYSIS_FORM_DATA yet.")

def main():
    gen = CRFDataGenerator()
    try:
        gen.run()
    except Exception as exc:
        print(f"\nCritical Error: {exc}")
    finally:
        if gen.conn:
            gen.conn.close()

if __name__ == "__main__":
    main()