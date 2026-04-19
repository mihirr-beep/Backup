# """
# Generate CRF Observation Data from Excel metadata files.
# This script mimics the key-building logic from main.py exactly:
# - Walks section hierarchy using dotted names
# - Appends SECTION_KEY + [[]\d+[]] for multiple records
# - Handles checkbox fields correctly
# """

# import pandas as pd
# from pathlib import Path
# from typing import Optional


# class CRFDataGenerator:
#     """Generates CRF observation data with AFD pattern keys – fully metadata-driven."""

#     def __init__(self, base_path: str):
#         self.base_path = Path(base_path)
#         self.crf_sections_df: Optional[pd.DataFrame] = None
#         self.std_sections_df: Optional[pd.DataFrame] = None
#         self.std_obs_df: Optional[pd.DataFrame] = None
#         self.sections_map: dict = {}

#     def load_data(self) -> None:
#         """Load Excel files."""
#         print("Loading Excel files...")

#         self.crf_sections_df = pd.read_excel(self.base_path / "CRF_SECTIONS.xlsx")
#         self.std_sections_df = pd.read_excel(self.base_path / "STD_SECTIONS.xlsx")
#         self.std_obs_df = pd.read_excel(self.base_path / "STD_SECTIONS_OBSERVATION.xlsx")

#         # Clean column names
#         for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
#             df.columns = df.columns.str.strip()

#         # Map SECTION_NAME → row data (supports dotted names)
#         self.sections_map = self.std_sections_df.set_index('SECTION_NAME').to_dict('index')

#         print(f"Loaded {len(self.crf_sections_df)} CRF sections")
#         print(f"Loaded {len(self.std_sections_df)} standard sections")
#         print(f"Loaded {len(self.std_obs_df)} observations")

#     def get_section_key(self, section_name: str) -> Optional[str]:
#         """Get SECTION_KEY for exact section name (including dotted)."""
#         if section_name in self.sections_map:
#             return self.sections_map[section_name].get('SECTION_KEY')
#         return None

#     def is_multiple(self, section_name: str) -> bool:
#         """Check if section allows multiple records."""
#         if section_name in self.sections_map:
#             return self.sections_map[section_name].get('MULTIPLE_RECORDS_FLAG') == 'Y'
#         return False

#     def build_afd_pattern_key(self, section_name: str, observation_key: str, is_checkbox: bool = False) -> str:
#         """
#         Fully dynamic key builder:
#         - Splits section_name by '.' to discover full hierarchy
#         - Looks up each level in sections_map
#         - Adds [[]\d+[]] when MULTIPLE_RECORDS_FLAG = 'Y'
#         - No hard-coded adjustments needed
#         """
#         if pd.isna(section_name) or not section_name.strip():
#             return str(observation_key) if pd.notna(observation_key) else ""

#         # Split into hierarchy levels (supports dotted names)
#         levels = section_name.split('.')

#         path_parts = []
#         current_path_parts = []

#         for level in levels:
#             current_path_parts.append(level)
#             full_section_name = '.'.join(current_path_parts)

#             key = self.get_section_key(full_section_name)
#             if key:
#                 part = str(key)
#                 if self.is_multiple(full_section_name):
#                     part += '[[]\\d+[]]'
#                 path_parts.append(part)

#         section_path = ".".join(path_parts)

#         # Combine with observation key
#         if section_path and pd.notna(observation_key):
#             final_key = f"{section_path}.{observation_key}"
#         else:
#             final_key = str(observation_key) if pd.notna(observation_key) else ""

#         # Safety for checkboxes – remove trailing array pattern if added by mistake
#         if is_checkbox:
#             last_part = final_key.split('.')[-1]
#             if last_part.endswith('[[]\\d+[]]'):
#                 final_key = '.'.join(final_key.split('.')[:-1]) + '.' + last_part[:-len('[[]\\d+[]]')]

#         return final_key

#     def generate_observations(self) -> pd.DataFrame:
#         """Generate all observations with AFD pattern keys."""
#         print("Generating observations...")

#         results = []

#         for _, crf_row in self.crf_sections_df.iterrows():
#             crf_name = crf_row.get('CRF_NAME')
#             section_name = crf_row.get('SECTION_NAME')

#             if pd.isna(section_name) or not section_name.strip():
#                 continue

#             section_obs = self.std_obs_df[self.std_obs_df['SECTION_NAME'] == section_name]

#             for _, obs_row in section_obs.iterrows():
#                 obs_code = obs_row.get('OBSERVATION_CODE')
#                 obs_key = obs_row.get('KEY')
#                 checkbox = obs_row.get('CHECKBOX', 'N')
#                 is_checkbox = checkbox == 'Y'

#                 afd_key = self.build_afd_pattern_key(section_name, obs_key, is_checkbox)

#                 # Expand [[]\d+[]] into explicit indices [0], [1], [2]
#                 if r'[[]\d+[]]' in afd_key:
#                     for i in range(3):
#                         # Replace all occurrences of the pattern with the specific index
#                         expanded_key = afd_key.replace(r'[[]\d+[]]', f'[{i}]')
                        
#                         results.append({
#                             'CRF_NAME': crf_name,
#                             'SECTION_NAME': section_name,
#                             'OBSERVATION_CODE': obs_code,
#                             'AFD_PATTERN_KEY': expanded_key,
#                             'IS_CHECKBOX': 'Y' if is_checkbox else 'N',
#                             'ACTIVE': 'Y'
#                         })
#                 else:
#                     results.append({
#                         'CRF_NAME': crf_name,
#                         'SECTION_NAME': section_name,
#                         'OBSERVATION_CODE': obs_code,
#                         'AFD_PATTERN_KEY': afd_key,
#                         'IS_CHECKBOX': 'Y' if is_checkbox else 'N',
#                         'ACTIVE': 'Y'
#                     })

#         df = pd.DataFrame(results, columns=[
#             'CRF_NAME', 'SECTION_NAME', 'OBSERVATION_CODE',
#             'AFD_PATTERN_KEY', 'IS_CHECKBOX', 'ACTIVE'
#         ])

#         print(f"Generated {len(df)} records")
#         return df

#     def save_to_excel(self, df: pd.DataFrame, filename: str = "Crf_Observation_Data.xlsx") -> None:
#         # Save to current script directory to avoid permission issues
#         output_path = Path(__file__).parent / filename
#         print(f"Saving to {output_path}...")
#         try:
#             df.to_excel(output_path, index=False)
#             print("Saved successfully")
#         except Exception as e:
#             print(f"Failed to save to {output_path}: {e}")
#             # Fallback to current working directory
#             output_path = Path.cwd() / filename
#             print(f"Trying to save to {output_path}...")
#             df.to_excel(output_path, index=False)
#             print("Saved successfully")

#     def run(self) -> pd.DataFrame:
#         self.load_data()
#         df = self.generate_observations()
#         self.save_to_excel(df)

#         print("\nSample Output (first 10 rows):")
#         print(df.head(10).to_string())

#         return df


# def main():
#     try:
#         base_path = r"C:\Users\Dell\Desktop\Script_CRF_Observation_DATA" # ← change if needed
#         generator = CRFDataGenerator(base_path)
#         generator.run()

#     except FileNotFoundError as e:
#         print(f"Error: File not found → {e}")
#     except PermissionError as e:
#         print(f"Permission denied: {e}")
#         print("Close Excel files if open.")
#     except Exception as e:
#         print(f"Error: {e}")
#         import traceback
#         traceback.print_exc()


# if __name__ == "__main__":
#     main()


"""
Generate CRF Observation Data DIRECTLY FROM ORACLE DATABASE (no Excel).
Outputs Crf_Observation_Data.xlsx

Fully dynamic – handles dotted SECTION_NAME hierarchy correctly.
Fixed:
- Dotted sections: recursive path building
- Checkbox fields: adds inner [0] for array-like keys (issues[], reasons[], etc.)
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict
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
    'user': os.getenv('DB_USER', 'system'),
    'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '1521'),
    'service': os.getenv('DB_SERVICE', 'xepdb1'),
}

# Optional: path to Oracle Instant Client if using thick mode
ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')

if ORACLE_CLIENT_LIB:
    print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)

class CRFDataGenerator:
    """Generates Crf_Observation_Data from Oracle database only."""

    def __init__(self):
        self.conn = None
        self.crf_sections_df = None
        self.std_sections_df = None
        self.std_obs_df = None
        self.sections_map: Dict[str, dict] = {}

    def connect(self):
        """Establish Oracle connection."""
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
        print("Database connection established.")

    def load_data_from_db(self):
        """Load all metadata directly from Oracle tables."""
        if self.conn is None:
            self.connect()

        print("Loading metadata from database...")

        engine = sqlalchemy.create_engine(
            f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/?service_name={DB_CONFIG['service']}"
        )

        with engine.connect() as conn:
            # CRF_SECTIONS
            self.crf_sections_df = pd.read_sql(
                "SELECT CRF_NAME, SEQUENCE, SECTION_NAME FROM CRF_SECTIONS ORDER BY CRF_NAME, SEQUENCE",
                conn
            )

            # STD_SECTIONS
            self.std_sections_df = pd.read_sql(
                "SELECT SECTION_NAME, SECTION_DESCRIPTION, SECTION_KEY, MULTIPLE_RECORDS_FLAG FROM STD_SECTIONS",
                conn
            )

            # STD_SECTION_OBSERVATIONS
            self.std_obs_df = pd.read_sql(
                "SELECT SECTION_NAME, SEQUENCE, OBSERVATION_CODE, KEY, CHECKBOX FROM STD_SECTION_OBSERVATIONS ORDER BY SECTION_NAME, SEQUENCE",
                conn
            )

        # Normalize column names to uppercase
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.columns = df.columns.str.strip().str.upper()

        # Build lookup map
        self.sections_map = self.std_sections_df.set_index('SECTION_NAME').to_dict('index')

        # Diagnostic info
        dotted = [n for n in self.sections_map if '.' in n]
        print(f"Detected {len(dotted)} dotted/hierarchical section names: {dotted[:5]}{'...' if len(dotted)>5 else ''}")
        print(f"CRF sections loaded: {len(self.crf_sections_df)}")
        print(f"Standard sections loaded: {len(self.std_sections_df)}")
        print(f"Observations loaded: {len(self.std_obs_df)}")

    def get_section_key(self, section_name: str) -> Optional[str]:
        return self.sections_map.get(section_name, {}).get('SECTION_KEY')

    def is_multiple(self, section_name: str) -> bool:
        return self.sections_map.get(section_name, {}).get('MULTIPLE_RECORDS_FLAG') == 'Y'

    def build_base_pattern(self, section_name: str) -> str:
        """Build hierarchical path with correct dotted name support."""
        if not section_name or pd.isna(section_name):
            return ""

        levels = section_name.split('.')
        path_parts = []
        current_parts = []

        for level in levels:
            current_parts.append(level)
            full_name = '.'.join(current_parts)
            key = self.get_section_key(full_name)
            if key:
                part = str(key)
                if self.is_multiple(full_name):
                    part += '[]'  # placeholder for later replacement
                path_parts.append(part)

        return ".".join(path_parts)

    def generate_observations(self) -> pd.DataFrame:
        print("Generating observation mappings (using [0] for repeatable sections)...")

        results = []

        for _, crf_row in self.crf_sections_df.iterrows():
            crf_name = crf_row.get('CRF_NAME')
            section_name = crf_row.get('SECTION_NAME')

            if pd.isna(section_name) or not section_name.strip():
                continue

            obs_rows = self.std_obs_df[self.std_obs_df['SECTION_NAME'] == section_name]

            for _, obs in obs_rows.iterrows():
                obs_code = obs.get('OBSERVATION_CODE')
                obs_key   = obs.get('KEY')
                is_checkbox = obs.get('CHECKBOX', 'N') == 'Y'

                base = self.build_base_pattern(section_name)

                # Build final base (section path + observation key)
                final_base = base
                if base and pd.notna(obs_key) and obs_key.strip():
                    final_base = f"{base}.{obs_key.strip()}"

                # ────────────────────────────────────────────────
                # Decide final key(s) to store
                # ────────────────────────────────────────────────
                if '[]' in final_base:
                    # Has at least one repeatable ancestor → use [0] representative
                    concrete_path = final_base.replace('[]', '[0]')

                    if is_checkbox:
                        inner = f"{concrete_path}[0]"
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': f"{obs_code}_QUESTION",
                            'AFD_PATTERN_KEY': f"{inner}.question",
                            'IS_CHECKBOX': 'Y',
                            'ACTIVE': 'Y'
                        })
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': f"{obs_code}_VALUE",
                            'AFD_PATTERN_KEY': f"{inner}.value",
                            'IS_CHECKBOX': 'Y',
                            'ACTIVE': 'Y'
                        })
                    else:
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': obs_code,
                            'AFD_PATTERN_KEY': concrete_path,
                            'IS_CHECKBOX': 'N',
                            'ACTIVE': 'Y'
                        })

                else:
                    # No repeaters in path
                    if is_checkbox:
                        # Still needs inner [0] for checkbox structure
                        inner = f"{final_base}[0]" if final_base else "[0]"
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': f"{obs_code}_QUESTION",
                            'AFD_PATTERN_KEY': f"{inner}.question",
                            'IS_CHECKBOX': 'Y',
                            'ACTIVE': 'Y'
                        })
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': f"{obs_code}_VALUE",
                            'AFD_PATTERN_KEY': f"{inner}.value",
                            'IS_CHECKBOX': 'Y',
                            'ACTIVE': 'Y'
                        })
                    else:
                        results.append({
                            'CRF_NAME': crf_name,
                            'SECTION_NAME': section_name,
                            'OBSERVATION_CODE': obs_code,
                            'AFD_PATTERN_KEY': final_base,
                            'IS_CHECKBOX': 'N',
                            'ACTIVE': 'Y'
                        })

        df = pd.DataFrame(results, columns=[
            'CRF_NAME', 'SECTION_NAME', 'OBSERVATION_CODE',
            'AFD_PATTERN_KEY', 'IS_CHECKBOX', 'ACTIVE'
        ])

        print(f"Generated {len(df):,} mapping rows")
        return df

    def save_to_excel(self, df: pd.DataFrame, filename="Crf_Observation_Data.xlsx"):
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / filename
        print(f"Saving to -> {output_path}")
        df.to_excel(output_path, index=False)
        print("File saved successfully.")

    def run(self):
        self.load_data_from_db()
        df = self.generate_observations()
        self.save_to_excel(df)
        # No arrow char here in current file, but usually good to check
        print("\nSample (first 15 rows):")
        print(df.head(15).to_string(index=False))

        # Optional: show some hierarchical examples
        print("\nSample hierarchical keys:")
        hierarchical = df[df['AFD_PATTERN_KEY'].str.contains(r'\[\d+\]')].head(8)
        if not hierarchical.empty:
            print(hierarchical[['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY']].to_string(index=False))


def main():
    gen = CRFDataGenerator()
    try:
        gen.run()
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if gen.conn:
            gen.conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()