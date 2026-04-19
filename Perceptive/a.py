"""
Generate CRF Observation Data DIRECTLY FROM ORACLE DATABASE.
Outputs Crf_Observation_Data.csv

Fully dynamic mapping generator:
- Dynamic Arrays: Uses wildcard `[*]` for sections with MULTIPLE_RECORDS_FLAG='Y' 
  (replacing the flawed hardcoded layout indices).
- Checkbox fields: Dynamically tracks occurrences of the same key to add 
  sequential indices (e.g., issues[0].question, issues[1].question).
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
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
    'service':  os.getenv('DB_SERVICE',  'XE'),
}

# Optional: path to Oracle Instant Client if using thick mode
ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')

if ORACLE_CLIENT_LIB:
    print(f"Initializing Oracle client from: {ORACLE_CLIENT_LIB}")
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)


class CRFDataGenerator:
    """Generates Crf_Observation_Data mapping dictionary from Oracle database."""

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
                WHERE  CRF_NAME IS NOT NULL AND SECTION_NAME IS NOT NULL
                ORDER  BY CRF_NAME, SEQUENCE
                """,
                conn
            )

            self.std_sections_df = pd.read_sql(
                """
                SELECT SECTION_NAME, SECTION_DESCRIPTION,
                       SECTION_KEY, MULTIPLE_RECORDS_FLAG
                FROM   STD_SECTIONS
                WHERE  SECTION_NAME IS NOT NULL
                """,
                conn
            )

            self.std_obs_df = pd.read_sql(
                """
                SELECT SECTION_NAME, SEQUENCE, OBSERVATION_CODE, KEY, CHECKBOX
                FROM   STD_SECTION_OBSERVATIONS
                WHERE  SECTION_NAME IS NOT NULL
                ORDER  BY SECTION_NAME, SEQUENCE
                """,
                conn
            )

        # Normalize column names
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.columns = df.columns.str.strip().str.upper()
            df.replace('NULL', pd.NA, inplace=True)

        # Build lookup map: SECTION_NAME → {SECTION_KEY, MULTIPLE_RECORDS_FLAG}
        self.std_sections_df['IS_MULTI'] = self.std_sections_df['MULTIPLE_RECORDS_FLAG'].str.strip().str.upper() == 'Y'
        self.sections_map = (
            self.std_sections_df
            .set_index('SECTION_NAME')
            .to_dict('index')
        )

        print(f"CRF sections loaded      : {len(self.crf_sections_df)}")
        print(f"Standard sections loaded : {len(self.std_sections_df)}")
        print(f"Observations loaded      : {len(self.std_obs_df)}")

    # ─────────────────────────────────────────────
    # Main generation logic (WILDCARD BASED)
    # ─────────────────────────────────────────────

    def _build_afd_key(self, prefix: str, obs_key: str | None, suffix: str | None = None) -> str:
        """Helper to safely concatenate the dot-notation keys."""
        if not obs_key:
            return prefix
        base = f'{prefix}.{obs_key}' if prefix else obs_key
        return f'{base}{suffix}' if suffix else base

    def generate_observations(self) -> pd.DataFrame:
        """
        Generates mapping rows utilizing wildcards [*] for dynamic arrays
        and sequential brackets [n] for static checkbox arrays.
        """
        print("Generating observation mappings (using [*] for repeatable patient data)...")

        # Pre-group observations for fast lookup
        obs_by_section = {}
        for _, row in self.std_obs_df.iterrows():
            obs_by_section.setdefault(row['SECTION_NAME'], []).append(row.to_dict())

        results = []

        for _, crf_row in self.crf_sections_df.iterrows():
            crf_name = str(crf_row.get('CRF_NAME')).strip()
            section_name = str(crf_row.get('SECTION_NAME')).strip()
            section_index = int(crf_row.get('SEQUENCE', 1)) - 1  # 0-based for metadata reference

            info = self.sections_map.get(section_name)
            if not info:
                continue

            section_key = info.get('SECTION_KEY')
            is_multi = info.get('IS_MULTI', False)

            # RULE 1: Use wildcard [*] for multiple records instead of form layout index
            if pd.isna(section_key) or not str(section_key).strip():
                prefix = ''
            elif is_multi:
                prefix = f'{section_key}[*]'
            else:
                prefix = str(section_key)

            # Tracker to handle static checkbox arrays (e.g., issues[0], issues[1])
            key_occurrence_tracker = {}
            section_observations = sorted(obs_by_section.get(section_name, []), key=lambda x: x['SEQUENCE'])

            for obs in section_observations:
                obs_code = str(obs.get('OBSERVATION_CODE', '')).strip()
                raw_key = obs.get('KEY')
                obs_index = int(obs.get('SEQUENCE', 1)) - 1
                is_checkbox = str(obs.get('CHECKBOX', 'N')).strip().upper() == 'Y'

                obs_key = None if pd.isna(raw_key) else str(raw_key).strip()

                if obs_key:
                    # RULE 2: If it's a checkbox, track occurrences to append [0], [1], etc.
                    current_count = key_occurrence_tracker.get(obs_key, 0)
                    key_occurrence_tracker[obs_key] = current_count + 1
                    
                    if is_checkbox:
                        resolved_obs_key = f"{obs_key}[{current_count}]"
                    else:
                        resolved_obs_key = obs_key
                else:
                    resolved_obs_key = None

                # Generate the string keys based on checkbox status
                if not is_checkbox:
                    afd_key = self._build_afd_key(prefix, resolved_obs_key)
                    results.append(self._build_row(
                        crf_name, section_name, section_index,
                        obs_code, obs_index, afd_key, is_checkbox=False
                    ))
                else:
                    # Checkboxes generate two rows: _QUESTION and _VALUE
                    results.append(self._build_row(
                        crf_name, section_name, section_index,
                        f'{obs_code}_QUESTION', obs_index, 
                        self._build_afd_key(prefix, resolved_obs_key, '.question'), 
                        is_checkbox=True
                    ))
                    results.append(self._build_row(
                        crf_name, section_name, section_index,
                        f'{obs_code}_VALUE', obs_index, 
                        self._build_afd_key(prefix, resolved_obs_key, '.value'), 
                        is_checkbox=True
                    ))

        columns = [
            'CRF_NAME', 'SECTION_NAME', 'SECTION_INDEX',
            'OBSERVATION_CODE', 'OBS_INDEX', 'AFD_PATTERN_KEY',
            'IS_CHECKBOX', 'ACTIVE'
        ]
        
        df = pd.DataFrame(results, columns=columns)
        print(f"Generated {len(df):,} mapping rows.")
        return df

    def _build_row(self, crf_name, section_name, section_index, obs_code, obs_index, pattern, is_checkbox):
        """Helper to standardise output dictionary generation."""
        return {
            'CRF_NAME': crf_name,
            'SECTION_NAME': section_name,
            'SECTION_INDEX': section_index,
            'OBSERVATION_CODE': obs_code,
            'OBS_INDEX': obs_index,
            'AFD_PATTERN_KEY': pattern,
            'IS_CHECKBOX': 'Y' if is_checkbox else 'N',
            'ACTIVE': 'Y',
        }

    # ─────────────────────────────────────────────
    # Output
    # ─────────────────────────────────────────────


    def save_to_csv(self, df: pd.DataFrame, filename: str = "Crf_Observation_Data.csv"):
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / filename
        print(f"Saving to → {output_path}")
        os.remove(output_path)
        df.to_csv(output_path, index=False, encoding='utf-8')
        print("File saved successfully.")

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run(self):
        self.load_data_from_db()
        df = self.generate_observations()
        self.save_to_csv(df)

        print("\n── Sample Output (first 10 rows) ──")
        print(df[['CRF_NAME', 'SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY']].head(10).to_string(index=False))

        print("\n── Sample Array Wildcards [*] ──")
        wildcards = df[df['AFD_PATTERN_KEY'].str.contains(r'\[\*\]', na=False)]
        if not wildcards.empty:
            print(wildcards[['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY']].head(5).to_string(index=False))

        print("\n── Sample Checkbox Sequential Arrays [n] ──")
        checkboxes = df[df['IS_CHECKBOX'] == 'Y']
        if not checkboxes.empty:
            print(checkboxes[['SECTION_NAME', 'OBSERVATION_CODE', 'AFD_PATTERN_KEY']].head(6).to_string(index=False))


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