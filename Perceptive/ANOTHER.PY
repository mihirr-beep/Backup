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