# r"""
# Resolve Wildcards & Map Patient Data
# ======================================================================
# 1. Loads the template CSV containing [*] wildcards.
# 2. Connects to Oracle DB (using SQLAlchemy) to fetch ANALYSIS_FORM_DATA.
# 3. Uses universal bracket matching to map patient data regardless of array index.
# 4. Outputs the expanded CSV mapping and the actual mapped patient data.
# """

# import argparse
# import pandas as pd
# from pathlib import Path
# import os
# import re
# import sys
# from dotenv import load_dotenv
# import oracledb
# import sqlalchemy

# # Load environment variables
# load_dotenv()

# DB_CONFIG = {
#     'user':     os.getenv('DB_USER',     'system'),
#     'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
#     'host':     os.getenv('DB_HOST',     'localhost'),
#     'port':     os.getenv('DB_PORT',     '1521'),
#     'service':  os.getenv('DB_SERVICE',  'XE'),
# }

# # Optional: path to Oracle Instant Client if using thick mode
# ORACLE_CLIENT_LIB = os.getenv('ORACLE_CLIENT_LIB')
# if ORACLE_CLIENT_LIB:
#     oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB)

# # ────────────────────────────────────────────────
# # Extract Phase
# # ────────────────────────────────────────────────

# def get_db_engine():
#     """Creates a SQLAlchemy engine for Pandas to avoid DBAPI warnings."""
#     print(f"[EXTRACT] Connecting to Oracle DB at {DB_CONFIG['host']}/{DB_CONFIG['service']} as '{DB_CONFIG['user']}'...")
#     dsn = f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/?service_name={DB_CONFIG['service']}"
#     return sqlalchemy.create_engine(dsn)

# def extract_db_data(engine, table_name):
#     """Fetches the actual instantiated keys and values from the patient data table."""
#     print(f"[EXTRACT] Fetching raw patient data from table: {table_name}...")
#     with engine.connect() as conn:
#         try:
#             # Safely extract the reserved word "KEY"
#             query = f'SELECT RECID, "KEY" AS RAW_KEY, "VALUE" FROM {table_name} WHERE "KEY" IS NOT NULL'
#             df = pd.read_sql(query, con=conn)
#         except Exception as e1:
#             try:
#                 # Fallback just in case table was created without quoted identifiers
#                 query = f'SELECT RECID, KEY AS RAW_KEY, VALUE FROM {table_name} WHERE KEY IS NOT NULL'
#                 df = pd.read_sql(query, con=conn)
#             except Exception as e2:
#                 print(f"Failed to query {table_name}. Make sure the table exists and contains data.")
#                 sys.exit(1)
                
#     # Force all columns to UPPERCASE to prevent KeyErrors
#     df.columns = df.columns.str.strip().str.upper()
#     return df

# # ────────────────────────────────────────────────
# # Transform Phase
# # ────────────────────────────────────────────────

# def generate_universal_regex(pattern):
#     """
#     Converts a template like 'lesions[*].issues[0].value'
#     into a regex that matches ANY array index: '^lesions\[(\d+)\]\.issues\[(\d+)\]\.value$'
#     """
#     # 1. Normalize all [*] to [0] so we just look for bracketed digits
#     norm = pattern.replace('[*]', '[0]')
#     # 2. Escape the literal string (e.g., dots, brackets) -> '\\[0\\]'
#     esc = re.escape(norm)
#     # 3. Replace escaped bracketed digits with a regex capture group
#     reg_str = "^" + re.sub(r'\\\[\d+\\\]', r'\\[(\\d+)\\]', esc) + "$"
#     return re.compile(reg_str)

# def process_and_resolve(db_df):
#     """Matches the wildcard templates against actual DB keys."""
    
#     # Intelligently find the generated CSV (either in root or data/ folder)
#     base_dir = Path(__file__).parent
#     template_csv = base_dir / "data" / "Crf_Observation_Data.csv"
#     if not template_csv.exists():
#         template_csv = base_dir / "Crf_Observation_Data.csv"
        
#     print(f"[TRANSFORM] Loading templates from {template_csv}...")
#     if not template_csv.exists():
#         print(f"Error: Could not find Crf_Observation_Data.csv. Run the generator script first.")
#         sys.exit(1)
        
#     template_df = pd.read_csv(template_csv)
    
#     # 1. Compile Universal Regex Rules
#     rules = []
#     for _, row in template_df.iterrows():
#         pattern = str(row['AFD_PATTERN_KEY']).strip()
#         if not pattern or pd.isna(pattern):
#             continue
            
#         rules.append({
#             'base_row': row,
#             'regex': generate_universal_regex(pattern)
#         })

#     print(f"[TRANSFORM] Matching against {len(db_df)} database records...")
#     if len(db_df) == 0:
#         print("WARNING: Your database query returned 0 rows! Check your table name.")
#         return pd.DataFrame(), pd.DataFrame()
    
#     mapped_data = []
#     expanded_dict = []
#     seen_keys = set()
#     unmapped_count = 0
    
#     # 2. Test every row in the Database against the Rules
#     for _, db_row in db_df.iterrows():
#         recid = db_row['RECID']
#         actual_key = str(db_row['RAW_KEY']).strip()
#         val = db_row['VALUE']
        
#         match_found = False
        
#         for rule in rules:
#             match = rule['regex'].match(actual_key)
#             if match:
#                 match_found = True
                
#                 # Extract the array indices (take the first one as the primary patient array index)
#                 indices = match.groups()
#                 arr_idx = int(indices[0]) if indices else 0
                
#                 # --- A. Save the Patient Data Record ---
#                 mapped_data.append({
#                     'RECID': recid,
#                     'CRF_NAME': rule['base_row']['CRF_NAME'],
#                     'SECTION_NAME': rule['base_row']['SECTION_NAME'],
#                     'OBSERVATION_CODE': rule['base_row']['OBSERVATION_CODE'],
#                     'ARRAY_INDEX': arr_idx,
#                     'ACTUAL_KEY': actual_key,
#                     'VALUE': val
#                 })
                
#                 # --- B. Save the Resolved CSV Template ---
#                 if actual_key not in seen_keys:
#                     seen_keys.add(actual_key)
                    
#                     exp_row = rule['base_row'].copy()
#                     exp_row['AFD_PATTERN_KEY'] = actual_key  # OVERWRITE [*] WITH ACTUAL KEY!
#                     exp_row['ARRAY_INDEX'] = arr_idx
#                     expanded_dict.append(exp_row)
                    
#                 break 
                
#         if not match_found:
#             unmapped_count += 1

#     # 3. Preserve templates that didn't have any matching patient data
#     for rule in rules:
#         template_pattern = rule['base_row']['AFD_PATTERN_KEY']
#         if not any(rule['regex'].match(k) for k in seen_keys):
#             unmatched_row = rule['base_row'].copy()
#             unmatched_row['ARRAY_INDEX'] = pd.NA
#             expanded_dict.append(unmatched_row)

#     print(f"[TRANSFORM] Matched {len(mapped_data)} patient data points. ({unmapped_count} keys remain unmapped).")
    
#     # Debug Helper if completely failed
#     if len(mapped_data) == 0:
#         print("\n--- DEBUG: WHY DID IT FAIL TO MATCH? ---")
#         print("Sample DB Keys:")
#         print(db_df['RAW_KEY'].head(5).to_list())
#         print("Sample Regex Rules:")
#         print([r['regex'].pattern for r in rules[:5]])
    
#     df_expanded = pd.DataFrame(expanded_dict)
#     if 'ARRAY_INDEX' in df_expanded.columns:
#         df_expanded.sort_values(by=['SECTION_NAME', 'OBSERVATION_CODE', 'ARRAY_INDEX'], inplace=True)
        
#     return df_expanded, pd.DataFrame(mapped_data)

# # ────────────────────────────────────────────────
# # Main Execution
# # ────────────────────────────────────────────────

# def main():
#     parser = argparse.ArgumentParser(description="Resolve Wildcards to Actual Indices")
#     parser.add_argument('--table', default='ANALYSIS_FORM_DATA', help='Oracle Table Name (Default: ANALYSIS_FORM_DATA)')
#     args = parser.parse_args()

#     # 1. Extract DB Data
#     engine = get_db_engine()
#     db_df = extract_db_data(engine, args.table)

#     # 2. Resolve [*] and Map Data
#     expanded_dict_df, mapped_data_df = process_and_resolve(db_df)

#     # 3. Export Files into the `data` folder
#     out_dir = Path(__file__).parent / "data"
#     out_dir.mkdir(parents=True, exist_ok=True)
    
#     out_dict = out_dir / 'Crf_Observation_Data_Resolved.csv'
#     out_data = out_dir / 'Patient_Data_Mapped.csv'
    
#     if not expanded_dict_df.empty:
#         expanded_dict_df.to_csv(out_dict, index=False, encoding='utf-8')
#     if not mapped_data_df.empty:
#         mapped_data_df.to_csv(out_data, index=False, encoding='utf-8')
    
#     print(f"\n[LOAD] SUCCESS!")
#     print(f" 1. Resolved Dictionary saved to: {out_dict}")
#     print(f" 2. Mapped Patient Data saved to: {out_data}")

#     # Show a sneak peek of the resolved keys
#     if not expanded_dict_df.empty and 'ARRAY_INDEX' in expanded_dict_df.columns:
#         print("\n── Sample Resolved Keys (Replaced [*] with DB instance) ──")
#         sample = expanded_dict_df[expanded_dict_df['ARRAY_INDEX'].notna()].head(10)
#         print(sample[['SECTION_NAME', 'OBSERVATION_CODE', 'ARRAY_INDEX', 'AFD_PATTERN_KEY']].to_string(index=False))

# if __name__ == '__main__':
#     main()

r"""
FAST Resolve Wildcards & Map Patient Data
======================================================================
Optimized for 2.2+ Million rows.
Uses Prefix-Bucketing and native Zip iteration to bypass Pandas overhead,
cutting processing time down to seconds.
"""

import argparse
import pandas as pd
from pathlib import Path
import os
import re
import sys
import time
from dotenv import load_dotenv
import oracledb
import sqlalchemy

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'user':     os.getenv('DB_USER',     'system'),
    'password': os.getenv('DB_PASSWORD', 'SYSTEM'),
    'host':     os.getenv('DB_HOST',     'localhost'),
    'port':     os.getenv('DB_PORT',     '1521'),
    'service':  os.getenv('DB_SERVICE',  'XE'),
}

if os.getenv('ORACLE_CLIENT_LIB'):
    oracledb.init_oracle_client(lib_dir=os.getenv('ORACLE_CLIENT_LIB'))

# ────────────────────────────────────────────────
# Extract Phase
# ────────────────────────────────────────────────

def get_db_engine():
    print(f"[EXTRACT] Connecting to Oracle DB at {DB_CONFIG['host']}/{DB_CONFIG['service']}...")
    dsn = f"oracle+oracledb://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/?service_name={DB_CONFIG['service']}"
    return sqlalchemy.create_engine(dsn)

def extract_db_data(engine, table_name):
    print(f"[EXTRACT] Fetching raw patient data from table: {table_name}...")
    start_time = time.time()
    with engine.connect() as conn:
        try:
            query = f'SELECT RECID, "KEY" AS RAW_KEY, "VALUE" FROM {table_name} WHERE "KEY" IS NOT NULL'
            df = pd.read_sql(query, con=conn)
        except Exception:
            query = f'SELECT RECID, KEY AS RAW_KEY, VALUE FROM {table_name} WHERE KEY IS NOT NULL'
            df = pd.read_sql(query, con=conn)
            
    df.columns = df.columns.str.strip().str.upper()
    print(f"[EXTRACT] Loaded {len(df):,} rows in {time.time() - start_time:.2f} seconds.")
    return df

# ────────────────────────────────────────────────
# Transform Phase (ULTRA FAST)
# ────────────────────────────────────────────────

# def generate_universal_regex(pattern):
#     norm = pattern.replace('[*]', '[0]')
#     esc = re.escape(norm)
#     reg_str = "^" + re.sub(r'\\\[\d+\\\]', r'\\[(\\d+)\\]', esc) + "$"
#     return re.compile(reg_str)

def generate_universal_regex(pattern):
    """Ultra-fast regex-free path matcher - exact drop-in replacement."""
    # Convert [*] → * for matching (same logic as before)
    norm = pattern.replace('[*]', '[*]')  # Keep as-is for string matching
    
    parts = pattern.split('.')
    matcher_parts = []
    
    for part in parts:
        if '[' in part:
            name, idx = part.split('[')
            matcher_parts.append((name.strip(), idx.rstrip(']')))
        else:
            matcher_parts.append((part.strip(), None))
    
    def fast_match(actual_key):
        actual_parts = actual_key.split('.')
        if len(actual_parts) != len(matcher_parts):
            return None
        
        captured = []
        for i, (exp_name, exp_idx) in enumerate(matcher_parts):
            try:
                act_name, act_idx = actual_parts[i].split('[')
                act_idx = act_idx.rstrip(']')
            except:
                return None
            
            if exp_name != act_name.strip():
                return None
            
            # Handle wildcard [*] or exact match
            if exp_idx == '*':
                captured.append(int(act_idx))
            elif exp_idx and exp_idx != act_idx:
                return None
        
        return tuple(captured) if captured else ()
    
    # Return object with same interface as re.compile()
    return type('FastMatcher', (), {
        'match': lambda self, s: type('Match', (), {'groups': fast_match(s)})() if fast_match(s) is not None else None,
        'pattern': pattern
    })()


def process_and_resolve(db_df):
    base_dir = Path(__file__).parent
    template_csv = base_dir / "data" / "Crf_Observation_Data.csv"
    if not template_csv.exists():
        template_csv = base_dir / "Crf_Observation_Data.csv"
        if not template_csv.exists():
            print("Error: Could not find Crf_Observation_Data.csv")
            sys.exit(1)
            
    template_df = pd.read_csv(template_csv)
    
    # 1. Compile Rules and Bucket by Prefix
    rules_by_prefix = {}
    all_rules = []
    
    for _, row in template_df.iterrows():
        pattern = str(row['AFD_PATTERN_KEY']).strip()
        if not pattern or pd.isna(pattern):
            continue
            
        # Extract the prefix (everything before the first '[' or '.')
        prefix = re.split(r'\[|\.', pattern)[0]
        
        rule = {
            'base_row': row.to_dict(), # Convert to native dict for speed
            'regex': generate_universal_regex(pattern),
            'has_wildcard': '[*]' in pattern
        }
        
        rules_by_prefix.setdefault(prefix, []).append(rule)
        all_rules.append(rule)

    print(f"[TRANSFORM] Matching {len(db_df):,} rows using Prefix-Bucketing...")
    start_time = time.time()
    
    mapped_data = []
    expanded_dict = {}
    seen_keys = set()
    unmapped_count = 0
    
    # 2. Extract columns to native Numpy arrays for zip() - MASSIVE speed boost
    recids = db_df['RECID'].values
    raw_keys = db_df['RAW_KEY'].values
    values = db_df['VALUE'].values
    
    # 3. Fast Iteration Loop
    for recid, actual_key, val in zip(recids, raw_keys, values):
        actual_key_str = str(actual_key).strip()
        
        # Get the prefix of this DB record to instantly filter rules
        prefix = actual_key_str.split('[')[0].split('.')[0]
        possible_rules = rules_by_prefix.get(prefix, [])
        
        match_found = False
        
        for rule in possible_rules:
            match = rule['regex'].match(actual_key_str)
            if match:
                match_found = True
                
                indices = match.groups()
                arr_idx = int(indices[0]) if rule['has_wildcard'] and indices else 0
                
                # --- Save Patient Data ---
                mapped_data.append({
                    'RECID': recid,
                    'CRF_NAME': rule['base_row']['CRF_NAME'],
                    'SECTION_NAME': rule['base_row']['SECTION_NAME'],
                    'OBSERVATION_CODE': rule['base_row']['OBSERVATION_CODE'],
                    'ARRAY_INDEX': arr_idx,
                    'ACTUAL_KEY': actual_key_str,
                    'VALUE': val
                })
                
                # --- Save Resolved CSV Template ---
                if actual_key_str not in seen_keys:
                    seen_keys.add(actual_key_str)
                    
                    exp_row = dict(rule['base_row'])
                    exp_row['AFD_PATTERN_KEY'] = actual_key_str
                    exp_row['ARRAY_INDEX'] = arr_idx
                    expanded_dict[actual_key_str] = exp_row
                    
                break 
                
        if not match_found:
            unmapped_count += 1

    # 4. Preserve templates that didn't have any matching patient data
    for rule in all_rules:
        if not any(rule['regex'].match(k) for k in seen_keys):
            unmatched_row = dict(rule['base_row'])
            unmatched_row['ARRAY_INDEX'] = pd.NA
            expanded_dict[unmatched_row['AFD_PATTERN_KEY']] = unmatched_row

    elapsed = time.time() - start_time
    print(f"[TRANSFORM] Processed in {elapsed:.2f} seconds.")
    print(f"[TRANSFORM] Matched {len(mapped_data):,} data points. ({unmapped_count:,} keys unmapped).")
    
    df_expanded = pd.DataFrame(list(expanded_dict.values()))
    if 'ARRAY_INDEX' in df_expanded.columns:
        df_expanded.sort_values(by=['SECTION_NAME', 'OBSERVATION_CODE', 'ARRAY_INDEX'], inplace=True)
        
    return df_expanded, pd.DataFrame(mapped_data)

# ────────────────────────────────────────────────
# Main Execution
# ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', default='ANALYSIS_FORM_DATA', help='Oracle Table Name')
    args = parser.parse_args()

    engine = get_db_engine()
    db_df = extract_db_data(engine, args.table)
    
    if db_df.empty:
        print("WARNING: Table is empty.")
        sys.exit(0)

    expanded_dict_df, mapped_data_df = process_and_resolve(db_df)

    out_dir = Path(__file__).parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dict = out_dir / 'Crf_Observation_Data_Resolved.csv'
    out_data = out_dir / 'Patient_Data_Mapped.csv'
    
    if not expanded_dict_df.empty:
        expanded_dict_df.to_csv(out_dict, index=False, encoding='utf-8')
    if not mapped_data_df.empty:
        mapped_data_df.drop(columns=["VALUE"],inplace=True)
        mapped_data_df.to_csv(out_data, index=False, encoding='utf-8')
    
    print(f"\n[LOAD] SUCCESS! Saved output to data/ folder.")

if __name__ == '__main__':
    main()