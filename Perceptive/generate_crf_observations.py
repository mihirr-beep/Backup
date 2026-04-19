import os
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional

USE_EXCEL = True

EXCEL_FILES = {
    "crf_sections": "CRF_SECTIONS.xlsx",
    "std_sections": "STD_SECTIONS.xlsx",
    "std_obs":      "STD_SECTIONS_OBSERVATION.xlsx",
}


class CRFDataGenerator:

    def __init__(self):
        self.crf_sections_df: pd.DataFrame = pd.DataFrame()
        self.std_sections_df: pd.DataFrame = pd.DataFrame()
        self.std_obs_df: pd.DataFrame = pd.DataFrame()
        self.sections_map: Dict[str, dict] = {}

    # ─────────────────────────────────────────────
    # LOAD DATA
    # ─────────────────────────────────────────────

    def load_from_excel(self):
        print("Loading Excel files...")
        self.crf_sections_df = pd.read_excel(EXCEL_FILES["crf_sections"])
        self.std_sections_df = pd.read_excel(EXCEL_FILES["std_sections"])
        self.std_obs_df = pd.read_excel(EXCEL_FILES["std_obs"])
        self._normalize()

    def _normalize(self):
        for df in [self.crf_sections_df, self.std_sections_df, self.std_obs_df]:
            df.columns = df.columns.str.strip().str.upper()
            df.replace("NULL", pd.NA, inplace=True)

        self.sections_map = (
            self.std_sections_df
            .dropna(subset=["SECTION_NAME"])
            .set_index("SECTION_NAME")
            .to_dict("index")
        )

        print("CRF Sections:", len(self.crf_sections_df))
        print("STD Sections:", len(self.std_sections_df))
        print("Observations:", len(self.std_obs_df))

    # ─────────────────────────────────────────────
    # SECTION HELPERS
    # ─────────────────────────────────────────────

    def _get_section_key(self, section_name: str) -> Optional[str]:
        entry = self.sections_map.get(section_name)
        if not entry:
            return None
        key = entry.get("SECTION_KEY")
        if pd.isna(key):
            return None
        return str(key).strip()

    def _is_multiple(self, section_name: str) -> bool:
        entry = self.sections_map.get(section_name)
        if not entry:
            return False
        return entry.get("MULTIPLE_RECORDS_FLAG") == "Y"

    # ─────────────────────────────────────────────
    # CORE FIX: HIERARCHICAL INDEX CALCULATION
    # ─────────────────────────────────────────────

    def _compute_hierarchical_indices(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.sort_values(["CRF_NAME", "SEQUENCE"]).copy()
        df["_INSTANCE_INDEX"] = 0

        for crf_name, group in df.groupby("CRF_NAME"):

            # counters per (parent_path, section_name)
            counters = {}

            # track current parent instance stack
            parent_stack = {}

            for idx, row in group.iterrows():

                section = str(row["SECTION_NAME"]).strip()
                depth = section.count(".")

                parent_path = section.rsplit(".", 1)[0] if depth > 0 else None

                if not self._is_multiple(section):
                    df.loc[idx, "_INSTANCE_INDEX"] = 0
                    continue

                counter_key = (parent_path, section)

                current_index = counters.get(counter_key, 0)
                df.loc[idx, "_INSTANCE_INDEX"] = current_index
                counters[counter_key] = current_index + 1

        return df

    # ─────────────────────────────────────────────
    # BUILD SECTION PATH WITH CORRECT INDEX
    # ─────────────────────────────────────────────

    def _build_section_pattern(self, section_name: str, index_map: Dict[str, int]) -> str:

        if not section_name:
            return ""

        levels = section_name.split(".")
        accumulated = []
        path_parts = []

        for level in levels:
            accumulated.append(level)
            full = ".".join(accumulated)

            key = self._get_section_key(full)
            if not key:
                continue

            if self._is_multiple(full):
                idx = index_map.get(full, 0)
                path_parts.append(f"{key}[{idx}]")
            else:
                path_parts.append(key)

        return ".".join(path_parts)

    # ─────────────────────────────────────────────
    # BUILD OBS KEY
    # ─────────────────────────────────────────────

    def _build_obs_key_pattern(self, obs_key: str) -> str:
        if not obs_key or pd.isna(obs_key):
            return ""

        if re.search(r'\[\d+\]', obs_key):
            return obs_key

        parts = obs_key.split(".")
        result = []
        accumulated = []

        for part in parts:
            accumulated.append(part)
            full = ".".join(accumulated)
            key = self._get_section_key(full)
            if key:
                result.append(key)
            else:
                result.append(part)

        return ".".join(result)

    # ─────────────────────────────────────────────
    # MAIN GENERATION
    # ─────────────────────────────────────────────

    def generate_observations(self) -> pd.DataFrame:

        print("Generating AFD mappings...")

        expanded = self._compute_hierarchical_indices(self.crf_sections_df)

        results: List[dict] = []

        obs_group = {
            k: v.sort_values("SEQUENCE")
            for k, v in self.std_obs_df.groupby("SECTION_NAME")
        }

        for _, crf_row in expanded.iterrows():

            crf_name = crf_row["CRF_NAME"]
            section_name = str(crf_row["SECTION_NAME"]).strip()
            instance_index = crf_row["_INSTANCE_INDEX"]

            if section_name not in obs_group:
                continue

            # build index map for entire path
            index_map = {}
            parts = section_name.split(".")
            accumulated = []
            for p in parts:
                accumulated.append(p)
                full = ".".join(accumulated)
                index_map[full] = instance_index

            section_pattern = self._build_section_pattern(section_name, index_map)

            for _, obs in obs_group[section_name].iterrows():

                obs_code = str(obs.get("OBSERVATION_CODE", "")).strip()
                obs_key = obs.get("KEY", "")
                is_checkbox = str(obs.get("CHECKBOX", "N")).strip() == "Y"

                obs_pattern = self._build_obs_key_pattern(obs_key)

                if section_pattern and obs_pattern:
                    full_pattern = f"{section_pattern}.{obs_pattern}"
                else:
                    full_pattern = section_pattern or obs_pattern

                if is_checkbox:
                    results.append({
                        "CRF_NAME": crf_name,
                        "SECTION_NAME": section_name,
                        "OBSERVATION_CODE": f"{obs_code}_QUESTION",
                        "AFD_PATTERN_KEY": f"{full_pattern}.question",
                        "IS_CHECKBOX": "Y"
                    })
                    results.append({
                        "CRF_NAME": crf_name,
                        "SECTION_NAME": section_name,
                        "OBSERVATION_CODE": f"{obs_code}_VALUE",
                        "AFD_PATTERN_KEY": f"{full_pattern}.value",
                        "IS_CHECKBOX": "Y"
                    })
                else:
                    results.append({
                        "CRF_NAME": crf_name,
                        "SECTION_NAME": section_name,
                        "OBSERVATION_CODE": obs_code,
                        "AFD_PATTERN_KEY": full_pattern,
                        "IS_CHECKBOX": "N"
                    })

        df = pd.DataFrame(results)
        print("Generated rows:", len(df))
        return df

    # ─────────────────────────────────────────────
    # SAVE
    # ─────────────────────────────────────────────

    def save_to_excel(self, df: pd.DataFrame, filename="Crf_Observation_Data.xlsx"):
        df.to_excel(filename, index=False)
        print("Saved:", filename)

    # ─────────────────────────────────────────────
    # RUN
    # ─────────────────────────────────────────────

    def run(self):
        if USE_EXCEL:
            self.load_from_excel()
        df = self.generate_observations()
        self.save_to_excel(df)


if __name__ == "__main__":
    gen = CRFDataGenerator()
    gen.run()