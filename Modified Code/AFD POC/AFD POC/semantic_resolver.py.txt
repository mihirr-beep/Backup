# semantic_resolver.py

import pandas as pd


class SemanticResolver:
    """
    Matches normalized AFD structure with CRF metadata.
    Produces semantic observation table.
    """

    def __init__(self, sections_df, observations_df):
        self.sections = sections_df
        self.observations = observations_df

    def resolve(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates semantic observation model.
        """

        semantic_rows = []

        for _, obs in self.observations.iterrows():

            matching = normalized_df[
                normalized_df["node_name"] == obs["key"]
            ]

            for _, row in matching.iterrows():

                semantic_rows.append({
                    "recid": row["recid"],
                    "section_name": obs["section_name"],
                    "observation_code": obs["observation_code"],
                    "value": row["value"],
                    "row_index": self._build_row_index(row),
                })

        return pd.DataFrame(semantic_rows)

    def _build_row_index(self, row):
        """
        Constructs hierarchical index using node_index chain.
        """
        if row["node_index"] is None:
            return "000"
        return str(row["node_index"]).zfill(3)