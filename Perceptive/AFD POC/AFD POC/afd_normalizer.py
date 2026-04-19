# afd_normalizer.py

import pandas as pd
from key_parser import AFDKeyParser


class AFDNormalizer:
    """
    Converts raw AFD EAV model into structured path representation.
    """

    def __init__(self):
        self.parser = AFDKeyParser()

    def normalize(self, afd_df: pd.DataFrame) -> pd.DataFrame:
        """
        Input:
            recid | key | value

        Output:
            recid | level | node_name | node_index | value
        """
        rows = []

        for _, row in afd_df.iterrows():
            tokens = self.parser.parse_key(row["key"])

            for level, token in enumerate(tokens, start=1):
                rows.append({
                    "recid": row["recid"],
                    "level": level,
                    "node_name": token["name"],
                    "node_index": token["index"],
                    "value": row["value"],
                    "full_key": row["key"]
                })

        return pd.DataFrame(rows)