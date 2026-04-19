# database.py
from sqlalchemy import create_engine, text
import pandas as pd


class DatabaseManager:
    """
    Handles database connections and execution.
    Keeps DB logic isolated from business logic.
    """

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        """
        Executes SELECT queries and returns DataFrame.
        """
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def execute_bulk_insert(self, table_name: str, df: pd.DataFrame):
        """
        Performs bulk insert into target table.
        """
        df.to_sql(table_name, self.engine, if_exists="append", index=False)