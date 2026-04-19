# main_engine.py

from database import DatabaseManager
from afd_normalizer import AFDNormalizer
from metadata_loader import CRFMetadataLoader
from semantic_resolver import SemanticResolver


class CRFProcessingEngine:
    """
    End-to-end processing pipeline.
    """

    def __init__(self, source_conn_str, target_conn_str):
        self.source_db = DatabaseManager(source_conn_str)
        self.target_db = DatabaseManager(target_conn_str)
        self.normalizer = AFDNormalizer()

    def process_study(self, study_id: str, crf_name: str):

        # 1. Load AFD
        afd_query = f"""
        SELECT recid, key, value
        FROM analysis_form_data
        """
        afd_df = self.source_db.fetch_dataframe(afd_query)

        # 2. Normalize keys
        normalized_df = self.normalizer.normalize(afd_df)

        # 3. Load metadata
        metadata_loader = CRFMetadataLoader(self.source_db)
        sections_df = metadata_loader.load_sections(crf_name)
        observations_df = metadata_loader.load_observations(crf_name)

        # 4. Resolve semantics
        resolver = SemanticResolver(sections_df, observations_df)
        semantic_df = resolver.resolve(normalized_df)

        # 5. Bulk insert results
        self.target_db.execute_bulk_insert("crf_results", semantic_df)

        print(f"Study {study_id} processed successfully.")