# metadata_loader.py

class CRFMetadataLoader:
    """
    Loads CRF structural metadata.
    Drives semantic resolution.
    """
 
    def __init__(self, db_manager):
        self.db = db_manager

    def load_sections(self, crf_name: str):
        query = f"""
        SELECT crf_name,
               section_name,
               section_key,
               multiple_records_flag
        FROM crf_sections
        WHERE crf_name = '{crf_name}'
        """
        return self.db.fetch_dataframe(query)

    def load_observations(self, crf_name: str):
        query = f"""
        SELECT section_name,
               observation_code,
               key,
               checkbox
        FROM std_section_observations
        WHERE crf_name = '{crf_name}'
        """
        return self.db.fetch_dataframe(query)