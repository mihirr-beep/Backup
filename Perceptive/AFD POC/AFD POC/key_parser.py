# key_parser.py

class AFDKeyParser:
    """
    Parses AFD key strings into structured hierarchical tokens.
    No regex. Unlimited depth.
    """

    @staticmethod
    def parse_key(key: str):
        """
        Example:
        imageQualityAssessments.imageQualityAssessmentList[0].issues[2].question

        Returns:
        [
          {'name': 'imageQualityAssessments', 'index': None},
          {'name': 'imageQualityAssessmentList', 'index': 0},
          {'name': 'issues', 'index': 2},
          {'name': 'question', 'index': None}
        ]
        """
        tokens = []
        parts = key.split(".")

        for part in parts:
            if "[" in part:
                name = part[:part.index("[")]
                index = int(part[part.index("[")+1:part.index("]")])
                tokens.append({"name": name, "index": index})
            else:
                tokens.append({"name": part, "index": None})

        return tokens