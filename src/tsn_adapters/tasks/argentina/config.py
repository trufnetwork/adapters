"""
Configuration constants for Argentina flows, including Prefect Variable names.
"""

class ArgentinaFlowVariableNames:
    """Centralizes the names of Prefect Variables used for Argentina flow state."""
    LAST_PREPROCESS_SUCCESS_DATE: str = "ARGENTINA_LAST_PREPROCESS_SUCCESS_DATE"
    LAST_AGGREGATION_SUCCESS_DATE: str = "ARGENTINA_LAST_AGGREGATION_SUCCESS_DATE"
    LAST_INSERTION_SUCCESS_DATE: str = "ARGENTINA_LAST_INSERTION_SUCCESS_DATE"
    
    DEFAULT_DATE: str = "1970-01-01" # Centralize default date string 