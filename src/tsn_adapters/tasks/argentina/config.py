"""
Configuration constants for Argentina flows, including Prefect Variable names.
"""

class ArgentinaFlowVariableNames:
    """
    Centralizes the names of Prefect Variables used for Argentina flow state.
    Names must contain only lowercase letters, numbers, and underscores.
    """
    LAST_PREPROCESS_SUCCESS_DATE: str = "argentina_last_preprocess_success_date"
    LAST_AGGREGATION_SUCCESS_DATE: str = "argentina_last_aggregation_success_date"
    LAST_INSERTION_SUCCESS_DATE: str = "argentina_last_insertion_success_date"
    
    DEFAULT_DATE: str = "1970-01-01" # Centralize default date string 