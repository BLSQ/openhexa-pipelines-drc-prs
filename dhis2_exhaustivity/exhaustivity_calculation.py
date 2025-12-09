import pandas as pd


def compute_exhaustivity(df: pd.DataFrame) -> int:
    """Compute exhaustivity value based on null values in required columns.
    
    For each row, checks if all required columns have non-null values.
    Returns 1 if all rows have all required columns filled, 0 otherwise.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data with columns:
        - stock_entry_fosa
        - stock_start_fosa
        - stock_end_fosa
        - quantity_consumed_fosa
        - quantity_lost_adjusted_fosa
        - days_of_stock_out_fosa
    
    Returns
    -------
    int
        1 if all rows have all required columns filled (no null values), 0 otherwise.
    """
    required_columns = [
        "stock_entry_fosa",
        "stock_start_fosa",
        "stock_end_fosa",
        "quantity_consumed_fosa",
        "quantity_lost_adjusted_fosa",
        "days_of_stock_out_fosa",
    ]
    
    # Check if all required columns exist in the dataframe
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check if dataframe is empty
    if len(df) == 0:
        return 0
    
    # For each row, check if all required columns have non-null values
    # If any row has a null value in any required column, return 0
    # Only return 1 if all rows have all required columns filled
    has_all_values = df[required_columns].notna().all(axis=1)
    
    # If all rows have all values filled, return 1, otherwise 0
    if has_all_values.all():
        return 1
    else:
        return 0

