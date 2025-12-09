import logging
from pathlib import Path

import pandas as pd
import polars as pl
from openhexa.sdk import current_run

def compute_exhaustivity(
    pipeline_path: Path,
    extract_id: str,
    periods: list[str],
) -> int:
    """Computes exhaustivity from extracted data based on null values in required columns.
    
    Reads parquet files from the extracts folder and checks if all required columns
    have non-null values for all rows. Returns 1 if all rows have all required 
    columns filled, 0 otherwise.
    
    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    periods : list[str]
        List of periods to process.
    
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
    
    extracts_folder = pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}"
    
    try:
        current_run.log_info(f"Computing exhaustivity for extract: {extract_id}")
        
        files_to_read = {
            p: (extracts_folder / f"data_{p}.parquet") if (extracts_folder / f"data_{p}.parquet").exists() else None
            for p in periods
        }
        missing_extracts = [k for k, v in files_to_read.items() if not v]
        
        if len(missing_extracts) == len(periods):
            raise FileNotFoundError(f"No parquet files found for {periods} in {extracts_folder}")
        
        if missing_extracts:
            current_run.log_warning(
                f"Expected {len(periods)} parquet files for exhaustivity computation, "
                f"but missing files for periods: {missing_extracts}."
            )
        
        try:
            df = pl.concat([pl.read_parquet(f) for f in files_to_read.values() if f is not None])
        except Exception as e:
            raise RuntimeError(f"Error reading parquet files for exhaustivity computation: {e!s}") from e
        
        # Check if dataframe is empty
        if len(df) == 0:
            current_run.log_warning("DataFrame is empty, returning exhaustivity value 0")
            return 0
        
        # Check if all required columns exist in the dataframe
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # For each row, check if all required columns have non-null values
        # If any row has a null value in any required column, return 0
        # Only return 1 if all rows have all required columns filled
        df_pandas = df.select(required_columns).to_pandas()
        has_all_values = df_pandas[required_columns].notna().all(axis=1)
        
        # If all rows have all values filled, return 1, otherwise 0
        exhaustivity_value = 1 if has_all_values.all() else 0
        current_run.log_info(f"Exhaustivity value computed: {exhaustivity_value}")
        return exhaustivity_value
        
    except Exception as e:
        logging.error(f"Exhaustivity computation error: {e!s}")
        current_run.log_error(f"Error computing exhaustivity: {e!s}")
        raise
    finally:
        current_run.log_info("Exhaustivity computation finished.")



def compute_and_log_exhaustivity(
    pipeline_path: Path,
    run_task: bool = True,
    extract_config: dict = None,
) -> int:
    """Computes exhaustivity based on extracted data after extraction is complete.
    
    Parameters
    ----------
    pipeline_path : Path
        The root path for the pipeline.
    run_task : bool
        Whether to run the computation.
    extract_config : dict, optional
        Configuration dictionary. If not provided, will be loaded from configuration file.
    
    Returns
    -------
    int
        Exhaustivity value (1 or 0)
    """
    if not run_task:
        current_run.log_info("Exhaustivity calculation skipped.")
        return 0
    
    try:
        # Load configuration if not provided
        if extract_config is None:
            from utils import load_configuration
            extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        
        # Get extract information
        target_extract = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
        if not target_extract:
            current_run.log_error("No extracts found in configuration for exhaustivity calculation.")
            return 0
        
        extract_id = target_extract[0].get("EXTRACT_UID")
        if not extract_id:
            current_run.log_error("No EXTRACT_UID found in configuration for exhaustivity calculation.")
            return 0
        
        # Get periods from configuration
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        from openhexa.toolbox.dhis2.periods import period_from_string
        
        months_lag = extract_config["SETTINGS"].get("NUMBER_MONTHS_WINDOW", 3)
        if not extract_config["SETTINGS"].get("STARTDATE"):
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = extract_config["SETTINGS"].get("STARTDATE")
        if not extract_config["SETTINGS"].get("ENDDATE"):
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
        else:
            end = extract_config["SETTINGS"].get("ENDDATE")
        
        # Generate periods list using the same logic as get_periods
        try:
            start_period = period_from_string(start)
            end_period = period_from_string(end)
            periods = (
                [str(p) for p in start_period.get_range(end_period)]
                if str(start_period) < str(end_period)
                else [str(start_period)]
            )
        except Exception as e:
            current_run.log_error(f"Error generating periods: {e!s}")
            return 0
        
        # Compute exhaustivity
        value = compute_exhaustivity(
            pipeline_path=pipeline_path,
            extract_id=extract_id,
            periods=periods,
        )
        return value
        
    except Exception as e:
        logging.error(f"Exhaustivity calculation error: {e!s}")
        current_run.log_error(f"Error in exhaustivity calculation: {e!s}")
        return 0
