import logging
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run

def compute_exhaustivity(
    pipeline_path: Path,
    extract_id: str,
    periods: list[str],
    expected_dx_uids: list[str] = None,
    expected_org_units: list[str] = None,
) -> pl.DataFrame:
    """Computes exhaustivity from extracted data based on VALUE null checks.
    
    For each combination of (PERIOD, DX_UID, ORG_UNIT), checks if all CATEGORY_OPTION_COMBO
    have a non-null VALUE. If all values are filled, exhaustivity = 1, otherwise 0.
    
    If a DX_UID is missing for a (PERIOD, ORG_UNIT) combination (form not submitted),
    exhaustivity = 0 (form not sent at all).
    
    Each DX_UID represents a form, and we check if the form was completely filled
    (all category option combos have values) for the period and org unit it was submitted.
    If any VALUE is null for a (PERIOD, DX_UID, ORG_UNIT) combination, exhaustivity = 0.
    
    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    periods : list[str]
        List of periods to process.
    expected_dx_uids : list[str], optional
        List of expected DX_UIDs for this extract. If provided, missing combinations
        will be marked as exhaustivity = 0.
    expected_org_units : list[str], optional
        List of expected ORG_UNITs for this extract. If provided, missing combinations
        will be marked as exhaustivity = 0.
    
    Returns
    -------
    pl.DataFrame
        DataFrame with columns: PERIOD, DX_UID, ORG_UNIT, EXHAUSTIVITY_VALUE
        EXHAUSTIVITY_VALUE is 1 if all CATEGORY_OPTION_COMBO have non-null VALUE, 0 otherwise.
        Missing (PERIOD, DX_UID, ORG_UNIT) combinations are included with value 0.
    """
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
            current_run.log_warning("DataFrame is empty, returning empty exhaustivity result")
            return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        # Check required columns exist
        required_columns = ["PERIOD", "DX_UID", "ORG_UNIT", "VALUE", "CATEGORY_OPTION_COMBO"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert VALUE to string and check for null/None/empty values
        # VALUE might be stored as string, so we check for null, None, empty string, or "None"
        df = df.with_columns([
            pl.when(pl.col("VALUE").is_null())
            .then(pl.lit(True))
            .when(pl.col("VALUE").cast(pl.Utf8).str.strip_chars() == "")
            .then(pl.lit(True))
            .when(pl.col("VALUE").cast(pl.Utf8).str.to_lowercase() == "none")
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("VALUE_IS_NULL")
        ])
        
        # Group by PERIOD, DX_UID, and ORG_UNIT, check if any VALUE is null in the group
        # If any VALUE is null for a (PERIOD, DX_UID, ORG_UNIT) combination, exhaustivity = 0
        # Otherwise exhaustivity = 1
        exhaustivity_df = (
            df.group_by(["PERIOD", "DX_UID", "ORG_UNIT"])
            .agg([
                pl.col("VALUE_IS_NULL").any().alias("HAS_NULL_VALUE"),
            ])
            .with_columns([
                pl.when(pl.col("HAS_NULL_VALUE"))
                .then(pl.lit(0))
                .otherwise(pl.lit(1))
                .alias("EXHAUSTIVITY_VALUE")
            ])
            .select(["PERIOD", "DX_UID", "ORG_UNIT", "EXHAUSTIVITY_VALUE"])
        )
        
        # If expected DX_UIDs and ORG_UNITs are provided, create a complete grid
        # and mark missing combinations as exhaustivity = 0
        if expected_dx_uids and expected_org_units:
            # Get unique periods, DX_UIDs, and ORG_UNITs from the data
            periods_in_data = exhaustivity_df["PERIOD"].unique().to_list()
            dx_uids_in_data = exhaustivity_df["DX_UID"].unique().to_list()
            org_units_in_data = exhaustivity_df["ORG_UNIT"].unique().to_list()
            
            # Use periods from data, but expected DX_UIDs and ORG_UNITs from config
            # Create a complete grid of all expected combinations
            all_combinations = []
            for period in periods_in_data:
                for dx_uid in expected_dx_uids:
                    for org_unit in expected_org_units:
                        all_combinations.append({
                            "PERIOD": period,
                            "DX_UID": dx_uid,
                            "ORG_UNIT": org_unit
                        })
            
            # Create DataFrame with all expected combinations
            expected_df = pl.DataFrame(all_combinations)
            
            # Left join with computed exhaustivity
            # Missing combinations will have null EXHAUSTIVITY_VALUE
            complete_exhaustivity = expected_df.join(
                exhaustivity_df,
                on=["PERIOD", "DX_UID", "ORG_UNIT"],
                how="left"
            ).with_columns([
                # Fill null values with 0 (form not submitted)
                pl.col("EXHAUSTIVITY_VALUE").fill_null(0)
            ])
            
            missing_count = len(complete_exhaustivity) - len(exhaustivity_df)
            if missing_count > 0:
                current_run.log_info(
                    f"Found {missing_count} missing (PERIOD, DX_UID, ORG_UNIT) combinations. "
                    f"Marked as exhaustivity = 0 (form not submitted)."
                )
            
            exhaustivity_df = complete_exhaustivity
        
        current_run.log_info(
            f"Exhaustivity computed for {len(exhaustivity_df)} combinations (PERIOD, DX_UID, ORG_UNIT). "
            f"Values: {exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()} complete, "
            f"{len(exhaustivity_df) - exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()} incomplete."
        )
        
        return exhaustivity_df
        
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
) -> pl.DataFrame:
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
    pl.DataFrame
        DataFrame with columns: PERIOD, DX_UID, EXHAUSTIVITY_VALUE
    """
    if not run_task:
        current_run.log_info("Exhaustivity calculation skipped.")
        return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
    
    try:
        # Load configuration if not provided
        if extract_config is None:
            from utils import load_configuration
            extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        
        # Get extract information
        target_extract = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
        if not target_extract:
            current_run.log_error("No extracts found in configuration for exhaustivity calculation.")
            return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        extract_id = target_extract[0].get("EXTRACT_UID")
        if not extract_id:
            current_run.log_error("No EXTRACT_UID found in configuration for exhaustivity calculation.")
            return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        # Get periods from configuration
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        from openhexa.toolbox.dhis2.periods import period_from_string
        
        exhaustivity_periods_window = extract_config["SETTINGS"].get("EXHAUSTIVITY_PERIODS_WINDOW", 3)
        if not extract_config["SETTINGS"].get("STARTDATE"):
            start = (datetime.now() - relativedelta(months=exhaustivity_periods_window)).strftime("%Y%m")
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
            return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        # Compute exhaustivity
        exhaustivity_df = compute_exhaustivity(
            pipeline_path=pipeline_path,
            extract_id=extract_id,
            periods=periods,
        )
        return exhaustivity_df
        
    except Exception as e:
        logging.error(f"Exhaustivity calculation error: {e!s}")
        current_run.log_error(f"Error in exhaustivity calculation: {e!s}")
        return pl.DataFrame({"PERIOD": [], "DX_UID": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
