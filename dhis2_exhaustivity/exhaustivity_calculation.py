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
    extract_config_item: dict = None,
    extracts_folder: Path = None,
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
    # Use provided extracts_folder or determine it based on extract_id
    if extracts_folder is None:
        # Determine extracts folder based on extract_id (same logic as in pipeline.py)
        extracts_base = pipeline_path / "data" / "extracts"
        
        # Try to find folder by extract_id pattern
        if "Fosa" in extract_id:
            extracts_folder = extracts_base / "Extract lvl 5"
        elif "BCZ" in extract_id:
            extracts_folder = extracts_base / "Extract lvl 3"
        else:
            # Try to find by extract_id in folder names
            for folder in extracts_base.iterdir():
                if folder.is_dir() and extract_id in folder.name:
                    extracts_folder = folder
                    break
            # Fallback to old structure if not found
            if extracts_folder is None or not extracts_folder.exists():
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
        
        # Apply mappings and filters from extract_config_item if provided
        if extract_config_item:
            # Filter by specific org units if provided
            org_units_filter = extract_config_item.get("ORG_UNITS")
            if org_units_filter:
                df = df.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
                current_run.log_info(f"Filtered by specific org units: {org_units_filter}")
            
            mappings = extract_config_item.get("MAPPINGS", {})
            if mappings:
                current_run.log_info(f"Applying mappings and filters from extract config for {extract_id}")
                chunks = []
                for uid, mapping in mappings.items():
                    uid_mapping = mapping.get("UID")
                    coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
                    
                    # Filter by DX_UID
                    df_uid = df.filter(pl.col("DX_UID") == uid)
                    
                    # Filter by COC if specified
                    if coc_mappings:
                        coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}
                        coc_keys = list(coc_mappings.keys())
                        df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                        # Replace COC values if mapping provided
                        coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items() if v is not None}
                        if coc_mappings_clean:
                            df_uid = df_uid.with_columns(
                                pl.col("CATEGORY_OPTION_COMBO").replace(coc_mappings_clean)
                            )
                    
                    # Apply UID mapping if specified
                    if uid_mapping:
                        df_uid = df_uid.with_columns(
                            pl.lit(uid_mapping).alias("DX_UID")
                        )
                    
                    chunks.append(df_uid)
                
                if chunks:
                    df = pl.concat(chunks)
                else:
                    df = pl.DataFrame()
        
        # Check if dataframe is empty
        if len(df) == 0:
            current_run.log_warning("DataFrame is empty after filtering, returning empty exhaustivity result")
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
        
        # Log raw values before aggregation
        current_run.log_info("=" * 80)
        current_run.log_info("RAW VALUES BEFORE AGGREGATION:")
        current_run.log_info("=" * 80)
        df_raw = df.select(["PERIOD", "DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "VALUE", "VALUE_IS_NULL"]).sort(["PERIOD", "DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO"])
        for row in df_raw.iter_rows(named=True):
            value_str = str(row["VALUE"]) if row["VALUE"] is not None else "NULL"
            is_null_str = "NULL" if row["VALUE_IS_NULL"] else "OK"
            current_run.log_info(
                f"PERIOD={row['PERIOD']} | DX_UID={row['DX_UID']} | ORG_UNIT={row['ORG_UNIT']} | "
                f"COC={row['CATEGORY_OPTION_COMBO']} | VALUE={value_str} | IS_NULL={is_null_str}"
            )
        current_run.log_info("=" * 80)
        
        # Determine expected COCs for each DX_UID (all COCs that appear for that DX_UID across ALL available data)
        # Read ALL parquet files in the extracts folder to get complete COC list, not just the current periods
        all_available_files = list(extracts_folder.glob("data_*.parquet"))
        if all_available_files:
            df_all_periods = pl.concat([pl.read_parquet(f) for f in all_available_files])
            # Apply same filters as df if extract_config_item is provided
            if extract_config_item:
                org_units_filter = extract_config_item.get("ORG_UNITS")
                if org_units_filter:
                    df_all_periods = df_all_periods.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
        else:
            df_all_periods = df
        
        expected_cocs_by_dx = {}
        for dx_uid in df_all_periods["DX_UID"].unique().to_list():
            dx_data = df_all_periods.filter(pl.col("DX_UID") == dx_uid)
            expected_cocs_by_dx[dx_uid] = sorted(dx_data["CATEGORY_OPTION_COMBO"].unique().to_list())
        
        current_run.log_info(f"Expected COCs per DX_UID (global, from all available data): {expected_cocs_by_dx}")
        
        # Group by PERIOD, DX_UID, and ORG_UNIT, check if all expected COCs are present and non-null
        # If any expected COC is missing or null for a (PERIOD, DX_UID, ORG_UNIT) combination, exhaustivity = 0
        # Otherwise exhaustivity = 1
        df_grouped_for_log = (
            df.group_by(["PERIOD", "DX_UID", "ORG_UNIT"])
            .agg([
                pl.col("CATEGORY_OPTION_COMBO").alias("COCs"),
                pl.col("VALUE").alias("VALUES"),
                pl.col("VALUE_IS_NULL").alias("NULL_FLAGS"),
            ])
        )
        
        # Check exhaustivity: all expected COCs must be present AND non-null
        exhaustivity_rows = []
        for row in df_grouped_for_log.iter_rows(named=True):
            period = row["PERIOD"]
            dx_uid = row["DX_UID"]
            org_unit = row["ORG_UNIT"]
            
            cocs_present = set(row["COCs"] if isinstance(row["COCs"], list) else [row["COCs"]])
            values_list = row["VALUES"] if isinstance(row["VALUES"], list) else [row["VALUES"]]
            null_flags_list = row["NULL_FLAGS"] if isinstance(row["NULL_FLAGS"], list) else [row["NULL_FLAGS"]]
            
            # Get expected COCs for this DX_UID (global, across all periods)
            expected_cocs = set(expected_cocs_by_dx.get(dx_uid, []))
            
            # Check if all expected COCs are present
            missing_cocs = expected_cocs - cocs_present
            
            # Check if any present COC has null value
            has_null_value = any(null_flags_list) if null_flags_list else False
            
            # Exhaustivity = 0 if any expected COC is missing OR any value is null
            exhaustivity_value = 0 if (missing_cocs or has_null_value) else 1
            
            exhaustivity_rows.append({
                "PERIOD": period,
                "DX_UID": dx_uid,
                "ORG_UNIT": org_unit,
                "EXHAUSTIVITY_VALUE": exhaustivity_value,
                "MISSING_COCs": list(missing_cocs),
                "HAS_NULL_VALUE": has_null_value
            })
            
            # Log aggregation details
            current_run.log_info(
                f"PERIOD={period} | DX_UID={dx_uid} | ORG_UNIT={org_unit}"
            )
            for i, (coc, value, is_null) in enumerate(zip(
                row["COCs"] if isinstance(row["COCs"], list) else [row["COCs"]],
                values_list,
                null_flags_list
            )):
                value_str = str(value) if value is not None else "NULL"
                null_str = "NULL" if is_null else "OK"
                current_run.log_info(f"  COC[{i}]={coc} | VALUE={value_str} | IS_NULL={null_str}")
            if missing_cocs:
                current_run.log_info(f"  -> MISSING_COCs={list(missing_cocs)}")
            current_run.log_info(f"  -> HAS_NULL_VALUE={has_null_value} | SCORE={exhaustivity_value}")
            current_run.log_info("-" * 80)
        
        # Create the final exhaustivity dataframe
        exhaustivity_df = pl.DataFrame(exhaustivity_rows).select([
            "PERIOD", "DX_UID", "ORG_UNIT", "EXHAUSTIVITY_VALUE"
        ])
        
        # Log final scores
        current_run.log_info("=" * 80)
        current_run.log_info("FINAL EXHAUSTIVITY SCORES:")
        current_run.log_info("=" * 80)
        for row in exhaustivity_df.iter_rows(named=True):
            current_run.log_info(
                f"PERIOD={row['PERIOD']} | DX_UID={row['DX_UID']} | ORG_UNIT={row['ORG_UNIT']} | "
                f"EXHAUSTIVITY_VALUE={row['EXHAUSTIVITY_VALUE']}"
            )
        current_run.log_info("=" * 80)
        
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
        
        # Determine output folder based on extract_id (same logic as in pipeline.py)
        if "Fosa" in extract_id:
            output_folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            output_folder_name = "Extract lvl 3"
        else:
            output_folder_name = f"Extract {extract_id}"
        
        # Save summary to a text file for easy viewing
        summary_file = pipeline_path / "data" / "processed" / output_folder_name / "summary.txt"
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write(f"EXHAUSTIVITY SUMMARY FOR EXTRACT: {extract_id}\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total combinations: {len(exhaustivity_df)}\n")
            f.write(f"Complete (score=1): {exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()}\n")
            f.write(f"Incomplete (score=0): {len(exhaustivity_df) - exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()}\n\n")
            
            # Create tabular view with raw data: rows = dx_uid, columns = category_option_combo
            # One table per (PERIOD, ORG_UNIT) combination
            periods = sorted(df["PERIOD"].unique().to_list())
            org_units = sorted(df["ORG_UNIT"].unique().to_list())
            # Use all DX_UIDs from config if available, otherwise use only those with data
            if extract_config_item and extract_config_item.get("UIDS"):
                dx_uids = sorted(extract_config_item.get("UIDS", []))
                current_run.log_info(f"Using all DX_UIDs from config: {dx_uids}")
            else:
                dx_uids = sorted(df["DX_UID"].unique().to_list())
                current_run.log_info(f"Using DX_UIDs from data only: {dx_uids}")
            
            # Get all COCs that appear for each DX_UID (expected COCs, global across all available data)
            # Use the same df_all_periods that was used for expected_cocs_by_dx
            # Also get all COCs across all DX_UIDs as fallback for DX_UIDs without data
            all_cocs_all_dx = sorted(df_all_periods["CATEGORY_OPTION_COMBO"].unique().to_list()) if len(df_all_periods) > 0 else []
            all_cocs_by_dx = {}
            for dx_uid in dx_uids:
                dx_data = df_all_periods.filter(pl.col("DX_UID") == dx_uid)
                cocs_for_dx = sorted(dx_data["CATEGORY_OPTION_COMBO"].unique().to_list())
                # If DX_UID has no data, use all COCs from all DX_UIDs as fallback
                if len(cocs_for_dx) == 0:
                    all_cocs_by_dx[dx_uid] = all_cocs_all_dx
                    current_run.log_info(f"DX_UID {dx_uid} has no data, using all COCs from other DX_UIDs: {all_cocs_all_dx}")
                else:
                    all_cocs_by_dx[dx_uid] = cocs_for_dx
            
            for period in periods:
                df_period = df.filter(pl.col("PERIOD") == period)
                
                for org_unit in org_units:
                    df_period_org = df_period.filter(pl.col("ORG_UNIT") == org_unit)
                    
                    if len(df_period_org) == 0:
                        continue
                    
                    # Get all COCs that appear for this org_unit in this period (across all DX_UIDs)
                    cocs_in_period_org = sorted(df_period_org["CATEGORY_OPTION_COMBO"].unique().to_list())
                    
                    # Create a pivot table: dx_uid (rows) x coc (columns)
                    pivot_data = []
                    for dx_uid in dx_uids:
                        # Get expected COCs for this DX_UID (global, across all periods)
                        expected_cocs = all_cocs_by_dx.get(dx_uid, [])
                        
                        row_data = {"DX_UID": dx_uid}
                        for coc in expected_cocs:
                            value_row = df_period_org.filter(
                                (pl.col("DX_UID") == dx_uid) & 
                                (pl.col("CATEGORY_OPTION_COMBO") == coc)
                            )
                            if len(value_row) > 0:
                                value = value_row["VALUE"][0]
                                is_null = value_row["VALUE_IS_NULL"][0]
                                if is_null or value is None:
                                    row_data[coc] = "NULL"
                                else:
                                    row_data[coc] = str(value)
                            else:
                                row_data[coc] = "MISSING"
                        pivot_data.append(row_data)
                    
                    # Write table header
                    f.write("\n" + "=" * 80 + "\n")
                    f.write(f"PERIOD: {period} | ORG_UNIT (Hôpital): {org_unit}\n")
                    f.write("=" * 80 + "\n")
                    f.write("Format: Lignes = DX_UID (Médicaments), Colonnes = Category Option Combo\n")
                    f.write("Valeurs: NULL = valeur manquante/null, MISSING = combinaison absente\n")
                    f.write("-" * 80 + "\n\n")
                    
                    # Get all unique COCs across all DX_UIDs (global) for column headers
                    all_cocs = set()
                    for dx_uid in dx_uids:
                        all_cocs.update(all_cocs_by_dx.get(dx_uid, []))
                    all_cocs = sorted(list(all_cocs))
                    
                    # Calculate column widths
                    dx_uid_width = max(len("DX_UID"), max(len(dx) for dx in dx_uids))
                    coc_widths = {coc: max(len(coc), 10) for coc in all_cocs}
                    
                    # Write header row
                    header = f"{'DX_UID':<{dx_uid_width}}"
                    for coc in all_cocs:
                        header += f" | {coc:<{coc_widths[coc]}}"
                    f.write(header + "\n")
                    f.write("-" * len(header) + "\n")
                    
                    # Write data rows
                    for row_data in pivot_data:
                        row_str = f"{row_data['DX_UID']:<{dx_uid_width}}"
                        for coc in all_cocs:
                            value = row_data.get(coc, "MISSING")
                            row_str += f" | {value:<{coc_widths[coc]}}"
                        f.write(row_str + "\n")
                    
                    # Calculate exhaustivity score for each dx_uid based on the table data
                    # If any COC in the table is MISSING or NULL for a DX_UID, score = 0
                    f.write("\n" + "-" * 80 + "\n")
                    f.write("EXHAUSTIVITY SCORES (0 = oubli détecté, 1 = complet):\n")
                    f.write("-" * 80 + "\n")
                    for row_data in pivot_data:
                        dx_uid = row_data["DX_UID"]
                        # Get expected COCs for this DX_UID
                        expected_cocs_for_dx = all_cocs_by_dx.get(dx_uid, [])
                        
                        # Check if all expected COCs are present and not MISSING/NULL
                        has_missing = False
                        missing_cocs_list = []
                        for coc in expected_cocs_for_dx:
                            value = row_data.get(coc, "MISSING")
                            if value == "MISSING" or value == "NULL":
                                has_missing = True
                                missing_cocs_list.append(coc)
                        
                        score = 0 if has_missing else 1
                        if has_missing:
                            f.write(f"DX_UID={dx_uid} | SCORE={score} (MISSING: {', '.join(missing_cocs_list)})\n")
                        else:
                            f.write(f"DX_UID={dx_uid} | SCORE={score}\n")
                    f.write("\n")
        
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
        
        # Get periods from configuration (using same logic as extraction)
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        from openhexa.toolbox.dhis2.periods import period_from_string
        
        extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
        if not extract_config["SETTINGS"].get("ENDDATE"):
            end = datetime.now().strftime("%Y%m")
        else:
            end = extract_config["SETTINGS"].get("ENDDATE")
        if not extract_config["SETTINGS"].get("STARTDATE"):
            end_date = datetime.strptime(end, "%Y%m")
            start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
        else:
            start = extract_config["SETTINGS"].get("STARTDATE")
        
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
