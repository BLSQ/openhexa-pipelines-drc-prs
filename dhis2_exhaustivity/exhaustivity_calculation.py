import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run
from utils import load_configuration

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
    
    For each combination of (PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT), checks if all DX_UIDs
    have a non-null VALUE. If all values are filled, exhaustivity = 1, otherwise 0.
    
    If a DX_UID is missing for a (PERIOD, COC, ORG_UNIT) combination (form not submitted),
    exhaustivity = 0 (form not sent at all).
    
    Each CATEGORY_OPTION_COMBO represents a category, and we check if all DX_UIDs (medications)
    have values for this category in the period and org unit. If any VALUE is null or missing
    for a (PERIOD, COC, ORG_UNIT) combination, exhaustivity = 0.
    
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
        DataFrame with columns: PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
        EXHAUSTIVITY_VALUE is 1 if all DX_UIDs have non-null VALUE for this COC, 0 otherwise.
        Missing (PERIOD, COC, ORG_UNIT) combinations are included with value 0.
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
            # Read files in parallel for better performance
            files_to_read_list = [f for f in files_to_read.values() if f is not None]
            if files_to_read_list:
                def read_parquet_file(file_path: Path) -> pl.DataFrame:
                    return pl.read_parquet(file_path)
                
                with ThreadPoolExecutor(max_workers=min(len(files_to_read_list), 8)) as executor:
                    dataframes = list(executor.map(read_parquet_file, files_to_read_list))
                df = pl.concat(dataframes)
            else:
                df = pl.DataFrame()
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
            return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
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
        
        # Log raw values before aggregation (limited to avoid performance issues)
        current_run.log_info("=" * 80)
        current_run.log_info("RAW VALUES BEFORE AGGREGATION (showing first 100 rows):")
        current_run.log_info("=" * 80)
        df_raw = df.select(["PERIOD", "DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "VALUE", "VALUE_IS_NULL"]).sort(["PERIOD", "DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO"]).head(100)
        for row in df_raw.iter_rows(named=True):
            value_str = str(row["VALUE"]) if row["VALUE"] is not None else "NULL"
            is_null_str = "NULL" if row["VALUE_IS_NULL"] else "OK"
            current_run.log_info(
                f"PERIOD={row['PERIOD']} | DX_UID={row['DX_UID']} | ORG_UNIT={row['ORG_UNIT']} | "
                f"COC={row['CATEGORY_OPTION_COMBO']} | VALUE={value_str} | IS_NULL={is_null_str}"
            )
        if len(df) > 100:
            current_run.log_info(f"... (showing first 100 of {len(df)} total rows)")
        current_run.log_info("=" * 80)
        
        # Determine expected DX_UIDs for each COC (all DX_UIDs that should appear with that COC)
        # Prefer using push_config mappings (source of truth), and fall back to data if not available.
        # Read ALL parquet files in the extracts folder to get complete DX_UID / COC list, not just the current periods
        all_available_files = list(extracts_folder.glob("data_*.parquet"))
        if all_available_files:
            # Read files in parallel for better performance
            def read_parquet_file(file_path: Path) -> pl.DataFrame:
                return pl.read_parquet(file_path)
            
            with ThreadPoolExecutor(max_workers=min(len(all_available_files), 8)) as executor:
                dataframes = list(executor.map(read_parquet_file, all_available_files))
            df_all_periods = pl.concat(dataframes)
            # Apply same filters as df if extract_config_item is provided
            if extract_config_item:
                org_units_filter = extract_config_item.get("ORG_UNITS")
                if org_units_filter:
                    df_all_periods = df_all_periods.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
        else:
            df_all_periods = df
        
        # 1) Try to build expected DX_UIDs per COC from push_config mappings
        expected_dx_uids_by_coc: dict[str, list[str]] = {}
        try:
            push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
            push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
            push_mappings: dict[str, dict] = {}
            for push_extract in push_extracts:
                push_mappings.update(push_extract.get("MAPPINGS", {}))

            # Limit to DX_UIDs that are relevant for this extract (from extract_config if available,
            # otherwise use DX_UIDs that appear in the data across all periods)
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids = set(extract_config_item.get("UIDS", []))
            else:
                relevant_dx_uids = set(df_all_periods["DX_UID"].unique().to_list())

            expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
            for dx_uid, mapping in push_mappings.items():
                if dx_uid not in relevant_dx_uids:
                    continue
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                for _src_coc, target_coc in coc_map.items():
                    if target_coc is None:
                        continue
                    coc_id = str(target_coc).strip()
                    if not coc_id:
                        continue
                    expected_dx_uids_by_coc_sets.setdefault(coc_id, set()).add(dx_uid)

            expected_dx_uids_by_coc = {
                coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
            }

            if expected_dx_uids_by_coc:
                current_run.log_info(
                    f"Expected DX_UIDs per COC loaded from push_config (global): {expected_dx_uids_by_coc}"
                )
        except Exception as e:
            current_run.log_warning(f"Could not load expected DX_UIDs per COC from push_config: {e!s}")
            expected_dx_uids_by_coc = {}

        # 2) Fallback: if push_config did not provide anything, derive expected DX_UIDs per COC from data
        if not expected_dx_uids_by_coc:
            expected_dx_uids_by_coc = {}
            for coc in df_all_periods["CATEGORY_OPTION_COMBO"].unique().to_list():
                coc_data = df_all_periods.filter(pl.col("CATEGORY_OPTION_COMBO") == coc)
                expected_dx_uids_by_coc[coc] = sorted(coc_data["DX_UID"].unique().to_list())
            current_run.log_info(
                f"Expected DX_UIDs per COC derived from data (no push_config mapping): {expected_dx_uids_by_coc}"
            )
        
        # Group by PERIOD, CATEGORY_OPTION_COMBO, and ORG_UNIT, check if all expected DX_UIDs are present and non-null
        # If any expected DX_UID is missing or null for a (PERIOD, COC, ORG_UNIT) combination, exhaustivity = 0
        # Otherwise exhaustivity = 1
        
        # OPTIMIZED: Use vectorized Polars operations instead of Python loops
        # Step 1: Group by (PERIOD, COC, ORG_UNIT) and aggregate DX_UIDs and null flags
        df_grouped = (
            df.group_by(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"])
            .agg([
                pl.col("DX_UID").unique().alias("DX_UIDs_PRESENT"),
                pl.col("VALUE_IS_NULL").any().alias("HAS_NULL_VALUE"),
            ])
        )
        
        # Step 2: Create a DataFrame with expected DX_UIDs per COC
        expected_dx_uids_list = []
        for coc, dx_uids in expected_dx_uids_by_coc.items():
            for dx_uid in dx_uids:
                expected_dx_uids_list.append({
                    "CATEGORY_OPTION_COMBO": coc,
                    "DX_UID_EXPECTED": dx_uid
                })
        expected_dx_uids_df = pl.DataFrame(expected_dx_uids_list) if expected_dx_uids_list else pl.DataFrame({
            "CATEGORY_OPTION_COMBO": [],
            "DX_UID_EXPECTED": []
        })
        
        # Step 3: For each (PERIOD, COC, ORG_UNIT), verify all expected DX_UIDs are present and non-null
        # Create all expected combinations and check which ones are present in data
        if len(expected_dx_uids_df) > 0:
            # Get unique periods and org units from data
            periods_df = df.select("PERIOD").unique()
            org_units_df = df.select("ORG_UNIT").unique()
            
            # Cross join to get all (PERIOD, COC, ORG_UNIT, DX_UID) combinations expected
            expected_combinations = (
                periods_df
                .join(expected_dx_uids_df, how="cross")
                .join(org_units_df, how="cross")
            )
            
            # Join with actual data to see which expected combinations are present
            # We need to check both presence and null status
            df_with_dx_uid = df.select([
                "PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "DX_UID", "VALUE_IS_NULL"
            ]).rename({"DX_UID": "DX_UID_EXPECTED"})  # Rename to match expected_combinations
            
            # Left join: expected combinations with actual data
            expected_with_data = (
                expected_combinations
                .join(
                    df_with_dx_uid,
                    on=["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "DX_UID_EXPECTED"],
                    how="left"
                )
                .with_columns([
                    # Mark as missing if DX_UID not found, or as null if found but VALUE_IS_NULL is True
                    pl.when(pl.col("VALUE_IS_NULL").is_null())
                    .then(pl.lit(True))  # Missing DX_UID (no match in join)
                    .when(pl.col("VALUE_IS_NULL") == True)
                    .then(pl.lit(True))  # Present but null
                    .otherwise(pl.lit(False))
                    .alias("IS_MISSING_OR_NULL")
                ])
            )
            
            # Group by (PERIOD, COC, ORG_UNIT) and check if all expected DX_UIDs are present and non-null
            exhaustivity_df = (
                expected_with_data
                .group_by(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"])
                .agg([
                    pl.col("IS_MISSING_OR_NULL").any().alias("HAS_MISSING_OR_NULL")
                ])
                .with_columns([
                    # Exhaustivity = 1 if no missing or null values, 0 otherwise
                    (~pl.col("HAS_MISSING_OR_NULL")).cast(pl.Int64).alias("EXHAUSTIVITY_VALUE")
                ])
                .select([
                    "PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "EXHAUSTIVITY_VALUE"
                ])
            )
        else:
            # If no expected DX_UIDs, just check for null values
            exhaustivity_df = (
                df_grouped
                .with_columns([
                    (~pl.col("HAS_NULL_VALUE")).cast(pl.Int64).alias("EXHAUSTIVITY_VALUE")
                ])
                .select([
                    "PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "EXHAUSTIVITY_VALUE"
                ])
            )
        
        # Also include combinations that exist in data but not in expected (shouldn't happen, but handle gracefully)
        data_only_combinations = (
            df_grouped
            .join(
                exhaustivity_df.select(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"]),
                on=["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"],
                how="anti"
            )
            .with_columns([
                # If not in expected, exhaustivity = 0
                pl.lit(0).alias("EXHAUSTIVITY_VALUE")
            ])
            .select([
                "PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "EXHAUSTIVITY_VALUE"
            ])
        )
        
        if len(data_only_combinations) > 0:
            exhaustivity_df = pl.concat([exhaustivity_df, data_only_combinations])
        
        # Log final scores (limited to avoid performance issues)
        current_run.log_info("=" * 80)
        current_run.log_info(f"FINAL EXHAUSTIVITY SCORES (showing first 100 of {len(exhaustivity_df)} combinations):")
        current_run.log_info("=" * 80)
        for row in exhaustivity_df.head(100).iter_rows(named=True):
            current_run.log_info(
                f"PERIOD={row['PERIOD']} | COC={row['CATEGORY_OPTION_COMBO']} | ORG_UNIT={row['ORG_UNIT']} | "
                f"EXHAUSTIVITY_VALUE={row['EXHAUSTIVITY_VALUE']}"
            )
        if len(exhaustivity_df) > 100:
            current_run.log_info(f"... (showing first 100 of {len(exhaustivity_df)} total combinations)")
        current_run.log_info("=" * 80)
        
        # If expected DX_UIDs and ORG_UNITs are provided, create a complete grid
        # and mark missing combinations as exhaustivity = 0
        # For each COC, we need all expected DX_UIDs to have values
        if expected_dx_uids and expected_org_units:
            # Get unique periods and COCs from the data
            periods_in_data = exhaustivity_df["PERIOD"].unique().to_list()
            cocs_in_data = exhaustivity_df["CATEGORY_OPTION_COMBO"].unique().to_list()
            org_units_in_data = exhaustivity_df["ORG_UNIT"].unique().to_list()
            
            # OPTIMIZED: Use Polars cross join instead of Python loops
            # Create a complete grid of all expected combinations
            periods_df = pl.DataFrame({"PERIOD": periods_in_data})
            cocs_df = pl.DataFrame({"CATEGORY_OPTION_COMBO": cocs_in_data})
            org_units_df = pl.DataFrame({"ORG_UNIT": expected_org_units})
            
            # Create DataFrame with all expected combinations using cross joins
            expected_df = (
                periods_df
                .join(cocs_df, how="cross")
                .join(org_units_df, how="cross")
            )
            
            # Left join with computed exhaustivity
            # Missing combinations will have null EXHAUSTIVITY_VALUE
            complete_exhaustivity = expected_df.join(
                exhaustivity_df,
                on=["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"],
                how="left"
            ).with_columns([
                # Fill null values with 0 (form not submitted)
                pl.col("EXHAUSTIVITY_VALUE").fill_null(0)
            ])
            
            missing_count = len(complete_exhaustivity) - len(exhaustivity_df)
            if missing_count > 0:
                current_run.log_info(
                    f"Found {missing_count} missing (PERIOD, COC, ORG_UNIT) combinations. "
                    f"Marked as exhaustivity = 0 (form not submitted)."
                )
            
            exhaustivity_df = complete_exhaustivity
        
        current_run.log_info(
            f"Exhaustivity computed for {len(exhaustivity_df)} combinations (PERIOD, COC, ORG_UNIT). "
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
            
            # Create tabular view with raw data: rows = category_option_combo, columns = dx_uid
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
            
            # Get all COCs that appear in the data (global across all available data)
            all_cocs = (
                sorted(df_all_periods["CATEGORY_OPTION_COMBO"].unique().to_list())
                if len(df_all_periods) > 0
                else []
            )

            # Get expected DX_UIDs for each COC.
            # Prefer using push_config mappings (same logic as above), and fall back to data if not available.
            expected_dx_uids_by_coc: dict[str, list[str]] = {}
            try:
                push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
                push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
                push_mappings: dict[str, dict] = {}
                for push_extract in push_extracts:
                    push_mappings.update(push_extract.get("MAPPINGS", {}))

                if extract_config_item and extract_config_item.get("UIDS"):
                    relevant_dx_uids = set(extract_config_item.get("UIDS", []))
                else:
                    relevant_dx_uids = set(df_all_periods["DX_UID"].unique().to_list())

                expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
                for dx_uid, mapping in push_mappings.items():
                    if dx_uid not in relevant_dx_uids:
                        continue
                    coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                    for _src_coc, target_coc in coc_map.items():
                        if target_coc is None:
                            continue
                        coc_id = str(target_coc).strip()
                        if not coc_id:
                            continue
                        expected_dx_uids_by_coc_sets.setdefault(coc_id, set()).add(dx_uid)

                expected_dx_uids_by_coc = {
                    coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
                }
                if expected_dx_uids_by_coc:
                    current_run.log_info(
                        f"[summary] Expected DX_UIDs per COC loaded from push_config: {expected_dx_uids_by_coc}"
                    )
            except Exception as e:
                current_run.log_warning(
                    f"[summary] Could not load expected DX_UIDs per COC from push_config, will fall back to data: {e!s}"
                )
                expected_dx_uids_by_coc = {}

            if not expected_dx_uids_by_coc:
                expected_dx_uids_by_coc = {}
                for coc in all_cocs:
                    coc_data = df_all_periods.filter(pl.col("CATEGORY_OPTION_COMBO") == coc)
                    expected_dx_uids_by_coc[coc] = sorted(coc_data["DX_UID"].unique().to_list())
            
            for period in periods:
                df_period = df.filter(pl.col("PERIOD") == period)
                
                for org_unit in org_units:
                    df_period_org = df_period.filter(pl.col("ORG_UNIT") == org_unit)
                    
                    if len(df_period_org) == 0:
                        continue
                    
                    # Create a pivot table: coc (rows) x dx_uid (columns)
                    pivot_data = []
                    for coc in all_cocs:
                        # Get expected DX_UIDs for this COC (global, across all periods)
                        expected_dx_uids = expected_dx_uids_by_coc.get(coc, [])
                        
                        row_data = {"CATEGORY_OPTION_COMBO": coc}
                        for dx_uid in dx_uids:
                            value_row = df_period_org.filter(
                                (pl.col("CATEGORY_OPTION_COMBO") == coc) & 
                                (pl.col("DX_UID") == dx_uid)
                            )
                            if len(value_row) > 0:
                                value = value_row["VALUE"][0]
                                is_null = value_row["VALUE_IS_NULL"][0]
                                if is_null or value is None:
                                    row_data[dx_uid] = "NULL"
                                else:
                                    row_data[dx_uid] = str(value)
                            else:
                                row_data[dx_uid] = "MISSING"
                        pivot_data.append(row_data)
                    
                    # Write table header
                    f.write("\n" + "=" * 80 + "\n")
                    f.write(f"PERIOD: {period} | ORG_UNIT (Hôpital): {org_unit}\n")
                    f.write("=" * 80 + "\n")
                    f.write("Format: Lignes = Category Option Combo, Colonnes = DX_UID (Médicaments)\n")
                    f.write("Valeurs: NULL = valeur manquante/null, MISSING = combinaison absente\n")
                    f.write("-" * 80 + "\n\n")
                    
                    # Calculate column widths
                    coc_width = max(len("CATEGORY_OPTION_COMBO"), max(len(coc) for coc in all_cocs))
                    dx_uid_widths = {dx_uid: max(len(dx_uid), 10) for dx_uid in dx_uids}
                    
                    # Write header row
                    header = f"{'CATEGORY_OPTION_COMBO':<{coc_width}}"
                    for dx_uid in dx_uids:
                        header += f" | {dx_uid:<{dx_uid_widths[dx_uid]}}"
                    f.write(header + "\n")
                    f.write("-" * len(header) + "\n")
                    
                    # Write data rows
                    for row_data in pivot_data:
                        row_str = f"{row_data['CATEGORY_OPTION_COMBO']:<{coc_width}}"
                        for dx_uid in dx_uids:
                            value = row_data.get(dx_uid, "MISSING")
                            row_str += f" | {value:<{dx_uid_widths[dx_uid]}}"
                        f.write(row_str + "\n")
                    
                    # Calculate exhaustivity score for each COC based on the table data
                    # If any expected DX_UID is MISSING or NULL for a COC, score = 0
                    f.write("\n" + "-" * 80 + "\n")
                    f.write("EXHAUSTIVITY SCORES (0 = oubli détecté, 1 = complet):\n")
                    f.write("-" * 80 + "\n")
                    for row_data in pivot_data:
                        coc = row_data["CATEGORY_OPTION_COMBO"]
                        # Get expected DX_UIDs for this COC
                        expected_dx_uids_for_coc = expected_dx_uids_by_coc.get(coc, [])
                        
                        # Check if all expected DX_UIDs are present and not MISSING/NULL
                        has_missing = False
                        missing_dx_uids_list = []
                        for dx_uid in expected_dx_uids_for_coc:
                            value = row_data.get(dx_uid, "MISSING")
                            if value == "MISSING" or value == "NULL":
                                has_missing = True
                                missing_dx_uids_list.append(dx_uid)
                        
                        score = 0 if has_missing else 1
                        if has_missing:
                            f.write(f"COC={coc} | SCORE={score} (MISSING: {', '.join(missing_dx_uids_list)})\n")
                        else:
                            f.write(f"COC={coc} | SCORE={score}\n")
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
        DataFrame with columns: PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
    """
    if not run_task:
        current_run.log_info("Exhaustivity calculation skipped.")
        return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
    
    try:
        # Load configuration if not provided
        if extract_config is None:
            from utils import load_configuration
            extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        
        # Get extract information
        target_extract = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
        if not target_extract:
            current_run.log_error("No extracts found in configuration for exhaustivity calculation.")
            return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        extract_id = target_extract[0].get("EXTRACT_UID")
        if not extract_id:
            current_run.log_error("No EXTRACT_UID found in configuration for exhaustivity calculation.")
            return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
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
            return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
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
        return pl.DataFrame({"PERIOD": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
