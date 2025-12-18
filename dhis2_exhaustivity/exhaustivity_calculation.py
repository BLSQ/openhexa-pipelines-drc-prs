import logging
from pathlib import Path
import time

import polars as pl
from openhexa.sdk import current_run
from utils import load_configuration

logger = logging.getLogger(__name__)

def safe_log_info(message: str, max_retries: int = 3):
    """Log info with retry logic to handle API errors."""
    for attempt in range(max_retries):
        try:
            current_run.log_info(message)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                continue
            # Fallback to Python logging if OpenHexa API fails
            logger.info(message)
            logger.warning(f"OpenHexa log_info failed after {max_retries} attempts: {e}")

def safe_log_warning(message: str, max_retries: int = 3):
    """Log warning with retry logic to handle API errors."""
    for attempt in range(max_retries):
        try:
            current_run.log_warning(message)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            logger.warning(message)
            logger.warning(f"OpenHexa log_warning failed after {max_retries} attempts: {e}")

def safe_log_error(message: str, max_retries: int = 3):
    """Log error with retry logic to handle API errors."""
    for attempt in range(max_retries):
        try:
            current_run.log_error(message)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            logger.error(message)
            logger.error(f"OpenHexa log_error failed after {max_retries} attempts: {e}")

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
        DataFrame with columns: PERIOD, DX_UID, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
        EXHAUSTIVITY_VALUE is 1 if all DX_UIDs have non-null VALUE for this COC, 0 otherwise.
        One row per (PERIOD, DX_UID, COC, ORG_UNIT) combination (matching CMM format).
        Missing combinations are included with value 0.
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
        safe_log_info(f"Computing exhaustivity for extract: {extract_id}")
        
        files_to_read = {
            p: (extracts_folder / f"data_{p}.parquet") if (extracts_folder / f"data_{p}.parquet").exists() else None
            for p in periods
        }
        missing_extracts = [k for k, v in files_to_read.items() if not v]
        
        # If no files found, return empty DataFrame (will be filled with exhaustivity=0 in complete grid logic)
        if len(missing_extracts) == len(periods):
            safe_log_warning(
                f"No parquet files found for {periods} in {extracts_folder}. "
                f"Will create exhaustivity=0 entries for all expected combinations."
            )
            df = pl.DataFrame({
                "PERIOD": [],
                "DX_UID": [],
                "CATEGORY_OPTION_COMBO": [],
                "ORG_UNIT": [],
                "VALUE": [],
                "VALUE_IS_NULL": [],
            })
        else:
            if missing_extracts:
                safe_log_warning(
                    f"Expected {len(periods)} parquet files for exhaustivity computation, "
                    f"but missing files for periods: {missing_extracts}. "
                    f"Computing exhaustivity with available {len(periods) - len(missing_extracts)} period(s). "
                    f"Missing periods will be filled with exhaustivity=0 in the complete grid."
            )
        
        try:
                available_files = [f for f in files_to_read.values() if f is not None]
                safe_log_info(f"Reading {len(available_files)} parquet file(s) for exhaustivity computation")
                
                # Read all files and normalize schemas before concatenation
                # This handles cases where some files have Null columns and others have String columns
                dfs = []
                for f in available_files:
                    df_file = pl.read_parquet(f)
                    # Ensure all required columns exist with correct types
                    # If a column is missing or Null type, cast it to String
                    required_cols = {
                        "PERIOD": pl.Utf8,
                        "DX_UID": pl.Utf8,
                        "CATEGORY_OPTION_COMBO": pl.Utf8,
                        "ORG_UNIT": pl.Utf8,
                        "VALUE": pl.Utf8,
                    }
                    
                    # Add missing columns as Null/String
                    for col, dtype in required_cols.items():
                        if col not in df_file.columns:
                            df_file = df_file.with_columns(pl.lit(None, dtype=dtype).alias(col))
                        elif df_file[col].dtype == pl.Null:
                            # Convert Null type to String type
                            df_file = df_file.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                    
                    dfs.append(df_file)
                
                # Use how="vertical_relaxed" to allow automatic type coercion if schemas differ slightly
                df = pl.concat(dfs, how="vertical_relaxed")
                safe_log_info(f"Loaded {len(df)} rows from extracted data")
        except Exception as e:
            raise RuntimeError(f"Error reading parquet files for exhaustivity computation: {e!s}") from e
        
        # IMPORTANT: Do NOT apply mappings before exhaustivity calculation
        # We need to calculate exhaustivity with COC SOURCE and DX_UID SOURCE values
        # The mappings define which (COC SOURCE, DX_UID SOURCE) pairs are valid
        # Mappings (COC SOURCE → COC TARGET, DX_UID SOURCE → DX_UID TARGET) will be applied AFTER exhaustivity calculation
        
        # Only apply filters (org units) if provided, but keep COCs and DX_UIDs as SOURCE
        if extract_config_item:
            # Filter by specific org units if provided
            org_units_filter = extract_config_item.get("ORG_UNITS")
            if org_units_filter:
                df = df.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
                safe_log_info(f"Filtered by specific org units: {org_units_filter}")
        
        # Note: Mappings are NOT applied here - they will be applied later in apply_analytics_data_element_extract_config
        # This ensures that exhaustivity calculation uses COC SOURCE and DX_UID SOURCE values
        # and only considers valid pairs defined in extract_config.MAPPINGS
        
        # Check if dataframe is empty
        if len(df) == 0:
            safe_log_warning("DataFrame is empty after filtering, returning empty exhaustivity result")
            return pl.DataFrame({"PERIOD": [], "DX_UID": [], "CATEGORY_OPTION_COMBO": [], "ORG_UNIT": [], "EXHAUSTIVITY_VALUE": []})
        
        # Check required columns exist
        required_columns = ["PERIOD", "DX_UID", "ORG_UNIT", "VALUE", "CATEGORY_OPTION_COMBO"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert VALUE to string and check for null/None/empty values
        # VALUE might be stored as string or numeric, so we first cast to string safely
        # Check for null, None, empty string, or "None"
        df = df.with_columns([
            pl.when(pl.col("VALUE").is_null())
            .then(pl.lit(True))
            .when(pl.col("VALUE").cast(pl.Utf8, strict=False).is_null())
            .then(pl.lit(True))
            .when(pl.col("VALUE").cast(pl.Utf8, strict=False).str.strip_chars() == "")
            .then(pl.lit(True))
            .when(pl.col("VALUE").cast(pl.Utf8, strict=False).str.to_lowercase() == "none")
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("VALUE_IS_NULL")
        ])
        
        # Log start of aggregation step (use logging for non-critical logs to reduce API calls)
        logging.info("Starting exhaustivity computation aggregation...")
        df_raw = df.select(["PERIOD", "DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "VALUE", "VALUE_IS_NULL"])
        logging.info(f"Processing {len(df_raw)} data rows for exhaustivity computation.")
        
        # Determine expected DX_UIDs for each COC from extracted data (source of truth)
        # The pairs (COC SOURCE, DX_UID SOURCE) come from the extracted data BEFORE mapping
        # Read ALL parquet files in the extracts folder to get complete DX_UID / COC list (BEFORE mapping)
        all_available_files = list(extracts_folder.glob("data_*.parquet"))
        if all_available_files:
            # Read all files and normalize schemas before concatenation
            # These files contain RAW data with COC SOURCE and DX_UID SOURCE (before mapping)
            dfs_all = []
            for f in all_available_files:
                df_file = pl.read_parquet(f)
                # Normalize schema: ensure Null columns are cast to String
                for col in df_file.columns:
                    if df_file[col].dtype == pl.Null:
                        df_file = df_file.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                dfs_all.append(df_file)
            df_all_periods_raw = pl.concat(dfs_all, how="vertical_relaxed")
            # Apply org units filter if provided (but keep COCs and DX_UIDs as SOURCE)
            if extract_config_item:
                org_units_filter = extract_config_item.get("ORG_UNITS")
                if org_units_filter:
                    df_all_periods_raw = df_all_periods_raw.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
        else:
            # If no files, use df but note that df might have mappings applied
            # In this case, we need to use the raw data from df before mappings
            df_all_periods_raw = df
        
        # Build expected DX_UIDs per COC from push_config.MAPPINGS (source of truth for pairs)
        # The mappings in push_config define which DX_UIDs are associated with which COCs
        expected_dx_uids_by_coc: dict[str, list[str]] = {}
        
        # 1) Try to get mappings from push_config first (source of truth for pairs)
        # Note: push_config may use different EXTRACT_UID names (e.g., "level_fosa", "level_zs")
        # instead of the extract_id (e.g., "Fosa_exhaustivity_data_elements", "BCZ_exhaustivity_data_elements")
        extract_mappings = {}
        if pipeline_path:
            try:
                push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
                push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
                
                # Try to find matching extract by EXTRACT_UID
                # First try exact match, then try alternative names
                matching_extract = None
                
                # Try exact match first
                matching_extract = next(
                    (e for e in push_extracts if e.get("EXTRACT_UID") == extract_id),
                    None
                )
                
                # If found but has empty mappings, or not found, try alternative names based on extract_id
                if not matching_extract or not matching_extract.get("MAPPINGS"):
                    if "Fosa" in extract_id or "fosa" in extract_id.lower():
                        alt_extract = next(
                            (e for e in push_extracts if e.get("EXTRACT_UID") == "level_fosa"),
                            None
                        )
                        if alt_extract and alt_extract.get("MAPPINGS"):
                            matching_extract = alt_extract
                    elif "BCZ" in extract_id or "zs" in extract_id.lower():
                        alt_extract = next(
                            (e for e in push_extracts if e.get("EXTRACT_UID") == "level_zs"),
                            None
                        )
                        if alt_extract and alt_extract.get("MAPPINGS"):
                            matching_extract = alt_extract
                
                if matching_extract:
                    push_mappings = matching_extract.get("MAPPINGS", {})
                    if push_mappings:
                        # Only include mappings for UIDs that are in extract_config.UIDS (if available)
                        # Exception: if we found mappings via alternative name (level_fosa, level_zs),
                        # use all mappings without filtering, as the UIDs may not match exactly
                        found_via_alternative = matching_extract.get("EXTRACT_UID") in ["level_fosa", "level_zs"]
                        
                        if extract_config_item and extract_config_item.get("UIDS") and not found_via_alternative:
                            extract_uids = set(extract_config_item.get("UIDS", []))
                            for uid, mapping in push_mappings.items():
                                # Include mapping if source UID OR target UID is in extract_config
                                target_uid = mapping.get("UID", "")
                                if uid in extract_uids or target_uid in extract_uids:
                                    extract_mappings[uid] = mapping
                        else:
                            # Use all mappings (either no UIDS filter, or found via alternative name)
                            extract_mappings.update(push_mappings)
                        if extract_mappings:
                            via_info = f" (found via {matching_extract.get('EXTRACT_UID')})" if found_via_alternative else ""
                            safe_log_info(f"Loaded {len(extract_mappings)} mappings from push_config for {extract_id}{via_info}")
                else:
                    safe_log_warning(f"Extract {extract_id} not found in push_config (tried exact match and alternatives: level_fosa, level_zs)")
            except Exception as e:
                safe_log_warning(f"Could not load mappings from push_config: {e!s}")
        
        # 2) If no mappings in push_config, try extract_config as fallback
        if not extract_mappings and extract_config_item:
            extract_mappings = extract_config_item.get("MAPPINGS", {})
            if extract_mappings:
                safe_log_info(f"Loaded {len(extract_mappings)} mappings from extract_config for {extract_id}")
        
        # 3) Build expected_dx_uids_by_coc from mappings (indexed by COC SOURCE, using DX_UID SOURCE)
        # This defines which (COC SOURCE, DX_UID SOURCE) pairs are valid
        # We only calculate exhaustivity for these valid pairs
        if extract_mappings:
            # Filter by extract_config.UIDS if available (defines which DX_UIDs SOURCE are relevant)
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids_source = set(extract_config_item.get("UIDS", []))
                logging.info(
                    f"Filtering DX_UIDs using extract_config.UIDS: {len(relevant_dx_uids_source)} UIDs from config"
                )
            else:
                relevant_dx_uids_source = None
            
            # Build expected_dx_uids_by_coc from mappings (COC SOURCE → DX_UID SOURCE pairs)
            # This defines valid pairs: for each COC SOURCE, which DX_UIDs SOURCE are associated
            # We use SOURCE values because mappings are NOT applied before exhaustivity calculation
            expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
            for dx_uid_source, mapping in extract_mappings.items():
                if relevant_dx_uids_source and dx_uid_source not in relevant_dx_uids_source:
                    continue
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                # Index by COC SOURCE (keys of coc_map), use DX_UID SOURCE
                # This defines which (COC SOURCE, DX_UID SOURCE) pairs are valid
                for src_coc, target_coc in coc_map.items():
                    if target_coc is None:
                        continue
                    src_coc_str = str(src_coc).strip()
                    if not src_coc_str:
                        continue
                    # Index by COC SOURCE, use DX_UID SOURCE (before mapping)
                    # This ensures we only calculate exhaustivity for valid pairs
                    expected_dx_uids_by_coc_sets.setdefault(src_coc_str, set()).add(str(dx_uid_source))
            
            expected_dx_uids_by_coc = {
                coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
            }
            logging.info(
                f"Expected DX_UIDs per COC loaded from mappings (indexed by COC SOURCE): {len(expected_dx_uids_by_coc)} COCs"
            )
        
        # 4) Fallback: if no mappings available, derive from extracted data (using SOURCE values)
        if not expected_dx_uids_by_coc:
            logging.info("No mappings found in extract_config or push_config, deriving pairs from extracted data (SOURCE values)")
            # Use df_all_periods_raw which has COC SOURCE and DX_UID SOURCE (before mapping)
            # Filter by extract_config.UIDS if available
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids = set(extract_config_item.get("UIDS", []))
                df_all_periods_filtered = df_all_periods_raw.filter(pl.col("DX_UID").is_in(list(relevant_dx_uids)))
            else:
                df_all_periods_filtered = df_all_periods_raw
            
            if len(df_all_periods_filtered) > 0:
                coc_dx_uids_df = (
                    df_all_periods_filtered.group_by("CATEGORY_OPTION_COMBO")
                    .agg(pl.col("DX_UID").unique().sort().alias("DX_UIDs"))
                )
                expected_dx_uids_by_coc = {
                    row["CATEGORY_OPTION_COMBO"]: row["DX_UIDs"] if isinstance(row["DX_UIDs"], list) else [row["DX_UIDs"]]
                    for row in coc_dx_uids_df.iter_rows(named=True)
                }
                logging.info(
                    f"Expected DX_UIDs per COC derived from extracted data (SOURCE values, fallback): {len(expected_dx_uids_by_coc)} COCs"
                )
            else:
                logging.warning("No data available to derive COC/DX_UID pairs. Using empty mapping.")
                expected_dx_uids_by_coc = {}
        
        # Group by PERIOD, CATEGORY_OPTION_COMBO, and ORG_UNIT, check if all expected DX_UIDs are present and non-null
        # If any expected DX_UID is missing or null for a (PERIOD, COC, ORG_UNIT) combination, exhaustivity = 0
        # Otherwise exhaustivity = 1
        df_grouped_for_log = (
            df.group_by(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"])
            .agg([
                pl.col("DX_UID").alias("DX_UIDs"),
                pl.col("VALUE").alias("VALUES"),
                pl.col("VALUE_IS_NULL").alias("NULL_FLAGS"),
            ])
        )
        
        # Check exhaustivity: all expected DX_UIDs must be present AND non-null
        # Note: Using iter_rows here is acceptable for complex set operations
        # The performance impact is minimal compared to I/O operations
        exhaustivity_rows = []
        for row in df_grouped_for_log.iter_rows(named=True):
            period = row["PERIOD"]
            coc = row["CATEGORY_OPTION_COMBO"]
            org_unit = row["ORG_UNIT"]
            
            dx_uids_present = set(row["DX_UIDs"] if isinstance(row["DX_UIDs"], list) else [row["DX_UIDs"]])
            null_flags_list = row["NULL_FLAGS"] if isinstance(row["NULL_FLAGS"], list) else [row["NULL_FLAGS"]]
            
            # Get expected DX_UIDs for this COC (global, across all periods)
            expected_dx_uids = set(expected_dx_uids_by_coc.get(coc, []))
            
            # Check if all expected DX_UIDs are present
            missing_dx_uids = expected_dx_uids - dx_uids_present
            
            # Check if any present DX_UID has null value
            has_null_value = any(null_flags_list) if null_flags_list else False
            
            # Exhaustivity = 0 if any expected DX_UID is missing OR any value is null
            exhaustivity_value = 0 if (missing_dx_uids or has_null_value) else 1
            
            # Create one row per expected DX_UID for this COC (like CMM format)
            # This ensures DX_UID is in the exhaustivity file, matching CMM format
            for dx_uid in expected_dx_uids:
                exhaustivity_rows.append({
                    "PERIOD": period,
                    "DX_UID": dx_uid,
                    "CATEGORY_OPTION_COMBO": coc,
                    "ORG_UNIT": org_unit,
                    "EXHAUSTIVITY_VALUE": exhaustivity_value,
                })
        
        # Create the final exhaustivity dataframe
        if exhaustivity_rows:
            exhaustivity_df = pl.DataFrame(exhaustivity_rows)
        else:
            # Empty DataFrame with correct schema
            exhaustivity_df = pl.DataFrame({
                "PERIOD": [],
                "DX_UID": [],
                "CATEGORY_OPTION_COMBO": [],
                "ORG_UNIT": [],
                "EXHAUSTIVITY_VALUE": [],
            })
        
        # Log summary only (use logging instead of current_run to reduce API calls)
        total_combinations = len(exhaustivity_df)
        if total_combinations > 0:
            complete_count = exhaustivity_df["EXHAUSTIVITY_VALUE"].sum()
            incomplete_count = total_combinations - complete_count
        else:
            complete_count = 0
            incomplete_count = 0
        logging.info(
            f"Exhaustivity computation completed: {total_combinations} combinations "
            f"({complete_count} complete, {incomplete_count} incomplete)"
        )
        # Only log to OpenHexa API for critical summary
        current_run.log_info(
            f"Exhaustivity: {total_combinations} combinations ({complete_count} complete, {incomplete_count} incomplete)"
        )
        
        # Create a complete grid using push_config mappings and expected periods/org_units
        # This ensures we detect missing COCs, periods, and ORG_UNITs
        periods_in_data = exhaustivity_df["PERIOD"].unique().to_list() if len(exhaustivity_df) > 0 else []
        cocs_in_data = exhaustivity_df["CATEGORY_OPTION_COMBO"].unique().to_list() if len(exhaustivity_df) > 0 else []
        
        # Use ALL COCs from push_config (not just those in data)
        # This ensures we detect missing COCs even if they have no data
        # Priority: 1) push_config mappings, 2) COCs in data
        all_expected_cocs = set(expected_dx_uids_by_coc.keys())  # COCs from push_config
        if cocs_in_data:
            all_expected_cocs.update(cocs_in_data)  # Add COCs found in data
        all_expected_cocs = sorted(list(all_expected_cocs))
        
        # Log summary of what's being used
        if expected_dx_uids_by_coc:
            logging.info(
                f"Using {len(all_expected_cocs)} COCs total: "
                f"{len(expected_dx_uids_by_coc)} from push_config, "
                f"{len(cocs_in_data)} found in data"
            )
            
        # Use expected periods (from periods parameter) and expected ORG_UNITs
        expected_periods = periods if periods else periods_in_data
        
        # If expected_org_units is not provided, derive from data to create complete grid
        # This ensures we include all periods even if some have no data
        if expected_org_units:
            expected_org_units_list = expected_org_units
            logging.info(
                f"Using {len(expected_org_units_list)} ORG_UNITs from config for complete grid"
            )
        else:
            # Derive from data: get all unique ORG_UNITs from all periods
            if len(exhaustivity_df) > 0:
                expected_org_units_list = sorted(exhaustivity_df["ORG_UNIT"].unique().to_list())
                logging.info(
                    f"Using {len(expected_org_units_list)} ORG_UNITs from data for complete grid "
                    f"(no ORG_UNITS in config)"
                )
            else:
                # If no data at all, we can't create a grid
                expected_org_units_list = []
                logging.warning(
                    "No expected ORG_UNITs provided and no data available - "
                    "complete grid will not be created"
                )
        
        # If we have expected periods and ORG_UNITs, create complete grid
        # Now we need to include DX_UID in the grid (one per COC)
        expected_df = None
        if expected_periods and expected_org_units_list and all_expected_cocs:
            # Build expected combinations: for each COC, get all its DX_UIDs
            grid_rows = []
            for coc in all_expected_cocs:
                dx_uids_for_coc = expected_dx_uids_by_coc.get(coc, [])
                if not dx_uids_for_coc:
                    # If no DX_UIDs for this COC, skip it or use empty list
                    continue
                for period in expected_periods:
                    for org_unit in expected_org_units_list:
                        for dx_uid in dx_uids_for_coc:
                            grid_rows.append({
                                "PERIOD": period,
                                "DX_UID": dx_uid,
                                "CATEGORY_OPTION_COMBO": coc,
                                "ORG_UNIT": org_unit,
                            })
            
            if grid_rows:
                expected_df = pl.DataFrame(grid_rows)
                total_expected = len(expected_df)
                logging.info(
                    f"Creating complete grid: {len(expected_periods)} periods × {len(all_expected_cocs)} COCs × "
                    f"{len(expected_org_units_list)} ORG_UNITs × avg {sum(len(expected_dx_uids_by_coc.get(coc, [])) for coc in all_expected_cocs) / max(len(all_expected_cocs), 1):.1f} DX_UIDs per COC = {total_expected} combinations"
                )
            else:
                # If no grid rows (e.g., expected_dx_uids_by_coc is empty), skip grid creation
                logging.warning("No grid rows to create (expected_dx_uids_by_coc is empty or no COCs). Skipping complete grid.")
        else:
            if not expected_periods:
                logging.warning("No expected periods, cannot create complete grid.")
            if not expected_org_units_list:
                logging.warning("No expected ORG_UNITs, cannot create complete grid.")
            if not all_expected_cocs:
                logging.warning("No expected COCs, cannot create complete grid.")
            
            # Left join with computed exhaustivity
            # Missing combinations will have null EXHAUSTIVITY_VALUE
        if expected_df is not None:
            if len(exhaustivity_df) > 0:
                complete_exhaustivity = expected_df.join(
                    exhaustivity_df,
                    on=["PERIOD", "DX_UID", "CATEGORY_OPTION_COMBO", "ORG_UNIT"],
                    how="left"
            ).with_columns([
                    # Fill null values with 0 (form not submitted or missing)
                pl.col("EXHAUSTIVITY_VALUE").fill_null(0)
            ])
            else:
                # If exhaustivity_df is empty, all combinations get exhaustivity=0
                complete_exhaustivity = expected_df.with_columns([
                    pl.lit(0).alias("EXHAUSTIVITY_VALUE")
                ])
            
            missing_count = len(complete_exhaustivity) - len(exhaustivity_df)
            if missing_count > 0:
                # Analyze what's missing
                missing_periods = set(expected_periods) - set(periods_in_data)
                missing_cocs = set(all_expected_cocs) - set(cocs_in_data)
                missing_org_units = set(expected_org_units_list) - set(exhaustivity_df["ORG_UNIT"].unique().to_list() if len(exhaustivity_df) > 0 else [])
                
                current_run.log_info(
                    f"Found {missing_count} missing (PERIOD, COC, ORG_UNIT) combinations. "
                    f"Marked as exhaustivity = 0 (form not submitted)."
                )
                if missing_periods:
                    current_run.log_warning(f"Missing {len(missing_periods)} periods")
                if missing_cocs:
                    current_run.log_warning(f"Missing {len(missing_cocs)} COCs (no data)")
                if missing_org_units:
                    current_run.log_warning(f"Missing {len(missing_org_units)} ORG_UNITs")
            
            exhaustivity_df = complete_exhaustivity
        else:
            # If no grid was created, use exhaustivity_df as-is (may be empty)
            # This happens when expected_dx_uids_by_coc is empty or no COCs/periods/org_units
            logging.warning("No complete grid created, using exhaustivity_df as-is (may be empty)")
            if expected_periods and not expected_org_units_list:
                current_run.log_warning(
                    "Expected ORG_UNITs not provided, cannot create complete grid. "
                    "Only combinations with data will be included."
                )
            elif not expected_periods and expected_org_units_list:
                current_run.log_warning(
                    "Expected periods not provided, cannot create complete grid. "
                    "Only combinations with data will be included."
                )
        
        # Log summary only if exhaustivity_df is not empty
        if len(exhaustivity_df) > 0:
            logging.info(
                f"Exhaustivity computed for {len(exhaustivity_df)} combinations (PERIOD, COC, ORG_UNIT). "
            f"Values: {exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()} complete, "
            f"{len(exhaustivity_df) - exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()} incomplete."
        )
        else:
            logging.warning("Exhaustivity DataFrame is empty. No combinations computed.")
        
        # Generate summary.txt for monitoring (same format as version 5f649ac)
        # Determine output folder name based on extract_id (exact same logic as reference version)
        if "Fosa" in extract_id:
            output_folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            output_folder_name = "Extract lvl 3"
        else:
            output_folder_name = f"Extract {extract_id}"
        
        # Save summary to a text file for easy viewing
        summary_file = pipeline_path / "data" / "processed" / output_folder_name / "summary.txt"
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(summary_file, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write(f"EXHAUSTIVITY SUMMARY FOR EXTRACT: {extract_id}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Total combinations: {len(exhaustivity_df)}\n")
                f.write(f"Complete (score=1): {exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()}\n")
                f.write(f"Incomplete (score=0): {len(exhaustivity_df) - exhaustivity_df['EXHAUSTIVITY_VALUE'].sum()}\n\n")
                
                # Create tabular view with raw data: rows = category_option_combo, columns = dx_uid
                # One table per (PERIOD, ORG_UNIT) combination
                # Limit to avoid API overload: max 50 combinations (periods × org_units)
                periods = sorted(df["PERIOD"].unique().to_list())
                org_units = sorted(df["ORG_UNIT"].unique().to_list())
                
                # Limit combinations to avoid overload
                max_combinations = 50
                total_combinations = len(periods) * len(org_units)
                if total_combinations > max_combinations:
                    logging.warning(
                        f"Summary generation limited to {max_combinations} combinations "
                        f"(from {total_combinations} total) to avoid API overload"
                    )
                    # Limit to first N periods and first M org_units
                    max_periods = min(len(periods), 10)
                    max_org_units = min(len(org_units), max_combinations // max_periods)
                    periods = periods[:max_periods]
                    org_units = org_units[:max_org_units]
                
                # Use all DX_UIDs from config if available, otherwise use only those with data
                if extract_config_item and extract_config_item.get("UIDS"):
                    dx_uids = sorted(extract_config_item.get("UIDS", []))
                    logging.info(f"Using {len(dx_uids)} DX_UIDs from config")
                else:
                    dx_uids = sorted(df["DX_UID"].unique().to_list())
                    logging.info(f"Using {len(dx_uids)} DX_UIDs from data")
                
                # Get all COCs that appear in the data (global across all available data)
                # Use raw data (before mapping) to get COC SOURCE values
                all_cocs = (
                    sorted(df_all_periods_raw["CATEGORY_OPTION_COMBO"].unique().to_list())
                    if len(df_all_periods_raw) > 0
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
                        relevant_dx_uids = set(df_all_periods_raw["DX_UID"].unique().to_list())

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
                        logging.info(
                            f"[summary] Expected DX_UIDs per COC loaded from push_config: {len(expected_dx_uids_by_coc)} COCs"
                        )
                except Exception as e:
                    current_run.log_warning(
                        f"[summary] Could not load expected DX_UIDs per COC from push_config, will fall back to data: {e!s}"
                    )
                    expected_dx_uids_by_coc = {}

                if not expected_dx_uids_by_coc:
                    expected_dx_uids_by_coc = {}
                    for coc in all_cocs:
                        coc_data = df_all_periods_raw.filter(pl.col("CATEGORY_OPTION_COMBO") == coc)
                        expected_dx_uids_by_coc[coc] = sorted(coc_data["DX_UID"].unique().to_list())
                
                # Optimize: use Polars pivot instead of nested loops with filters
                # Create a lookup dict for faster access
                df_with_status = df.with_columns([
                    pl.when(pl.col("VALUE_IS_NULL") == True)
                    .then(pl.lit("NULL"))
                    .when(pl.col("VALUE").is_null())
                    .then(pl.lit("NULL"))
                    .otherwise(pl.col("VALUE").cast(pl.Utf8))
                    .alias("VALUE_STR")
                ])
                
                for period in periods:
                    df_period = df_with_status.filter(pl.col("PERIOD") == period)
                    
                    for org_unit in org_units:
                        df_period_org = df_period.filter(pl.col("ORG_UNIT") == org_unit)
                        
                        if len(df_period_org) == 0:
                            continue
                        
                        # Create a pivot table using Polars: coc (rows) x dx_uid (columns)
                        # First, create a combined key for pivot
                        pivot_df = df_period_org.select([
                            "CATEGORY_OPTION_COMBO",
                            "DX_UID",
                            "VALUE_STR"
                        ]).pivot(
                            values="VALUE_STR",
                            index="CATEGORY_OPTION_COMBO",
                            columns="DX_UID",
                            aggregate_function="first"
                        )
                        
                        # Convert to dict for easier manipulation
                        pivot_data = []
                        for row in pivot_df.iter_rows(named=True):
                            coc = row["CATEGORY_OPTION_COMBO"]
                            row_data = {"CATEGORY_OPTION_COMBO": coc}
                            for dx_uid in dx_uids:
                                value = row.get(dx_uid)
                                if value is None:
                                    row_data[dx_uid] = "MISSING"
                                elif value == "NULL":
                                    row_data[dx_uid] = "NULL"
                                else:
                                    row_data[dx_uid] = str(value)
                            pivot_data.append(row_data)
                        
                        # Ensure all COCs are represented (even if no data)
                        existing_cocs = {row["CATEGORY_OPTION_COMBO"] for row in pivot_data}
                        for coc in all_cocs:
                            if coc not in existing_cocs:
                                row_data = {"CATEGORY_OPTION_COMBO": coc}
                                for dx_uid in dx_uids:
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
                        coc_width = max(len("CATEGORY_OPTION_COMBO"), max(len(coc) for coc in all_cocs) if all_cocs else 20)
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
        except Exception as e:
            safe_log_warning(f"Could not write summary.txt: {e!s}")
        
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
