from __future__ import annotations

import logging
import time
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run

from utils import (
    get_extract_config,
    load_drug_mapping,
    load_pipeline_config,
)

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
    expected_dx_uids: list[str] | None = None,
    expected_org_units: list[str] | None = None,
    extract_config_item: dict | None = None,
    extracts_folder: Path | None = None,
) -> pl.DataFrame:
    """Computes exhaustivity from extracted data based on VALUE null checks.
    
    For each combination of (PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT), checks if all DX_UIDs mappés
    (selon les mappings pour ce COC) sont présents ET ont des valeurs non-null/non-vides.
    
    Logic:
    - For each (PERIOD, ORG_UNIT, COC) combination:
      - Récupérer les DX_UIDs mappés pour ce COC spécifique (selon les mappings)
      - Vérifier que TOUS ces DX_UIDs mappés sont présents dans les données
      - Vérifier que TOUS ces DX_UIDs mappés ont des valeurs non-null/non-vides
      - Si TOUS sont présents ET non-null → exhaustivity = 1
      - Si UN DX_UID mappé est manquant OU a une valeur null/vide → exhaustivity = 0
    - Si un COC n'est pas présent dans les données → exhaustivity = 0 (COC manquant)
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    periods : list[str]
        List of periods to process.
    expected_dx_uids : list[str] | None, optional
        List of expected DX_UIDs for this extract. If provided, missing combinations
        will be marked as exhaustivity = 0.
    expected_org_units : list[str] | None, optional
        List of expected ORG_UNITs for this extract. If provided, missing combinations
        will be marked as exhaustivity = 0.
    extract_config_item : dict | None, optional
        Extract configuration dictionary from extract_config.json.
    extracts_folder : Path | None, optional
        Path to the extracts folder. If None, will be determined from extract_id.
    
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
        # Format: "Extract {extract_id}" (e.g., "Extract Fosa_exhaustivity_data_elements")
        extracts_base = pipeline_path / "data" / "extracts"
        extracts_folder = extracts_base / f"Extract {extract_id}"
    
    try:
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
                safe_log_info(
                    f"ℹ️  Expected {len(periods)} parquet files, but {len(missing_extracts)} period(s) have no data in DHIS2: "
                    f"{sorted(missing_extracts)}. "
                    f"Computing exhaustivity with available {len(periods) - len(missing_extracts)} period(s). "
                    f"Missing periods will be filled with exhaustivity=0 (normal if data doesn't exist yet in DHIS2)."
                )
        
        try:
            available_files = [f for f in files_to_read.values() if f is not None]
            safe_log_info(f"Reading {len(available_files)} parquet file(s) for exhaustivity computation")
            
            # Read all files and normalize schemas before concatenation
            # This handles cases where some files have Null columns and others have String columns
            # IMPORTANT: Select columns in a fixed order to avoid schema mismatch errors
            required_cols_order = [
                "PERIOD",
                "DX_UID",
                "CATEGORY_OPTION_COMBO",
                "ORG_UNIT",
                "VALUE",
            ]
            
            dfs = []
            for f in available_files:
                df_file = pl.read_parquet(f)
                
                # Ensure all required columns exist with correct types
                # If a column is missing or Null type, cast it to String
                for col in required_cols_order:
                    if col not in df_file.columns:
                        df_file = df_file.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
                    elif df_file[col].dtype == pl.Null:
                        # Convert Null type to String type
                        df_file = df_file.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                
                # Select only required columns in fixed order to ensure consistent schema
                # This prevents "schema names differ" errors when concatenating
                df_file = df_file.select(required_cols_order)
                
                dfs.append(df_file)
            
            # Use how="vertical_relaxed" to allow automatic type coercion if schemas differ slightly
            df = pl.concat(dfs, how="vertical_relaxed")
            safe_log_info(f"Loaded {len(df)} rows from extracted data")
        except Exception as e:
            raise RuntimeError(
                f"Error reading parquet files for exhaustivity computation: {e!s}"
            ) from e
        
        # IMPORTANT: Do NOT apply mappings before exhaustivity calculation
        # We need to calculate exhaustivity with COC SOURCE and DX_UID SOURCE values
        # The mappings define which (COC SOURCE, DX_UID SOURCE) pairs are valid
        # Mappings (COC SOURCE → COC TARGET, DX_UID SOURCE → DX_UID TARGET)
        # will be applied AFTER exhaustivity calculation
        
        # Only apply filters (org units) if provided, but keep COCs and DX_UIDs as SOURCE
        if extract_config_item:
            # Filter by specific org units if provided
            org_units_filter = extract_config_item.get("ORG_UNITS")
            if org_units_filter:
                df = df.filter(pl.col("ORG_UNIT").is_in(org_units_filter))
                safe_log_info(
                    f"Filtered by specific org units: {org_units_filter}"
                )
        
        # Note: Mappings are NOT applied here - they will be applied later
        # in apply_analytics_data_element_extract_config
        # This ensures that exhaustivity calculation uses COC SOURCE and DX_UID SOURCE
        # values and only considers valid pairs defined in extract_config.MAPPINGS
        
        # Check if dataframe is empty
        if len(df) == 0:
            safe_log_warning("DataFrame is empty after filtering, returning empty exhaustivity result")
            return pl.DataFrame({
                "PERIOD": [],
                "DX_UID": [],
                "CATEGORY_OPTION_COMBO": [],
                "ORG_UNIT": [],
                "EXHAUSTIVITY_VALUE": [],
            })
        
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
            .when(pl.col("VALUE").cast(pl.Utf8, strict=False).str.strip_chars().str.len_chars() == 0)
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
            # IMPORTANT: Select columns in a fixed order to avoid schema mismatch errors
            required_cols_order_all = [
                "PERIOD",
                "DX_UID",
                "CATEGORY_OPTION_COMBO",
                "ORG_UNIT",
                "VALUE",
            ]
            
            dfs_all = []
            for f in all_available_files:
                df_file = pl.read_parquet(f)
                # Normalize schema: ensure Null columns are cast to String
                for col in required_cols_order_all:
                    if col not in df_file.columns:
                        df_file = df_file.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
                    elif df_file[col].dtype == pl.Null:
                        df_file = df_file.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                
                # Select only required columns in fixed order to ensure consistent schema
                df_file = df_file.select(required_cols_order_all)
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
        
        # Build expected DX_UIDs per COC from drug_mapping files (source of truth for pairs)
        # The drug_mapping files define which DX_UIDs are associated with which COCs
        expected_dx_uids_by_coc: dict[str, list[str]] = {}
        
        # Load mappings from drug_mapping files via pipeline_config
        extract_mappings = {}
        if pipeline_path:
            try:
                config_dir = pipeline_path / "configuration"
                pipeline_config = load_pipeline_config(config_dir)
                
                # Load drug mapping file directly (hardcoded path based on extract_id)
                mapping_prefix = extract_id.split("_")[0].lower()  # e.g., "Fosa" from "Fosa_exhaustivity_data_elements"
                mapping_file = f"drug_mapping_{mapping_prefix}.json"
                extract_mappings, _ = load_drug_mapping(config_dir, mapping_file)
                if extract_mappings:
                    safe_log_info(
                        f"Loaded {len(extract_mappings)} mappings "
                        f"from {mapping_file} for {extract_id}"
                    )
                else:
                    safe_log_warning(f"Extract {extract_id} not found in pipeline_config")
            except Exception as e:
                safe_log_warning(f"Could not load mappings: {e!s}")
        
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
            
            # Build expected_dx_uids_by_coc from mappings
            # NOTE: In load_drug_mapping, we create {coc: coc} (source = target, no transformation)
            # The extracted data contains COCs directly from DHIS2 (no transformation)
            # So we can use coc_map.keys() or coc_map.values() - they're the same
            expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
            for dx_uid_source, mapping in extract_mappings.items():
                if relevant_dx_uids_source and dx_uid_source not in relevant_dx_uids_source:
                    continue
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                # Iterate over COCs (keys and values are identical since source = target)
                for coc in coc_map:
                    if coc is None:
                        continue
                    coc_str = str(coc).strip()
                    if not coc_str:
                        continue
                    # Index by COC (extracted data has these COCs directly from DHIS2)
                    expected_dx_uids_by_coc_sets.setdefault(coc_str, set()).add(str(dx_uid_source))
            
            expected_dx_uids_by_coc = {
                coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
            }
            logging.info(
                f"Expected DX_UIDs per COC loaded from mappings: {len(expected_dx_uids_by_coc)} COCs"
            )
        
        # Filter data by valid (DX_UID, COC) pairs from mappings (like other pipelines do)
        # This ensures we only process valid pairs according to the mapping
        if extract_mappings:
            # Build set of valid (DX_UID, COC) pairs from mappings
            valid_pairs = set()
            for dx_uid_source, mapping in extract_mappings.items():
                if relevant_dx_uids_source and dx_uid_source not in relevant_dx_uids_source:
                    continue
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                for coc in coc_map:
                    if coc is None:
                        continue
                    coc_str = str(coc).strip()
                    if coc_str:
                        valid_pairs.add((str(dx_uid_source), coc_str))
            
            if valid_pairs:
                safe_log_info(
                    f"Built {len(valid_pairs)} valid (DX_UID, COC) pairs "
                    f"from {len(extract_mappings)} mappings"
                )
            else:
                safe_log_warning("No valid (DX_UID, COC) pairs found in extract_mappings, skipping filter")
            
            if valid_pairs:
                rows_before_filter = len(df)
                safe_log_info(
                    f"Filtering data by valid (DX_UID, COC) pairs: {len(valid_pairs)} valid pairs from mapping, "
                    f"{rows_before_filter} rows before filter"
                )
                # Filter to keep only valid (DX_UID, COC) pairs
                # Create a DataFrame with valid pairs and join to filter
                valid_pairs_list = list(valid_pairs)
                valid_pairs_df = pl.DataFrame(
                    {
                        "DX_UID": [pair[0] for pair in valid_pairs_list],
                        "CATEGORY_OPTION_COMBO": [pair[1] for pair in valid_pairs_list],
                    }
                )
                df = df.join(valid_pairs_df, on=["DX_UID", "CATEGORY_OPTION_COMBO"], how="inner")
                rows_after_filter = len(df)
                if rows_before_filter != rows_after_filter:
                    safe_log_info(
                        f"Filtered data by valid (DX_UID, COC) pairs: {rows_before_filter} -> {rows_after_filter} rows "
                        f"({rows_before_filter - rows_after_filter} rows with invalid pairs removed)"
                    )
                else:
                    safe_log_info(
                        f"Filtered data by valid (DX_UID, COC) pairs: "
                        f"{rows_after_filter} rows (all data matches valid pairs)"
                    )
        else:
            safe_log_warning("extract_mappings is None or empty, skipping filter by valid pairs")
        if expected_dx_uids_by_coc:
            # Fallback: if no extract_mappings but we have expected_dx_uids_by_coc, filter by COC only
            expected_cocs = set(expected_dx_uids_by_coc.keys())
            rows_before_filter = len(df)
            df = df.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(list(expected_cocs)))
            rows_after_filter = len(df)
            if rows_before_filter != rows_after_filter:
                logging.info(
                    f"Filtered data by expected COCs (fallback): {rows_before_filter} -> {rows_after_filter} rows "
                    f"({rows_before_filter - rows_after_filter} rows with unexpected COCs removed)"
                )
        
        # 4) Fallback: if no mappings available, derive from extracted data (using SOURCE values)
        if not expected_dx_uids_by_coc:
            logging.info(
                "No mappings found in extract_config or push_config, "
                "deriving pairs from extracted data (SOURCE values)"
            )
            # Use df_all_periods_raw which has COC SOURCE and DX_UID SOURCE (before mapping)
            # Filter by extract_config.UIDS if available
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids = set(extract_config_item.get("UIDS", []))
                df_all_periods_filtered = df_all_periods_raw.filter(pl.col("DX_UID").is_in(list(relevant_dx_uids)))
            else:
                df_all_periods_filtered = df_all_periods_raw
            
            if len(df_all_periods_filtered) > 0:
                coc_dx_uids_df = (
                    df_all_periods_filtered
                    .group_by("CATEGORY_OPTION_COMBO")
                    .agg(pl.col("DX_UID").unique().sort().alias("DX_UIDs"))
                )
                expected_dx_uids_by_coc = {
                    row["CATEGORY_OPTION_COMBO"]: (
                        row["DX_UIDs"] if isinstance(row["DX_UIDs"], list)
                        else [row["DX_UIDs"]]
                    )
                    for row in coc_dx_uids_df.iter_rows(named=True)
                }
                logging.info(
                    f"Expected DX_UIDs per COC derived from extracted data "
                    f"(SOURCE values, fallback): {len(expected_dx_uids_by_coc)} COCs"
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
        
        # Check exhaustivity: for each (PERIOD, ORG_UNIT, COC), check if all
        # expected DX_UIDs (from mappings) are present AND non-null
        # Logic: exhaustivity = 1 if all expected DX_UIDs for this COC are present with non-null values, 0 otherwise
        # We only check valid (DX_UID, COC) pairs according to mappings
        # Note: Using iter_rows here is acceptable for complex set operations
        # The performance impact is minimal compared to I/O operations
        exhaustivity_rows = []
        
        # First, process all combinations present in data
        for row in df_grouped_for_log.iter_rows(named=True):
            period = row["PERIOD"]
            coc = row["CATEGORY_OPTION_COMBO"]
            org_unit = row["ORG_UNIT"]
            
            dx_uids_present_list = row["DX_UIDs"] if isinstance(row["DX_UIDs"], list) else [row["DX_UIDs"]]
            null_flags_list = row["NULL_FLAGS"] if isinstance(row["NULL_FLAGS"], list) else [row["NULL_FLAGS"]]
            
            # Get expected DX_UIDs for this COC from mappings (valid pairs only)
            expected_dx_uids = set(expected_dx_uids_by_coc.get(coc, [])) if expected_dx_uids_by_coc else set()
            
            # If no mappings, use DX_UIDs present in data (fallback)
            if not expected_dx_uids:
                expected_dx_uids = set(dx_uids_present_list)
            
            # Build a map of DX_UID -> null_flag for present DX_UIDs
            dx_uid_to_null_flag = {
                dx_uid: null_flag
                for dx_uid, null_flag in zip(dx_uids_present_list, null_flags_list, strict=False)
            }
            
            # Check if ALL DX_UIDs mappés pour ce COC sont présents ET ont des valeurs non-null
            # Logic: on vérifie seulement les DX_UIDs qui sont mappés pour ce COC spécifique
            # Si TOUS les DX_UIDs mappés sont présents ET non-null → exhaustivity = 1
            # Si UN DX_UID mappé est manquant OU a une valeur null/vide → exhaustivity = 0
            
            missing_dx_uids = []
            null_or_empty_dx_uids = []
            
            for dx_uid in expected_dx_uids:
                if dx_uid not in dx_uid_to_null_flag:
                    # DX_UID mappé manquant (pas présent dans les données)
                    missing_dx_uids.append(dx_uid)
                elif dx_uid_to_null_flag[dx_uid]:
                    # DX_UID mappé présent mais avec valeur null/vide
                    null_or_empty_dx_uids.append(dx_uid)
            
            # Exhaustivity = 1 seulement si TOUS les DX_UIDs mappés sont présents ET non-null
            exhaustivity_value = 1 if (not missing_dx_uids and not null_or_empty_dx_uids) else 0
            
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
        
        # Note: Complete grid creation with all expected ORG_UNITs and COCs
        # happens later (after exhaustivity_df is created)
        # This ensures we create the full form grid to check if hospitals filled everything correctly
        
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
            complete_count = int(exhaustivity_df["EXHAUSTIVITY_VALUE"].sum())
            pct_complete = (complete_count / total_combinations * 100) if total_combinations > 0 else 0.0
        else:
            complete_count = 0
            pct_complete = 0.0
        logging.info(f"Exhaustivity computation: {pct_complete:.1f}% complete")
        
        # Create a complete grid using push_config mappings and expected periods/org_units
        # This ensures we detect missing COCs, periods, and ORG_UNITs
        periods_in_data = exhaustivity_df["PERIOD"].unique().to_list() if len(exhaustivity_df) > 0 else []
        cocs_in_data = exhaustivity_df["CATEGORY_OPTION_COMBO"].unique().to_list() if len(exhaustivity_df) > 0 else []
        
        # Track which periods had missing parquet files (no data in DHIS2)
        # This helps us avoid redundant warnings later
        # missing_extracts is defined earlier in the function (line 137)
        periods_with_missing_files = set(missing_extracts)
        
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
        
        # If expected_org_units is provided, create a complete grid with ALL org units from the dataset
        # Org units without data will have exhaustivity=0
        # If expected_org_units is None, use only org units present in extracted data
        
        create_complete_grid = False
        expected_org_units_list = []
        
        if expected_org_units:
            # Use ALL org units from the dataset (not just those in extracted data)
            create_complete_grid = True
            expected_org_units_list = expected_org_units
            
            # Log comparison between expected and actual org units
            if len(exhaustivity_df) > 0:
                org_units_in_data = set(exhaustivity_df["ORG_UNIT"].unique().to_list())
                missing_org_units = len(expected_org_units) - len(org_units_in_data)
                logging.info(
                    f"Creating complete grid with ALL {len(expected_org_units)} expected ORG_UNITs. "
                    f"{len(org_units_in_data)} have data, {missing_org_units} will have exhaustivity=0."
                )
            else:
                logging.info(
                    f"No data available, creating grid with ALL "
                    f"{len(expected_org_units)} expected ORG_UNITs "
                    f"(all will have exhaustivity=0)."
                )
        else:
            # No expected_org_units = use only org units present in extracted data
            logging.info(
                "No expected_org_units provided - using only org units present in extracted data"
            )
        
        # Si on doit créer une grille complète
        expected_df = None
        if create_complete_grid and expected_periods and expected_org_units_list and all_expected_cocs:
            # Build expected combinations: for each COC, get all its DX_UIDs
            grid_rows = []
            
            for coc in all_expected_cocs:
                dx_uids_for_coc = expected_dx_uids_by_coc.get(coc, [])
                if not dx_uids_for_coc:
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
                avg_dx_uids = (
                    sum(len(expected_dx_uids_by_coc.get(coc, [])) for coc in all_expected_cocs)
                    / max(len(all_expected_cocs), 1)
                )
                logging.info(
                    f"Creating complete grid: {len(expected_periods)} periods x "
                    f"{len(all_expected_cocs)} COCs x {len(expected_org_units_list)} ORG_UNITs x "
                    f"avg {avg_dx_uids:.1f} DX_UIDs per COC = {total_expected} combinations"
                )
            else:
                logging.warning(
                    "No grid rows to create (expected_dx_uids_by_coc is empty "
                    "or no COCs). Skipping complete grid."
                )
        
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
                org_units_in_result = (
                    exhaustivity_df["ORG_UNIT"].unique().to_list()
                    if len(exhaustivity_df) > 0 else []
                )
                missing_org_units = set(expected_org_units_list) - set(org_units_in_result)
                
                # Only warn about periods that have parquet files but no data
                # (periods without parquet files are already logged earlier as "missing files")
                periods_with_files_but_no_data = missing_periods - periods_with_missing_files
                if periods_with_files_but_no_data:
                    safe_log_warning(
                        f"⚠️  {len(periods_with_files_but_no_data)} period(s) have parquet files but no data: "
                        f"{sorted(list(periods_with_files_but_no_data))}"
                    )
                elif missing_periods:
                    # Periods without parquet files (no data in DHIS2) - this is normal
                    safe_log_info(
                        f"ℹ️  {len(missing_periods)} period(s) have no data in DHIS2 "
                        f"(no parquet files created): {sorted(list(missing_periods))}. "
                        f"This is normal if data doesn't exist yet in DHIS2."
                    )
                
                # Only warn about COCs if they're expected but completely missing
                if missing_cocs:
                    safe_log_warning(f"Missing {len(missing_cocs)} COC(s) (no data in any period)")
            
            exhaustivity_df = complete_exhaustivity
        else:
            # Pas de grille complète créée - utiliser seulement les combinaisons présentes dans les données
            logging.info("No complete grid created - using only combinations present in extracted data")
            if len(exhaustivity_df) == 0:
                current_run.log_warning(
                    "No exhaustivity data computed. Only combinations with data in parquet files will be included."
                )
        
        # Log only if exhaustivity_df is empty (unexpected)
        if len(exhaustivity_df) == 0:
            logging.warning("Exhaustivity DataFrame is empty. No combinations computed.")
        
        return exhaustivity_df
        
    except Exception as e:
        logging.error(f"Exhaustivity computation error: {e!s}")
        current_run.log_error(f"Error computing exhaustivity: {e!s}")
        raise
