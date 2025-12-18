import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from exhaustivity_calculation import compute_exhaustivity
from d2d_library.db_queue import Queue
from d2d_library.dhis2_extract_handlers import DHIS2Extractor
from d2d_library.dhis2_pusher import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from openhexa.toolbox.dhis2.periods import period_from_string
from requests.exceptions import HTTPError, RequestException
from utils import (
    configure_logging,
    connect_to_dhis2,
    load_configuration,
    save_to_parquet,
)

# Independent implementation: no dependency on dhis2_dataset_sync pipeline

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SAN-123
#   -https://bluesquare.atlassian.net/browse/SAN-124
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs

# extract data from source DHIS2
@pipeline("dhis2_exhaustivity")
@parameter(
    code="run_ou_sync",
    name="Update dataset org units (recommended)",
    type=bool,
    default=True,
    help="Copy organisation units from source dataset to target dataset in the same DHIS2 instance.",
)
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2.",
)
@parameter(
    code="run_compute_data",
    name="Compute exhaustivity",
    type=bool,
    default=True,
    help="Compute exhaustivity values from extracted data.",
)
# push data to target DHIS2
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=False,
    help="Push data to target DHIS2. Set to True only for production runs.",
)
@parameter(
    code="run_ds_sync",
    name="Sync dataset statuses (after push)",
    type=bool,
    default=False,
    help="Sync dataset completion statuses (not implemented yet).",
)
def dhis2_exhaustivity(run_ou_sync: bool, run_extract_data: bool, run_compute_data: bool, run_push_data: bool, run_ds_sync: bool):
    """Extract data elements from the PRS DHIS2 instance.

    Compute the exhaustivity value based on required columns completeness.
    The results are then pushed back to PRS DHIS2 to the target exhaustivity data elements.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"

    try:
        # 1) Extract data from source DHIS2
        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
        )

        # 2) Compute exhaustivity from extracted data
        compute_exhaustivity_data(
            pipeline_path=pipeline_path,
            run_task=run_compute_data,
        )

        # 3) Sync dataset org units: copy org units from source dataset to target dataset
        # (both in the same DHIS2 instance - no pyramid sync needed)
        # This ensures the target dataset has the correct org units before pushing data
        if run_ou_sync:
            sync_ready = update_dataset_org_units(
                pipeline_path=pipeline_path,
                run_task=True,
            )
        else:
            sync_ready = True

        # 4) Apply mappings & push data to target DHIS2
        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=sync_ready,
        )

        # 4) Optional dataset statuses sync (after push) - not implemented yet
        # if run_push_data and run_ds_sync:
        #     # Dataset statuses sync not implemented yet
        #     pass

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_exhaustivity.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
):
    """Extracts data elements from the source DHIS2 instance and saves them in parquet format."""
    if not run_task:
        current_run.log_info("Data elements extraction task skipped.")
        return

    current_run.log_info("Data elements extraction task started.")
    configure_logging(logs_path=pipeline_path / "logs" / "extract", task_name="extract_data")

    # load configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    try:
        # Get extraction window (number of months to extract, including current month)
        extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
        
        # ENDDATE: Always use current month (datetime.now()) - overrides config value
        # Config values are kept for reference/documentation but are overridden by dynamic logic
        end = datetime.now().strftime("%Y%m")
        config_enddate = extract_config["SETTINGS"].get("ENDDATE")
        if config_enddate:
            current_run.log_info(f"ENDDATE in config ({config_enddate}) overridden by dynamic date: {end}")
        
        # STARTDATE: Always calculated from ENDDATE and EXTRACTION_MONTHS_WINDOW - overrides config value
        # Example: if EXTRACTION_MONTHS_WINDOW=3 and ENDDATE=202412, STARTDATE=202410 (3 months: Oct, Nov, Dec)
        end_date = datetime.strptime(end, "%Y%m")
        start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
        config_startdate = extract_config["SETTINGS"].get("STARTDATE")
        if config_startdate:
            current_run.log_info(f"STARTDATE in config ({config_startdate}) overridden by dynamic calculation: {start}")
        
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

    # For extraction, use STARTDATE to ENDDATE (same range for exhaustivity computation)
    start_extraction = start

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    download_settings = extract_config["SETTINGS"].get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

    # Setup extractor
    # See docs about return_existing_file impact.
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client, download_mode=download_settings, return_existing_file=False
    )
    current_run.log_info(
        f"Download MODE: {extract_config['SETTINGS']['MODE']} - Extraction and Exhaustivity: {start_extraction} to {end} "
        f"({extraction_window} months including current month)"
    )

    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        extract_periods=get_periods(start_extraction, end),
        extract_config=extract_config,
        )


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    extract_periods: list[str],
    extract_config: dict = None,
):
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    current_run.log_info("Starting data element extracts.")
    source_datasets = get_datasets(dhis2_extractor.dhis2_client)
    # NOTE: We do NOT retrieve the full pyramid here - we retrieve org units per extract as needed

    # loop over the available extract configurations
    for idx, extract in enumerate(data_element_extracts):
        extract_id = extract.get("EXTRACT_UID")
        org_units_level = extract.get("ORG_UNITS_LEVEL", None)
        data_element_uids = extract.get("UIDS", [])
        dataset_orgunits_uid = extract.get("DATASET_ORGUNITS_UID")

        if extract_id is None:
            current_run.log_warning(
                f"No 'EXTRACT_UID' defined for extract position: {idx}. This is required, extract skipped."
            )
            continue

        if org_units_level is None:
            current_run.log_warning(f"No 'ORG_UNITS_LEVEL' defined for extract: {extract_id}, extract skipped.")
            continue

        if len(data_element_uids) == 0:
            current_run.log_warning(f"No data elements defined for extract: {extract_id}, extract skipped.")
            continue

        # get org units from the dataset directly if DATASET_ORGUNITS_UID is provided, otherwise use org_units_level
        # NEVER retrieve full pyramid - always filter by dataset or level
        if dataset_orgunits_uid is not None:
            source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_orgunits_uid]))
            org_units = source_dataset["organisation_units"].explode().to_list()
            dataset_name = source_dataset["name"][0] if len(source_dataset) > 0 else "Unknown"
        else:
            # Use retrieve_ou_list which filters by level directly (does NOT retrieve full pyramid)
            # This is more efficient than retrieving full pyramid and filtering
            from utils import retrieve_ou_list
            org_units = retrieve_ou_list(dhis2_client=dhis2_extractor.dhis2_client, ou_level=org_units_level)
            dataset_name = f"Level {org_units_level}"
        
        # Limit org units for testing if MAX_ORG_UNITS_FOR_TEST is set in config
        if extract_config and extract_config.get("SETTINGS", {}).get("MAX_ORG_UNITS_FOR_TEST"):
            max_org_units = extract_config["SETTINGS"]["MAX_ORG_UNITS_FOR_TEST"]
            original_count = len(org_units)
            if len(org_units) > max_org_units:
                org_units = org_units[:max_org_units]
                current_run.log_info(
                    f"Limited to {max_org_units} org units for testing (from {original_count} total)"
                )

        current_run.log_info(
            f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
            f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
            f"(dataset: {dataset_name})."
        )

        # Create folder name based on org units level from config
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        else:
            folder_name = f"Extract {extract_id}"

        # run data elements extraction per period
        for period in extract_periods:
            try:
                dhis2_extractor.analytics_data_elements.download_period(
                    data_elements=data_element_uids,
                    org_units=org_units,
                    period=period,
                    output_dir=pipeline_path / "data" / "extracts" / folder_name,
                )

            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                # Log more details about the error
                current_run.log_warning(
                    f"Extract {extract_id} download failed for period {period} ({error_type}): {error_msg[:200]}. Continuing with next period."
                )
                logging.error(f"Extract {extract_id} - period {period} error ({error_type}): {e!s}")
                # Log full traceback for debugging
                import traceback
                logging.error(f"Full traceback for period {period}:\n{traceback.format_exc()}")
                continue  # continue with next period instead of stopping the entire extract

        current_run.log_info(f"Extract {extract_id} finished.")


def get_periods(start: str, end: str) -> list[str]:
    """Generate a list of period strings between the start and end periods (inclusive).

    Parameters
    ----------
    start : str
        The start period as a string (e.g., '202301').
    end : str
        The end period as a string (e.g., '202312').

    Returns
    -------
    list[str]
        List of period strings from start to end.
    """
    try:
        start_period = period_from_string(start)
        end_period = period_from_string(end)
        return (
            [str(p) for p in start_period.get_range(end_period)]
            if str(start_period) < str(end_period)
            else [str(start_period)]
        )
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e!s}") from e


@dhis2_exhaustivity.task
def compute_exhaustivity_data(
    pipeline_path: Path,
    run_task: bool = True,
) -> None:
    """Computes exhaustivity from extracted data and enqueues the result for pushing.
    
    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    run_task : bool
        Whether to run the task or skip it.
    """
    if not run_task:
        current_run.log_info("Exhaustivity computation task skipped.")
        return

    current_run.log_info("Exhaustivity computation task started.")
    try:
        configure_logging(logs_path=pipeline_path / "logs" / "compute", task_name="compute_exhaustivity")
    except Exception as e:
        # If configure_logging fails (e.g., in test environment), continue anyway
        current_run.log_warning(f"Could not configure logging: {e!s}")

    # Load extract config to get extracts and date range
    from utils import load_configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    
    # Get date range (same logic as extract_data)
    extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    
    # Initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)
    
    # Compute exhaustivity for all extracts
    exhaustivity_periods = get_periods(start, end)
    current_run.log_info(f"Processing {len(extract_config['DATA_ELEMENTS'].get('EXTRACTS', []))} extracts for {len(exhaustivity_periods)} periods")
    for target_extract in extract_config["DATA_ELEMENTS"].get("EXTRACTS", []):
        extract_id = target_extract.get("EXTRACT_UID")
        current_run.log_info(f"Computing exhaustivity for extract: {extract_id}")
        compute_exhaustivity_and_queue(
            pipeline_path=pipeline_path,
            extract_id=extract_id,
            exhaustivity_periods=exhaustivity_periods,
            push_queue=push_queue,
        )
        current_run.log_info(f"Completed exhaustivity computation for extract: {extract_id}")


def compute_exhaustivity_and_queue(
    pipeline_path: Path,
    extract_id: str,
    exhaustivity_periods: list[str],
    push_queue: Queue,
) -> None:
    """Computes exhaustivity from extracted data and enqueues the result for pushing.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    exhaustivity_periods : list[str]
        List of periods to process.
    push_queue : Queue
        Queue to enqueue the exhaustivity result file for pushing.
    """
    # Load config to get org units level for folder naming
    from utils import load_configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    
    # Find the extract configuration to determine folder name
    extract_config_item = None
    for extract_item in extract_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
        if extract_item.get("EXTRACT_UID") == extract_id:
            extract_config_item = extract_item
            break
    
    # Create folder name based on org units level from config
    if extract_config_item:
        org_units_level = extract_config_item.get("ORG_UNITS_LEVEL")
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        else:
            folder_name = f"Extract {extract_id}"
    else:
        # Fallback: use extract_id if config not found
        folder_name = f"Extract {extract_id}"
    
    extracts_folder = pipeline_path / "data" / "extracts" / folder_name
    output_dir = pipeline_path / "data" / "processed" / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean summary file at the start for this extract
    
    try:
        # Get expected DX_UIDs and ORG_UNITs from extract configuration
        # (extract_config already loaded above)
        
        expected_dx_uids = None
        expected_org_units = None
        
        if extract_config_item:
            expected_dx_uids = extract_config_item.get("UIDS", [])
            # Get expected org units: priority order:
            # 1) extract_config.ORG_UNITS (explicit list)
            # 2) SOURCE_DATASET_UID from push_config (org units from source dataset)
            # 3) ORG_UNITS_LEVEL from extract_config (filter pyramid by level, NOT full pyramid)
            # 4) extracted data (all unique ORG_UNITs across all periods) - last resort
            if extract_config_item.get("ORG_UNITS"):
                expected_org_units = extract_config_item.get("ORG_UNITS")
                current_run.log_info(
                    f"Using {len(expected_org_units)} expected ORG_UNITs from extract_config.ORG_UNITS"
                )
            else:
                # Try to get from SOURCE_DATASET_UID in push_config (preferred method)
                try:
                    from utils import load_configuration
                    push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
                    push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
                    source_dataset_uid = None
                    for push_extract in push_extracts:
                        if push_extract.get("EXTRACT_UID") == extract_id:
                            source_dataset_uid = push_extract.get("SOURCE_DATASET_UID")
                            break
                    
                    if source_dataset_uid:
                        # Get org units from source dataset (more limited than full pyramid)
                        from openhexa.toolbox.dhis2.dataframe import get_datasets
                        from utils import connect_to_dhis2
                        config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
                        dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)
                        datasets = get_datasets(dhis2_client)
                        source_dataset = datasets.filter(pl.col("id").is_in([source_dataset_uid]))
                        if len(source_dataset) > 0:
                            expected_org_units = source_dataset["organisation_units"].explode().to_list()
                            current_run.log_info(
                                f"Using {len(expected_org_units)} ORG_UNITs from SOURCE_DATASET_UID "
                                f"({source_dataset_uid}) in push_config"
                            )
                        else:
                            raise ValueError(f"Source dataset {source_dataset_uid} not found")
                    else:
                        raise ValueError("No SOURCE_DATASET_UID found in push_config")
                except Exception as e:
                    current_run.log_warning(
                        f"Could not get org units from SOURCE_DATASET_UID: {e}. "
                        f"Falling back to extracted data (never retrieving full pyramid)."
                    )
                    # Fallback: get from extracted data (NEVER retrieve full pyramid)
                    # This ensures we never retrieve the full pyramid, only use what we already extracted
                    try:
                        # Read all parquet files to get all org units
                        all_period_files = list(extracts_folder.glob("data_*.parquet"))
                        if all_period_files:
                            # Read all files and normalize schemas before concatenation
                            dfs_all = []
                            for f in all_period_files:
                                df_file = pl.read_parquet(f)
                                # Normalize schema: ensure Null columns are cast to String
                                for col in df_file.columns:
                                    if df_file[col].dtype == pl.Null:
                                        df_file = df_file.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                                dfs_all.append(df_file)
                            all_data = pl.concat(dfs_all, how="vertical_relaxed")
                            expected_org_units = all_data["ORG_UNIT"].unique().to_list()
                            current_run.log_info(
                                f"Using {len(expected_org_units)} ORG_UNITs from extracted data "
                                f"(last resort fallback)"
                            )
                        else:
                            expected_org_units = None
                    except Exception as e3:
                        current_run.log_warning(f"Could not determine expected org units: {e3}")
                        expected_org_units = None
        
        # Compute exhaustivity values for ALL periods at once
        # This ensures we detect missing periods and create a complete grid
        # Returns DataFrame with PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
        current_run.log_info(
            f"Computing exhaustivity for {len(exhaustivity_periods)} periods: {exhaustivity_periods}. "
            f"Missing periods will be filled with exhaustivity=0."
        )
        exhaustivity_df = compute_exhaustivity(
            pipeline_path=pipeline_path,
            extract_id=extract_id,
            periods=exhaustivity_periods,  # Pass all periods
            expected_dx_uids=expected_dx_uids,
            expected_org_units=expected_org_units,
            extract_config_item=extract_config_item,
            extracts_folder=extracts_folder,
        )

        # Save exhaustivity data per period
        current_run.log_info(f"Exhaustivity computation completed: {len(exhaustivity_df)} total combinations across all periods")
        
        # Get all periods from the result (may include periods not in exhaustivity_periods if data exists)
        if len(exhaustivity_df) > 0 and "PERIOD" in exhaustivity_df.columns:
            periods_in_result = sorted(exhaustivity_df["PERIOD"].unique().to_list())
        else:
            periods_in_result = []
        current_run.log_info(f"Periods in exhaustivity result: {periods_in_result}")
        current_run.log_info(f"Periods requested: {exhaustivity_periods}")
        
        # Use periods from result if available, otherwise use requested periods
        # This ensures we save files for all periods that have data (even if not in exhaustivity_periods)
        # Also include all requested periods to ensure we create files even if they have no data (exhaustivity=0)
        periods_to_save = sorted(set(periods_in_result + exhaustivity_periods))
        current_run.log_info(f"Periods to save: {periods_to_save}")
        
        for period in periods_to_save:
            # Filter for the current period
            period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
            
            if len(period_exhaustivity) == 0:
                # With complete grid, this should not happen, but if it does, log and skip
                current_run.log_warning(
                    f"No exhaustivity data computed for period {period}. "
                    f"This may indicate missing data or configuration issues. "
                    f"Expected periods in result: {sorted(exhaustivity_df['PERIOD'].unique().to_list()) if len(exhaustivity_df) > 0 else []}"
                )
                continue

            # Save exhaustivity data with DX_UID (matching CMM format)
            # Keep all columns: PERIOD, DX_UID, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
            period_exhaustivity_simplified = period_exhaustivity.select([
                "PERIOD",
                "DX_UID",
                "CATEGORY_OPTION_COMBO",
                "ORG_UNIT",
                "EXHAUSTIVITY_VALUE"
            ])

            try:
                save_to_parquet(
                    data=period_exhaustivity_simplified,
                    filename=output_dir / f"exhaustivity_{period}.parquet",
                )
                push_queue.enqueue(f"{extract_id}|{output_dir / f'exhaustivity_{period}.parquet'}")
                current_run.log_info(f"Saved exhaustivity data for period {period}: {len(period_exhaustivity)} combinations")
            except Exception as e:
                logging.error(f"Exhaustivity saving error: {e!s}")
                current_run.log_error(f"Error saving exhaustivity parquet file for period {period}.")
    finally:
        current_run.log_info("Exhaustivity computation finished.")
        push_queue.enqueue("FINISH")


def format_for_exhaustivity_import(
    exhaustivity_df: pl.DataFrame, 
    original_data: pl.DataFrame = None,
    pipeline_path: Path = None,
    extract_id: str = None,
    extract_config_item: dict = None,
) -> pl.DataFrame:
    """Formats the exhaustivity DataFrame for DHIS2 import.

    Parameters
    ----------
    exhaustivity_df : pl.DataFrame
        DataFrame with columns: PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
    original_data : pl.DataFrame, optional
        Original data to determine expected DX_UIDs for each COC. If not provided, will try to use push_config.
    pipeline_path : Path, optional
        Path to the pipeline directory. Required if original_data is not provided.
    extract_id : str, optional
        Extract ID. Used to match with push_config if needed.
    extract_config_item : dict, optional
        Extract configuration item. Used to determine relevant DX_UIDs.

    Returns
    -------
    pl.DataFrame
        A DataFrame formatted for DHIS2 import with columns:
        DATA_TYPE, DX_UID, PERIOD, ORG_UNIT, VALUE, etc.
        Creates one entry per DX_UID expected for each COC.
    """
    # Determine expected DX_UIDs for each COC
    # Priority: 1) push_config, 2) original_data, 3) exhaustivity_df COCs (fallback)
    # Note: expected_dx_uids_by_coc is indexed by SOURCE COC (from exhaustivity files)
    # The COC mapping (source → target) will be done later in apply_analytics_data_element_extract_config
    expected_dx_uids_by_coc: dict[str, list[str]] = {}
    
            # 1) Try to load from push_config (source of truth)
    if pipeline_path:
        try:
            from utils import load_configuration
            push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
            push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
            # Use all mappings from all extracts (like the working version)
            push_mappings: dict[str, dict] = {}
            for push_extract in push_extracts:
                push_mappings.update(push_extract.get("MAPPINGS", {}))

            # Limit to DX_UIDs that are relevant for this extract (from extract_config, not from original_data)
            # We want to include ALL DX_UIDs from push_config that are in the extract config,
            # even if they don't have data in original_data (they should still get exhaustivity=0 entries)
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids = set(extract_config_item.get("UIDS", []))
            else:
                # If no extract_config_item, use all DX_UIDs from push_config
                relevant_dx_uids = None

            expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
            for dx_uid, mapping in push_mappings.items():
                if relevant_dx_uids and dx_uid not in relevant_dx_uids:
                    continue
                # Use source UID (dx_uid) - the mapping to target UID will be done later in apply_analytics_data_element_extract_config
                # This matches the working version logic
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                # Build expected_dx_uids_by_coc indexed by SOURCE COC (from exhaustivity files)
                # The COC mapping (source → target) will be done later in apply_analytics_data_element_extract_config
                for src_coc, target_coc in coc_map.items():
                    if target_coc is None:
                        continue
                    src_coc_str = str(src_coc).strip()
                    if not src_coc_str:
                        continue
                    # Index by SOURCE COC (from exhaustivity files), not target COC
                    # Use source UID (dx_uid) - mapping will be applied later
                    expected_dx_uids_by_coc_sets.setdefault(src_coc_str, set()).add(str(dx_uid))

            expected_dx_uids_by_coc = {
                coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
            }
            # Log summary only (no detailed info to avoid API overload)
            if expected_dx_uids_by_coc:
                current_run.log_info(
                    f"Loaded push_config mappings: {len(expected_dx_uids_by_coc)} COCs"
                )
        except Exception as e:
            current_run.log_warning(f"Could not load expected DX_UIDs per COC from push_config: {e!s}")
            expected_dx_uids_by_coc = {}
    
    # 2) Fallback: use original_data if push_config didn't provide anything
    if not expected_dx_uids_by_coc and original_data is not None and len(original_data) > 0:
        current_run.log_warning(
            "format_for_exhaustivity_import: Using fallback to original_data for expected DX_UIDs per COC "
            "(push_config did not provide mappings)"
        )
        # Group by COC to get expected DX_UIDs
        expected_dx_uids_by_coc_df = (
            original_data.group_by("CATEGORY_OPTION_COMBO")
            .agg(pl.col("DX_UID").unique().alias("DX_UIDs"))
        )
        expected_dx_uids_by_coc = {
            row["CATEGORY_OPTION_COMBO"]: sorted(row["DX_UIDs"] if isinstance(row["DX_UIDs"], list) else [row["DX_UIDs"]])
            for row in expected_dx_uids_by_coc_df.iter_rows(named=True)
        }
    
    # 3) If still no mapping, we can't determine expected DX_UIDs
    if not expected_dx_uids_by_coc:
        current_run.log_warning(
            "No original data or push_config mapping available to format_for_exhaustivity_import, "
            "cannot determine expected DX_UIDs"
        )
        return pl.DataFrame({
            "DATA_TYPE": [],
            "DX_UID": [],
            "PERIOD": [],
            "ORG_UNIT": [],
            "CATEGORY_OPTION_COMBO": [],
            "ATTRIBUTE_OPTION_COMBO": [],
            "VALUE": [],
            "RATE_TYPE": [],
            "DOMAIN_TYPE": [],
        })
    
    # DX_UID is already in exhaustivity_df (like CMM format)
    # Just format for DHIS2 import by adding required columns (like format_for_import in CMM pipeline)
    current_run.log_info(
        f"Formatting exhaustivity data for import: {len(exhaustivity_df)} entries "
        f"(DX_UID already present, matching CMM format)"
    )
    
    # Check if DX_UID is present (should be if compute_exhaustivity was updated correctly)
    if "DX_UID" not in exhaustivity_df.columns:
        current_run.log_warning(
            "DX_UID not found in exhaustivity_df. Falling back to COC expansion logic."
        )
        # Fallback: expand COC → DX_UID (old logic)
        expanded_rows = []
        for row in exhaustivity_df.iter_rows(named=True):
            period = row["PERIOD"]
            source_coc = row["CATEGORY_OPTION_COMBO"]
            org_unit = row["ORG_UNIT"]
            exhaustivity_value = row["EXHAUSTIVITY_VALUE"]
            dx_uids = expected_dx_uids_by_coc.get(source_coc, [])
            if dx_uids:
                for dx_uid in dx_uids:
                    expanded_rows.append({
                        "PERIOD": period,
                        "DX_UID": str(dx_uid),
                        "ORG_UNIT": org_unit,
                        "CATEGORY_OPTION_COMBO": source_coc,
                        "EXHAUSTIVITY_VALUE": exhaustivity_value,
                    })
        if not expanded_rows:
            return pl.DataFrame({
                "DATA_TYPE": [], "DX_UID": [], "PERIOD": [], "ORG_UNIT": [],
                "CATEGORY_OPTION_COMBO": [], "ATTRIBUTE_OPTION_COMBO": [],
                "VALUE": [], "RATE_TYPE": [], "DOMAIN_TYPE": [],
            })
        exhaustivity_df = pl.DataFrame(expanded_rows)
    
    # Format for DHIS2 import (like format_for_import in CMM pipeline)
    df_final = exhaustivity_df.with_columns([
        pl.lit("DATA_ELEMENT").alias("DATA_TYPE"),
        pl.col("DX_UID").cast(pl.Utf8).alias("DX_UID"),  # Already present, ensure String type
        pl.col("CATEGORY_OPTION_COMBO").cast(pl.Utf8).alias("CATEGORY_OPTION_COMBO"),  # Already present
        pl.col("EXHAUSTIVITY_VALUE").cast(pl.Int64).alias("VALUE"),
        pl.lit(None).cast(pl.Utf8).alias("ATTRIBUTE_OPTION_COMBO"),
        pl.lit(None).cast(pl.Utf8).alias("RATE_TYPE"),
        pl.lit("AGGREGATED").alias("DOMAIN_TYPE"),
    ]).select([
        "DATA_TYPE",
        "DX_UID",
        "PERIOD",
        "ORG_UNIT",
        "CATEGORY_OPTION_COMBO",
        "ATTRIBUTE_OPTION_COMBO",
        "VALUE",
        "RATE_TYPE",
        "DOMAIN_TYPE",
    ])
    
    # Return Polars DataFrame (DHIS2Pusher accepts Polars)
    return df_final


@dhis2_exhaustivity.task
def update_dataset_org_units(
    pipeline_path: Path,
    run_task: bool = True,
) -> bool:
    """Updates the organisation units of datasets in the PRS DHIS2 instance.

    NOTE: This is PRS specific.

    Returns
    -------
    bool
        True if the update was performed, False if skipped.
    """
    if not run_task:
        current_run.log_info("Update dataset org units task skipped.")
        return True

    try:
        current_run.log_info("Starting update of dataset organisation units.")

        # Copy org units from source datasets to target datasets (both in the same DHIS2 instance)
        # Read all dataset mappings from push_config.json
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")
        config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")
        dhis2_client = connect_to_dhis2(connection_str=prs_conn, cache_dir=None)

        # Collect all dataset mappings from extracts that have TARGET_DATASET_UID
        # This includes both regular extracts and exhaustivity extracts
        dataset_mappings = []
        for extract in config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
            source_ds = extract.get("SOURCE_DATASET_UID")
            target_ds = extract.get("TARGET_DATASET_UID")
            extract_id = extract.get("EXTRACT_UID", "unknown")
            
            # For all extracts (regular and exhaustivity), we need both SOURCE_DATASET_UID and TARGET_DATASET_UID
            # to sync org units from source dataset to target dataset (both in the same DHIS2 instance)
            if target_ds and source_ds:
                dataset_mappings.append({
                    "source": source_ds,
                    "target": target_ds,
                    "extract_id": extract_id
                })

        if not dataset_mappings:
            current_run.log_warning("No dataset mappings found. Skipping dataset org units sync.")
            return True

        current_run.log_info(f"Found {len(dataset_mappings)} dataset mapping(s) to sync.")
        
        # Sync each dataset mapping
        # Copy org units from source dataset to target dataset (both in the same DHIS2 instance)
        for mapping in dataset_mappings:
            current_run.log_info(
                f"Syncing org units for '{mapping['extract_id']}': "
                f"{mapping['source']} -> {mapping['target']}"
            )
            sync_result = push_dataset_org_units(
                dhis2_client=dhis2_client,
                source_dataset_id=mapping["source"],
                target_dataset_id=mapping["target"],
            dry_run=config["SETTINGS"].get("DRY_RUN", True),
        )
            
            # Check if sync failed
            if sync_result and "error" in sync_result:
                error_msg = f"Failed to sync org units for '{mapping['extract_id']}': {sync_result.get('error')}"
                current_run.log_error(error_msg)
                logging.error(error_msg)
                # Continue with other mappings but log the error
                continue

    except Exception as e:
        current_run.log_error("An error occurred during dataset org units update. Process stopped.")
        logging.error(f"An error occurred during dataset org units update: {e}")
        raise

    return True


def push_dataset_org_units(
    dhis2_client: DHIS2, source_dataset_id: str, target_dataset_id: str, dry_run: bool = True
) -> dict:
    """Updates the organisation units of a DHIS2 dataset.

    Parameters
    ----------
    dhis2_client : DHIS2
        DHIS2 client for the target instance.
    source_dataset_id : str
        The ID of the dataset from where to retrieve the org unit ids.
    target_dataset_id : str
        The ID of the dataset to be updated.
    dry_run : bool, optional
        If True, performs a dry run without making changes (default is True).

    Returns
    -------
    dict
        The response from the DHIS2 API, or an error payload.
    """
    datasets = get_datasets(dhis2_client)
    source_dataset = datasets.filter(pl.col("id").is_in([source_dataset_id]))
    target_dataset = datasets.filter(pl.col("id").is_in([target_dataset_id]))
    
    # Validate that both datasets exist
    if len(source_dataset) == 0:
        error_msg = f"Source dataset {source_dataset_id} not found in DHIS2"
        current_run.log_error(error_msg)
        logging.error(error_msg)
        return {"error": error_msg, "status_code": 404}
    
    if len(target_dataset) == 0:
        error_msg = f"Target dataset {target_dataset_id} not found in DHIS2"
        current_run.log_error(error_msg)
        logging.error(error_msg)
        return {"error": error_msg, "status_code": 404}
    
    source_ous = source_dataset["organisation_units"].explode().to_list()
    target_ous = target_dataset["organisation_units"].explode().to_list()

    # here first check if the list of ids is different
    new_org_units = set(source_ous) - set(target_ous)
    if len(new_org_units) == 0:
        current_run.log_info("Source and target dataset organisation units are in sync, no update needed.")
        return {"status": "skipped", "message": "No update needed, org units are identical."}

    current_run.log_info(
        f"Found {len(new_org_units)} new org units to add to target dataset "
        f"'{target_dataset['name'].item()}' ({target_dataset_id})."
    )

    # Step 1: GET current dataset
    url = f"{dhis2_client.api.url}/dataSets/{target_dataset_id}"
    dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    if "error" in dataset_payload:
        return dataset_payload

    # Step 2: Update organisationUnits (just push the source OUs)
    dataset_payload["organisationUnits"] = [{"id": ou_id} for ou_id in source_ous]

    # Step 3: PUT updated dataset
    update_response = dhis2_request(
        dhis2_client.api.session, "put", url, json=dataset_payload, params={"dryRun": str(dry_run).lower()}
    )

    if "error" in update_response:
        current_run.log_info(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
        logging.error(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
    else:
        msg = f"Dataset {target_dataset['name'].item()} ({target_dataset_id}) org units updated: {len(source_ous)}"
        current_run.log_info(msg)
        logging.info(msg)

    return update_response


@dhis2_exhaustivity.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
):
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return True

    current_run.log_info("Starting data push.")

    # setup
    configure_logging(logs_path=pipeline_path / "logs" / "push", task_name="push_data")
    # load_configuration is imported at module level, use it directly
    config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
    dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    # Push parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)
    push_wait = config["SETTINGS"].get("PUSH_WAIT_MINUTES", 5)

    # log parameters
    logging.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")
    current_run.log_info(
        f"Pushing data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=dhis2_client,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
    )

    # Map data types to their respective mapping functions
    # Due to the old version of PRS DHIS2 2.37, we retrieve data from analytics endpoint (not dataValueSets)
    dispatch_map = {
        "DATA_ELEMENT": (config["DATA_ELEMENTS"]["EXTRACTS"], apply_analytics_data_element_extract_config),
        "REPORTING_RATE": (config["REPORTING_RATES"]["EXTRACTS"], apply_reporting_rates_extract_config),
        "INDICATOR": (config["INDICATORS"]["EXTRACTS"], apply_indicators_extract_config),
    }

    # loop over the queue
    while True:
        next_period = push_queue.peek()
        if next_period == "FINISH":
            push_queue.dequeue()  # remove marker if present
            break

        if not next_period:
            current_run.log_info("Push data process: waiting for updates")
            time.sleep(60 * int(push_wait))
            continue

        try:
            # Read file path from queue
            _, extract_file_path = split_on_pipe(next_period)
            extract_path = Path(extract_file_path)
            # Read with polars (keep everything in Polars)
            extract_data = pl.read_parquet(extract_path)
            short_path = f"{extract_path.parent.name}/{extract_path.name}"
            
            # Determine extract_id from file path (ignore extract_id from queue)
            # Match folder name pattern "Extract lvl X" with ORG_UNITS_LEVEL in extract_config
            extract_config_full = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
            extracts_list = extract_config_full.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
            parent_folder = extract_path.parent.name
            
            # Extract level from folder name and find matching extract
            extract_id = None
            if "lvl" in parent_folder.lower():
                try:
                    level_from_folder = int(parent_folder.split("lvl")[-1].strip().split()[0])
                    # Find extract with matching ORG_UNITS_LEVEL
                    matching_extract = next(
                        (e for e in extracts_list
                         if e.get("ORG_UNITS_LEVEL") == level_from_folder),
                        None
                    )
                    if matching_extract:
                        extract_id = matching_extract.get("EXTRACT_UID")
                except (ValueError, IndexError):
                    pass
            
            if not extract_id:
                current_run.log_error(f"Could not determine extract_id from folder path: {parent_folder}. Skipping.")
                push_queue.dequeue()
                continue
        except Exception as e:
            current_run.log_error(f"Failed to read extract from queue item: {next_period}. Error: {e}")
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            current_run.log_info(f"Pushing data for extract {extract_id}: {short_path}.")
            
            # Detect exhaustivity files by presence of CATEGORY_OPTION_COMBO column
            # (exhaustivity files have COC format, other files have DX_UID format)
            is_exhaustivity = "CATEGORY_OPTION_COMBO" in extract_data.columns
            
            if is_exhaustivity:
                # Handle EXHAUSTIVITY data: expand from COC format to DX_UID format
                # extract_data is already Polars
                exhaustivity_df_pl = extract_data
                
                # Load extract config to get expected DX_UIDs
                # extract_config_full already loaded above, reuse it
                extract_config_item = next(
                    (e for e in extracts_list if e.get("EXTRACT_UID") == extract_id),
                    None
                )
                
                # Read original data if available (for fallback)
                # Use simple folder detection logic (like the working version)
                original_data = None
                extracts_folder = pipeline_path / "data" / "extracts"
                if "Fosa" in extract_id:
                    extracts_folder = extracts_folder / "Extract lvl 5"
                elif "BCZ" in extract_id:
                    extracts_folder = extracts_folder / "Extract lvl 3"
                else:
                    for folder in extracts_folder.iterdir():
                        if folder.is_dir() and extract_id in folder.name:
                            extracts_folder = folder
                            break
                
                # Try to read original data from the same period
                period = exhaustivity_df_pl["PERIOD"].unique().to_list()[0] if len(exhaustivity_df_pl) > 0 else None
                if period:
                    period_file = extracts_folder / f"data_{period}.parquet"
                    if period_file.exists():
                        try:
                            original_data = pl.read_parquet(period_file)
                        except Exception:
                            pass
                
                # Expand from COC format to DX_UID format for DHIS2 import
                # Format for DHIS2 import (DX_UID is already in exhaustivity file, like CMM format)
                # Just add required columns, no need to expand COC → DX_UID
                df_final = format_for_exhaustivity_import(
                    exhaustivity_df_pl,  # Already contains DX_UID, PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
                    original_data=original_data,
                    pipeline_path=pipeline_path,
                    extract_id=extract_id,
                    extract_config_item=extract_config_item,
                )
                
                # Check if format_for_exhaustivity_import returned empty DataFrame
                # df_final is now Polars (from format_for_exhaustivity_import)
                if df_final.is_empty():
                    current_run.log_warning(f"format_for_exhaustivity_import returned empty DataFrame for {extract_id}, skipping push")
                    push_queue.dequeue()
                    continue
                
                # Apply mapping and push data
                # Use the extract_id we determined (should match push_config now)
                cfg_list, mapper_func = dispatch_map["DATA_ELEMENT"]
                extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})
                
                if not extract_config:
                    current_run.log_warning(f"Extract config not found in push_config for {extract_id}, pushing without mappings")
                
                df_mapped = mapper_func(df=df_final, extract_config=extract_config)
                
                # Check if mapped DataFrame is empty (df_mapped is polars from apply_analytics_data_element_extract_config)
                if df_mapped.is_empty() if hasattr(df_mapped, 'is_empty') else len(df_mapped) == 0:
                    current_run.log_warning(f"Mapped DataFrame is empty for {extract_id}, skipping push")
                    push_queue.dequeue()
                    continue
                
                # df_mapped is already Polars (apply_analytics_data_element_extract_config returns pl.DataFrame)
                pusher.push_data(df_data=df_mapped)
            
            else:
                # Handle other data types (DATA_ELEMENT, REPORTING_RATE, INDICATOR)
                # Determine data type from DATA_TYPE column
                if "DATA_TYPE" not in extract_data.columns:
                    current_run.log_warning(f"No DATA_TYPE column in extract: {short_path}. Skipping.")
                    push_queue.dequeue()  # remove unknown item
                    continue
                
                # extract_data is Polars DataFrame, so unique() returns Polars Series
                data_type = extract_data["DATA_TYPE"].unique().to_list()[0]
                
            if data_type not in dispatch_map:
                current_run.log_warning(f"Unknown DATA_TYPE '{data_type}' in extract: {short_path}. Skipping.")
                push_queue.dequeue()  # remove unknown item
                continue

            # Get config and mapping function
            cfg_list, mapper_func = dispatch_map[data_type]
            extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})

            # Apply mapping and push data
            df_mapped = mapper_func(df=extract_data, extract_config=extract_config)
            pusher.push_data(df_data=df_mapped)

            # Success → dequeue
            push_queue.dequeue()
            current_run.log_info(f"Data push finished for extract: {short_path}.")

        except Exception as e:
            error_msg = f"Fatal error for extract {extract_id} ({short_path}): {e!s}"
            current_run.log_error(f"Fatal error for extract {extract_id} ({short_path}), stopping push process.")
            logging.error(error_msg)
            # Log full traceback for debugging
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            raise  # crash on error

    current_run.log_info("Data push task finished.")
    return True


def split_on_pipe(s: str) -> tuple[str, str | None]:
    """Splits a string on the first pipe character and returns a tuple.

    Parameters
    ----------
    s : str
        The string to split.

    Returns
    -------
    tuple[str, str | None]
        A tuple containing the part before the pipe and the part after the pipe (or None if no pipe is found).
    """
    parts = s.split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]


def apply_analytics_data_element_extract_config(df, extract_config: dict):
    """Applies data element mappings to the extracted data.

    It also filters data elements based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame or pl.DataFrame
        DataFrame containing the extracted data (pandas or polars).
    extract_config : dict
        This is a dictionary containing the extract mappings.

    Returns
    -------
    pl.DataFrame
        DataFrame with mapped data elements (always returns polars).
    """
    # Handle empty DataFrame - support both pandas and polars
    if isinstance(df, pd.DataFrame):
        if len(df) == 0:
            current_run.log_warning("Empty DataFrame provided to apply_analytics_data_element_extract_config, returning empty DataFrame.")
            return pl.DataFrame(schema={col: pl.Utf8 for col in df.columns})
        # Convert pandas to polars for processing (like the working version)
        df_pl = pl.from_pandas(df)
    else:
        if df.is_empty():
            current_run.log_warning("Empty DataFrame provided to apply_analytics_data_element_extract_config, returning empty DataFrame.")
            return df
        df_pl = df
    
    if len(extract_config) == 0:
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df_pl

    extract_mappings = extract_config.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data element mappings.")
        return df_pl

    # Ensure DX_UID is explicitly a String before processing
    # This is a defensive check - DX_UID should already be a String from format_for_exhaustivity_import
    if "DX_UID" in df_pl.columns:
        # If DX_UID is somehow a List, flatten it; otherwise just cast to String
        if df_pl["DX_UID"].dtype == pl.List:
            current_run.log_warning("DX_UID column is a List type, flattening to String (this should not happen)")
            df_pl = df_pl.with_columns([
                pl.col("DX_UID").list.get(0).cast(pl.Utf8).alias("DX_UID")
            ])
        else:
            df_pl = df_pl.with_columns([
                pl.col("DX_UID").cast(pl.Utf8).alias("DX_UID")
            ])

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(f"Applying data element mappings for extract: {extract_config.get('EXTRACT_UID')}.")
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided) using polars
        df_uid = df_pl.filter(pl.col("DX_UID") == uid)
        
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            coc_keys = list(coc_mappings_clean.keys())
            # Only filter and replace by COC if CATEGORY_OPTION_COMBO column exists
            if "CATEGORY_OPTION_COMBO" in df_uid.columns:
                df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                # Replace values using polars replace
                df_uid = df_uid.with_columns(
                    pl.col("CATEGORY_OPTION_COMBO").replace(coc_mappings_clean)
                )
        
        if aoc_mappings:
            aoc_mappings = {k: v for k, v in aoc_mappings.items() if v is not None}  # Do not replace with None
            aoc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()}
            aoc_keys = list(aoc_mappings_clean.keys())
            # Only filter and replace by AOC if ATTRIBUTE_OPTION_COMBO column exists
            if "ATTRIBUTE_OPTION_COMBO" in df_uid.columns:
                df_uid = df_uid.filter(pl.col("ATTRIBUTE_OPTION_COMBO").is_in(aoc_keys))
                # Replace values using polars replace
                df_uid = df_uid.with_columns(
                    pl.col("ATTRIBUTE_OPTION_COMBO").replace(aoc_mappings_clean)
                )

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logging.warning("No data elements matched the provided mappings, returning empty dataframe.")
        # Return empty polars DataFrame with same schema
        return pl.DataFrame(schema=df_pl.schema)

    # Concatenate using polars
    df_filtered_pl = pl.concat(chunks)

    # Apply UID mappings using polars replace
    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered_pl = df_filtered_pl.with_columns(
            pl.col("DX_UID").replace(uid_mappings_clean)
        )

    # Fill missing AOC (PRS default) using polars
    df_filtered_pl = df_filtered_pl.with_columns(
        pl.col("ATTRIBUTE_OPTION_COMBO").fill_null("HllvX50cXC0")
    )

    return df_filtered_pl


def apply_reporting_rates_extract_config():
    """Placeholder for reporting rates extract mapping.

    Raises
    ------
    NotImplementedError
        This function is not yet implemented.
    """
    raise NotImplementedError


def apply_indicators_extract_config():
    """Placeholder for indicators extract mapping.

    Raises
    ------
    NotImplementedError
        This function is not yet implemented.
    """
    raise NotImplementedError


def dhis2_request(session: requests.Session, method: str, url: str, **kwargs: any) -> dict:
    """Wrapper around requests to handle DHIS2 GET/PUT with error handling.

    Parameters
    ----------
    session : requests.Session
        Session object used to perform requests.
    method : str
        HTTP method: 'get' or 'put'.
    url : str
        Full URL for the request.
    **kwargs
        Additional arguments for session.request (json, params, etc.)

    Returns
    -------
    dict
        Either the response JSON or an error payload with 'error' and 'status_code'.
    """
    try:
        r = session.request(method, url, **kwargs)
        r.raise_for_status()
        return r.json()
    except HTTPError as e:
        try:
            return {
                "error": f"HTTP error during {method.upper()} {e} status_code: {r.status_code} response: {r.json()}"
            }
        except Exception:
            return {"error": f"HTTP error during {method.upper()} {e} status_code: {r.status_code}"}
    except RequestException as e:
        return {"error": f"Request error during {method.upper()} {url}: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error during {method.upper()} {url}: {e}"}


if __name__ == "__main__":
    dhis2_exhaustivity()

