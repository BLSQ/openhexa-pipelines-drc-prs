import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import requests
from config_sync import sync_configs_from_drug_mapping
from d2d_library.db_queue import Queue
from d2d_library.dhis2_extract_handlers import DHIS2Extractor
from d2d_library.dhis2_pusher import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from exhaustivity_calculation import compute_exhaustivity
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from openhexa.toolbox.dhis2.periods import period_from_string
from utils import (
    configure_logging,
    connect_to_dhis2,
    get_extract_config,
    load_drug_mapping,
    load_pipeline_config,
    save_to_parquet,
)

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SAN-123
#   -https://bluesquare.atlassian.net/browse/SAN-124
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs


def dhis2_request(
    session: requests.Session, method: str, url: str, **kwargs: Any
) -> dict:
    """Make a request to DHIS2 API and return JSON response.
    
    Parameters
    ----------
    session : requests.Session
        The session object to use for the request.
    method : str
        HTTP method (get, post, put, delete).
    url : str
        The URL to request.
    **kwargs : dict
        Additional arguments to pass to the request.
        
    Returns
    -------
    dict
        JSON response from the API, or error dict if request fails.
    """
    try:
        response = getattr(session, method.lower())(url, **kwargs)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"DHIS2 request failed: {e}")
        return {"error": str(e)}


# extract data from source DHIS2
@pipeline("dhis2_exhaustivity", timeout=3600)  # 1 hour
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
def dhis2_exhaustivity(
    run_ou_sync: bool,
    run_extract_data: bool,
    run_compute_data: bool,
    run_push_data: bool,
):
    """Extract data elements from the PRS DHIS2 instance.

    Compute the exhaustivity value based on required columns completeness.
    The results are then pushed back to PRS DHIS2 to the target exhaustivity data elements.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"

    # Validate drug_mapping files
    sync_configs_from_drug_mapping(pipeline_path)

    # Reset queue at pipeline start to clear any old items from previous runs
    # This MUST happen before any @task starts to avoid race conditions
    queue_dir = pipeline_path / "data"
    queue_dir.mkdir(parents=True, exist_ok=True)
    db_path = queue_dir / ".queue.db"
    push_queue = Queue(db_path)
    push_queue.reset()
    current_run.log_info("🧹 Queue reset at pipeline start")

    try:
        # 1) Extract data from source DHIS2
        extract_ready = extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
        )

        # 2) Sync dataset org units (runs in parallel with extract - no dependency)
        # Returns dict with 'success' and 'new_org_units' (new org units per extract)
        sync_result = update_dataset_org_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,
        )
        
        # Extract new org units from sync result
        # New org units will only be counted for current period onwards (not historical)
        new_org_units = {}
        if isinstance(sync_result, dict):
            new_org_units = sync_result.get("new_org_units", {})
            sync_ready = sync_result.get("success", True)
        else:
            sync_ready = sync_result  # Fallback for boolean return

        # 3) Compute exhaustivity from extracted data (must wait for extract!)
        # Pass new_org_units so they're only counted for current period
        compute_ready = compute_exhaustivity_data(
            pipeline_path=pipeline_path,
            run_task=run_compute_data,
            wait=extract_ready,
            new_org_units=new_org_units,
        )

        # 4) Push data to target DHIS2 (must wait for both compute AND sync)
        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=(compute_ready and sync_ready),
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_exhaustivity.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
) -> bool:
    """Extract data elements from the source DHIS2 instance.

    Saves them in parquet format.
    
    Returns
    -------
    bool
        True if extraction completed successfully, False otherwise.
    """
    if not run_task:
        current_run.log_info("Data elements extraction task skipped.")
        return True

    current_run.log_info("Data elements extraction task started.")
    configure_logging(logs_path=pipeline_path / "logs" / "extract", task_name="extract_data")

    # Load pipeline configuration
    config = load_pipeline_config(pipeline_path / "configuration")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=config["CONNECTIONS"]["SOURCE_DHIS2"], cache_dir=None
    )

    try:
        # Check if dynamic date is enabled
        dynamic_date = config["EXTRACTION"].get("DYNAMIC_DATE", True)  # Default to True for backward compatibility
        
        if dynamic_date:
            # Dynamic mode: use current month + MONTHS_WINDOW
            extraction_window = config["EXTRACTION"].get("MONTHS_WINDOW", 3)
            
            # ENDDATE: Use current month (datetime.now())
            end = datetime.now().strftime("%Y%m")
            config_enddate = config["EXTRACTION"].get("ENDDATE")
            if config_enddate:
                current_run.log_info(
                    f"ENDDATE in config ({config_enddate}) "
                    f"overridden by dynamic date: {end}"
                )
            
            # STARTDATE: Calculated from ENDDATE and MONTHS_WINDOW
            # Example: if MONTHS_WINDOW=3 and ENDDATE=202412, STARTDATE=202410 (3 months: Oct, Nov, Dec)
            end_date = datetime.strptime(end, "%Y%m")
            start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
            config_startdate = config["EXTRACTION"].get("STARTDATE")
            if config_startdate:
                current_run.log_info(
                    f"STARTDATE in config ({config_startdate}) "
                    f"overridden by dynamic calculation: {start}"
                )
            
            current_run.log_info(
                f"Using DYNAMIC_DATE mode: {start} to {end} ({extraction_window} months including current month)"
            )
        else:
            # Static mode: use STARTDATE and ENDDATE from config
            start = config["EXTRACTION"].get("STARTDATE")
            end = config["EXTRACTION"].get("ENDDATE")
            
            if not start or not end:
                raise ValueError("DYNAMIC_DATE is False but STARTDATE or ENDDATE is missing in config")
            
            current_run.log_info(f"Using STATIC_DATE mode: {start} to {end} (from config)")
        
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

    # For extraction, use STARTDATE to ENDDATE (same range for exhaustivity computation)
    start_extraction = start

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    download_settings = config["EXTRACTION"].get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

    # Setup extractor
    # See docs about return_existing_file impact.
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client, download_mode=download_settings, return_existing_file=False
    )
    current_run.log_info(
        f"Download MODE: {download_settings} - Extraction and Exhaustivity: {start_extraction} to {end} "
        f"({extraction_window} months including current month)"
    )

    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=config.get("EXTRACTS", []),
        extract_periods=get_periods(start_extraction, end),
        pipeline_config=config,
    )
    
    return True


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    extract_periods: list[str],
    pipeline_config: dict | None = None,
) -> None:
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    current_run.log_info("Starting data element extracts.")
    source_datasets = get_datasets(dhis2_extractor.dhis2_client)
    config_dir = pipeline_path / "configuration"

    # loop over the available extract configurations
    for idx, extract in enumerate(data_element_extracts):
        extract_id = extract.get("EXTRACT_ID")
        drug_mapping_file = extract.get("DRUG_MAPPING_FILE")
        dataset_uid = extract.get("SOURCE_DATASET_UID")  # Source dataset to get org units from

        if extract_id is None:
            current_run.log_warning(
                f"No 'EXTRACT_ID' defined for extract position: {idx}. This is required, extract skipped."
            )
            continue

        if dataset_uid is None:
            current_run.log_warning(f"No 'SOURCE_DATASET_UID' defined for extract: {extract_id}, extract skipped.")
            continue

        # Load UIDs from drug_mapping file
        if drug_mapping_file:
            _, data_element_uids = load_drug_mapping(config_dir, drug_mapping_file)
        else:
            data_element_uids = []
            current_run.log_warning(f"No DRUG_MAPPING_FILE for extract: {extract_id}")

        if len(data_element_uids) == 0:
            current_run.log_warning(f"No data elements defined for extract: {extract_id}, extract skipped.")
            continue

        # Get org units directly from SOURCE_DATASET_UID (source dataset)
        source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_uid]))
        if len(source_dataset) == 0:
            current_run.log_warning(f"Dataset {dataset_uid} not found for extract: {extract_id}, extract skipped.")
            continue
        
        org_units = source_dataset["organisation_units"].explode().to_list()
        dataset_name = source_dataset["name"][0] if len(source_dataset) > 0 else "Unknown"
        
        # Limit org units for testing if MAX_ORG_UNITS_FOR_TEST is set in config
        if pipeline_config and pipeline_config.get("EXTRACTION", {}).get("MAX_ORG_UNITS_FOR_TEST"):
            max_org_units = pipeline_config["EXTRACTION"]["MAX_ORG_UNITS_FOR_TEST"]
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

        # Create folder name based on extract_id
        folder_name = f"Extract {extract_id}"

        # Clean old extract files that are outside the requested periods
        extracts_dir = pipeline_path / "data" / "extracts" / folder_name
        if extracts_dir.exists():
            old_files = list(extracts_dir.glob("data_*.parquet"))
            files_to_delete = [f for f in old_files if f.stem.replace("data_", "") not in extract_periods]
            if files_to_delete:
                current_run.log_info(
                    f"🧹 Cleaning {len(files_to_delete)} old extract file(s) outside window for {extract_id}"
                )
                for old_file in files_to_delete:
                    try:
                        old_file.unlink()
                        logging.info(f"Deleted old extract file: {old_file.name}")
                    except Exception as e:
                        current_run.log_warning(f"Could not delete old extract file {old_file.name}: {e}")

        # run data elements extraction per period
        for period in extract_periods:
            try:
                output_file = dhis2_extractor.analytics_data_elements.download_period(
                    data_elements=data_element_uids,
                    org_units=org_units,
                    period=period,
                    output_dir=pipeline_path / "data" / "extracts" / folder_name,
                )
                if output_file is None:
                    current_run.log_warning(
                        f"Extract {extract_id} download returned None "
                        f"for period {period}. No file created."
                    )
                elif not output_file.exists():
                    current_run.log_warning(
                        f"Extract {extract_id} download returned path {output_file} "
                        f"but file does not exist for period {period}."
                    )
                else:
                    current_run.log_info(
                        f"Extract {extract_id} successfully created file "
                        f"for period {period}: {output_file}"
                    )

            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                # Log more details about the error
                current_run.log_warning(
                    f"Extract {extract_id} download failed for period {period} "
                    f"({error_type}): {error_msg[:200]}. Continuing with next period."
                )
                logging.error(f"Extract {extract_id} - period {period} error ({error_type}): {e!s}")
                # Log full traceback for debugging
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
    wait: bool = True,
    new_org_units: dict | None = None,
) -> bool:
    """Compute exhaustivity from extracted data and save the result to processed/ folder.
    
    Returns
    -------
    bool
        True if computation completed successfully, False otherwise.
    
    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    run_task : bool
        Whether to run the task or skip it.
    wait : bool
        Dependency flag - task waits for this to be True before running.
    new_org_units : dict, optional
        Dict mapping extract_id -> list of new org unit IDs.
        New org units will only be included in exhaustivity for the current period onwards.
    """
    if not run_task:
        current_run.log_info("Exhaustivity computation task skipped.")
        return True
    
    if new_org_units is None:
        new_org_units = {}

    current_run.log_info("Exhaustivity computation task started.")
    
    # Initialize queue early (before try block) so we can always enqueue FINISH
    # Use data folder (writable on OpenHexa) instead of configuration folder
    queue_dir = pipeline_path / "data"
    queue_dir.mkdir(parents=True, exist_ok=True)
    db_path = queue_dir / ".queue.db"
    push_queue = Queue(db_path)
    
    try:
        configure_logging(logs_path=pipeline_path / "logs" / "compute", task_name="compute_exhaustivity")
    except Exception as e:
        # If configure_logging fails (e.g., in test environment), continue anyway
        current_run.log_warning(f"Could not configure logging: {e!s}")

    # Load pipeline config
    config = load_pipeline_config(pipeline_path / "configuration")
    
    # Get date range (same logic as extract_data)
    dynamic_date = config["EXTRACTION"].get("DYNAMIC_DATE", True)  # Default to True for backward compatibility
    
    if dynamic_date:
        # Dynamic mode: use current month + MONTHS_WINDOW
        extraction_window = config["EXTRACTION"].get("MONTHS_WINDOW", 3)
        end = datetime.now().strftime("%Y%m")
        end_date = datetime.strptime(end, "%Y%m")
        start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    else:
        # Static mode: use STARTDATE and ENDDATE from config
        start = config["EXTRACTION"].get("STARTDATE")
        end = config["EXTRACTION"].get("ENDDATE")
        
        if not start or not end:
            raise ValueError("DYNAMIC_DATE is False but STARTDATE or ENDDATE is missing in config")
    
    # Note: Queue is reset at pipeline start (in main function) to avoid race conditions
    # Compute exhaustivity for all extracts
    exhaustivity_periods = get_periods(start, end)
    current_period = end  # Current period = most recent (e.g., 202512)
    
    current_run.log_info(
        f"Processing {len(config.get('EXTRACTS', []))} extracts "
        f"for {len(exhaustivity_periods)} periods"
    )
    if new_org_units:
        current_run.log_info(f"📍 New org units detected - will only count from period {current_period} onwards")
    
    for target_extract in config.get("EXTRACTS", []):
        extract_id = target_extract.get("EXTRACT_ID")
        current_run.log_info(f"Computing exhaustivity for extract: {extract_id}")
        
        # Get new org units for this specific extract
        extract_new_org_units = new_org_units.get(extract_id, [])
        if extract_new_org_units:
            current_run.log_info(
                f"📍 {len(extract_new_org_units)} new org units for '{extract_id}' "
                f"- excluded from periods before {current_period}"
            )
        
        try:
            compute_exhaustivity_and_queue(
                pipeline_path=pipeline_path,
                extract_id=extract_id,
                exhaustivity_periods=exhaustivity_periods,
                push_queue=push_queue,
                new_org_units=extract_new_org_units,
                current_period=current_period,
            )
            current_run.log_info(f"✅ Completed exhaustivity computation for extract: {extract_id}")
        except Exception as e:
            current_run.log_error(f"❌ Error computing exhaustivity for {extract_id}: {e!s}")
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback for {extract_id}:\n{traceback_str}")
            # Continue with next extract instead of crashing
            current_run.log_warning(f"⚠️  Skipping {extract_id} due to error, continuing with next extract")
    
    # Enqueue FINISH marker after all extracts are processed
    push_queue.enqueue("FINISH")
    current_run.log_info("✅ FINISH marker enqueued")
    
    return True


def compute_exhaustivity_and_queue(
    pipeline_path: Path,
    extract_id: str,
    exhaustivity_periods: list[str],
    push_queue: Queue,
    new_org_units: list[str] | None = None,
    current_period: str | None = None,
) -> bool:
    """Compute exhaustivity from extracted data and save the result to processed/ folder.
    
    Returns
    -------
    bool
        True if computation completed successfully, False otherwise.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    exhaustivity_periods : list[str]
        List of periods to process.
    push_queue : Queue
        Queue for pushing processed files.
    new_org_units : list[str], optional
        List of new org unit IDs that should only be counted from current_period onwards.
    current_period : str, optional
        The current period (e.g., '202512'). New org units are excluded from periods before this.
    """
    if new_org_units is None:
        new_org_units = []
    # Load pipeline config
    config = load_pipeline_config(pipeline_path / "configuration")
    config_dir = pipeline_path / "configuration"
    
    # Find the extract configuration
    extract_config_item = get_extract_config(config, extract_id)
    
    # Create folder name based on extract_id
    folder_name = f"Extract {extract_id}"
    
    extracts_folder = pipeline_path / "data" / "extracts" / folder_name
    output_dir = pipeline_path / "data" / "processed" / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    current_run.log_info(
        f"📁 Processing extract {extract_id}: "
        f"extracts folder: {extracts_folder.relative_to(pipeline_path)}, "
        f"output folder: {output_dir.relative_to(pipeline_path)}"
    )
    
    # Clean old processed files for this extract before starting
    if output_dir.exists():
        old_files = list(output_dir.glob("*.parquet"))
        if old_files:
            current_run.log_info(f"🧹 Cleaning {len(old_files)} old processed file(s) for extract {extract_id}")
            for old_file in old_files:
                try:
                    old_file.unlink()
                    logging.info(f"Deleted old processed file: {old_file.name}")
                except Exception as e:
                    current_run.log_warning(f"Could not delete old file {old_file.name}: {e}")
                    logging.warning(f"Could not delete old file {old_file.name}: {e}")
    
    # Get expected DX_UIDs from drug_mapping and ORG_UNITs from source dataset
    expected_dx_uids = None
    expected_org_units = None
    
    if extract_config_item:
        # Load UIDs from drug_mapping file
        drug_mapping_file = extract_config_item.get("DRUG_MAPPING_FILE")
        if drug_mapping_file:
            _, expected_dx_uids = load_drug_mapping(config_dir, drug_mapping_file)
        
        # Get org units from SOURCE_DATASET_UID (REQUIRED - no fallback)
        source_dataset_uid = extract_config_item.get("SOURCE_DATASET_UID")
        if not source_dataset_uid:
            raise ValueError(
                f"No SOURCE_DATASET_UID defined for extract: {extract_id}"
            )
        
        try:
            # Use SOURCE_DHIS2 to retrieve org units from SOURCE_DATASET_UID
            dhis2_client = connect_to_dhis2(
                connection_str=config["CONNECTIONS"]["SOURCE_DHIS2"], cache_dir=None
            )
            datasets = get_datasets(dhis2_client)
            source_dataset = datasets.filter(pl.col("id").is_in([source_dataset_uid]))
            if len(source_dataset) == 0:
                raise ValueError(f"Source dataset {source_dataset_uid} not found in DHIS2")
            
            expected_org_units = source_dataset["organisation_units"].explode().to_list()
            current_run.log_info(
                f"Retrieved {len(expected_org_units)} org units "
                f"from SOURCE_DATASET_UID {source_dataset_uid}"
            )
        except Exception as e:
            error_msg = (
                f"Failed to retrieve org units from SOURCE_DATASET_UID "
                f"{source_dataset_uid} for extract {extract_id}: {e!s}"
            )
            current_run.log_error(error_msg)
            logging.error(error_msg)
            raise RuntimeError(error_msg) from e
    exhaustivity_df = compute_exhaustivity(
        pipeline_path=pipeline_path,
        extract_id=extract_id,
        periods=exhaustivity_periods,
        expected_dx_uids=expected_dx_uids,
        expected_org_units=expected_org_units,  # Pass all org units from dataset for zero-filling
        extract_config_item=extract_config_item,
        extracts_folder=extracts_folder,
        new_org_units=new_org_units,  # New org units only counted from current_period
        current_period=current_period,
    )

    # Save exhaustivity data per period
    # Get all periods from the result (may include periods not in exhaustivity_periods if data exists)
    if len(exhaustivity_df) > 0 and "PERIOD" in exhaustivity_df.columns:
        periods_in_result = sorted(exhaustivity_df["PERIOD"].unique().to_list())
    else:
        periods_in_result = []
    
    # Use periods from result if available, otherwise use requested periods
    # Also include all requested periods to ensure we create files even if they have no data (exhaustivity=0)
    periods_to_save = sorted(set(periods_in_result + exhaustivity_periods))

    for period in periods_to_save:
        # Filter for the current period
        period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
        
        if len(period_exhaustivity) == 0:
            # If no data for this period, skip it (don't create rows for org units without data)
            current_run.log_info(
                f"Period {period}: no data, skipping "
                f"(no org units will be pushed for this period)"
            )
            continue

        # Save only the aggregated view per (PERIOD, ORG_UNIT): value = 1 only if all COCs for that org/period are 1
        period_exhaustivity_org_level = (
            period_exhaustivity
            .group_by(["PERIOD", "ORG_UNIT"])
            .agg([pl.col("EXHAUSTIVITY_VALUE").min().alias("EXHAUSTIVITY_VALUE")])
            .select([pl.col("PERIOD"), pl.col("ORG_UNIT"), pl.col("EXHAUSTIVITY_VALUE")])
            .drop("CATEGORY_OPTION_COMBO", strict=False)
        )
        
        # Calculate statistics for logging
        num_org_units = len(period_exhaustivity_org_level)
        num_values_1 = int(period_exhaustivity_org_level["EXHAUSTIVITY_VALUE"].sum())
        percentage_1 = (num_values_1 / num_org_units * 100) if num_org_units > 0 else 0.0
        
        # Calculate COC-level statistics (before aggregation to ORG_UNIT level)
        # Aggregate by (PERIOD, COC, ORG_UNIT): exhaustivity = 1 only if all DX_UIDs for that COC are 1
        if "CATEGORY_OPTION_COMBO" in period_exhaustivity.columns:
            period_exhaustivity_coc_level = (
                period_exhaustivity
                .group_by(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"])
                .agg([pl.col("EXHAUSTIVITY_VALUE").min().alias("EXHAUSTIVITY_VALUE")])
            )
            coc_count_1_total = int(period_exhaustivity_coc_level.filter(pl.col("EXHAUSTIVITY_VALUE") == 1).height)
            coc_count_0_total = int(period_exhaustivity_coc_level.filter(pl.col("EXHAUSTIVITY_VALUE") == 0).height)
            coc_total = len(period_exhaustivity_coc_level)
            coc_percentage = (coc_count_1_total / coc_total * 100) if coc_total > 0 else 0.0
        else:
            coc_count_1_total = 0
            coc_count_0_total = 0
            coc_total = 0
            coc_percentage = 0.0

        try:
            # Write aggregated file in the main processed folder (replaces old COC-level files)
            aggregated_file = output_dir / f"exhaustivity_{period}.parquet"
            save_to_parquet(
                data=period_exhaustivity_org_level,
                filename=aggregated_file,
            )
            
            # Enqueue aggregated file for pushing
            push_queue.enqueue(f"{extract_id}|{aggregated_file}")
            
            # Clean log with only essential info
            current_run.log_info(
                f"📊 Period {period}: {num_org_units} ORG_UNITs, {percentage_1:.1f}% exhaustivity | "
                f"COCs: {coc_count_1_total} complètes (1), "
                f"{coc_count_0_total} incomplètes (0) = "
                f"{coc_percentage:.1f}% complètes"
            )
        except Exception as e:
            current_run.log_error(f"❌ Error saving exhaustivity data for {extract_id} - period {period}: {e!s}")
            logging.error(f"Exhaustivity saving error: {e!s}")
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            raise
    
    # After all periods are saved for this extract, enqueue a marker to indicate extract is complete
    # This allows push_data to collect all files for an extract before concatenating and pushing
    push_queue.enqueue(f"EXTRACT_COMPLETE|{extract_id}")
    current_run.log_info(f"✅ All periods saved for extract {extract_id}, extract complete marker enqueued")
    
    current_run.log_info("Exhaustivity computation finished.")
    return True


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

        # Load pipeline configuration
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")
        config = load_pipeline_config(pipeline_path / "configuration")
        target_conn = config["CONNECTIONS"]["TARGET_DHIS2"]
        dry_run = config["PUSH_SETTINGS"].get("DRY_RUN", True)

        # Sync org units for each extract
        for extract in config.get("EXTRACTS", []):
            source_dataset_uid = extract.get("SOURCE_DATASET_UID")
            target_dataset_uid = extract.get("TARGET_DATASET_UID")
            
            if source_dataset_uid and target_dataset_uid:
                push_dataset_org_units(
                    dhis2_client=connect_to_dhis2(connection_str=target_conn, cache_dir=None),
                    source_dataset_id=source_dataset_uid,
                    target_dataset_id=target_dataset_uid,
                    dry_run=dry_run,
                )

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
) -> bool:
    """Push data elements to the target DHIS2 instance.
    
    Returns
    -------
    bool
        True if push completed successfully, False otherwise.
    """
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return True

    current_run.log_info("Starting data push.")

    # setup
    configure_logging(logs_path=pipeline_path / "logs" / "push", task_name="push_data")
    # load pipeline configuration
    config = load_pipeline_config(pipeline_path / "configuration")
    dhis2_client = connect_to_dhis2(connection_str=config["CONNECTIONS"]["TARGET_DHIS2"], cache_dir=None)

    # Initialize queue for producer-consumer pattern
    # Use data folder (writable on OpenHexa) instead of configuration folder
    queue_dir = pipeline_path / "data"
    queue_dir.mkdir(parents=True, exist_ok=True)
    db_path = queue_dir / ".queue.db"
    push_queue = Queue(db_path)

    # Push parameters from PUSH_SETTINGS
    push_settings = config.get("PUSH_SETTINGS", {})
    import_strategy = push_settings.get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = push_settings.get("DRY_RUN", True)
    max_post = push_settings.get("MAX_POST", 2000)
    push_wait = push_settings.get("PUSH_WAIT_MINUTES", 5)

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

    # Dictionary to collect files per extract (maintains current output behavior: concatenate before push)
    extract_files_collected: dict[str, list[Path]] = {}

    # Loop over the queue (producer-consumer pattern)
    # This ensures we wait for files to be created and process them as they become available
    # We collect all files for an extract before concatenating and pushing (maintaining current output behavior)
    while True:
        next_item = push_queue.peek()
        queue_count = push_queue.count()
        
        if next_item == "FINISH":
            push_queue.dequeue()  # remove marker if present
            current_run.log_info("✅ FINISH marker received, exiting push loop")
            break

        if not next_item:
            current_run.log_info(
                f"Push data process: waiting for exhaustivity files to be computed... "
                f"(queue empty, {queue_count} items)"
            )
            # Queue SQLite handles synchronization between processes
            # Check periodically for new items
            time.sleep(60 * int(push_wait))
            continue
        
        # Log queue status only when queue count changes significantly or when processing extract complete markers
        # Skip verbose logging for individual file items

        try:
            # Check if this is an extract complete marker
            if next_item.startswith("EXTRACT_COMPLETE|"):
                # Extract complete marker - we should have collected all files for this extract
                _, extract_id = split_on_pipe(next_item)
                push_queue.dequeue()  # remove marker
                # If we have files collected for this extract, process them now
                if extract_id in extract_files_collected and len(extract_files_collected[extract_id]) > 0:
                    current_run.log_info(
                        f"📦 Extract {extract_id} complete, processing "
                        f"{len(extract_files_collected[extract_id])} file(s)"
                    )
                    # Process all files for this extract (concatenate and push)
                    process_extract_files(
                        extract_id=extract_id,
                        file_paths=extract_files_collected[extract_id],
                        pipeline_config=config,
                        pusher=pusher,
                        dhis2_client=dhis2_client,
                    )
                    # Clear collected files for this extract
                    del extract_files_collected[extract_id]
                continue
            
            # Read extract from queue
            extract_id, extract_file_path = split_on_pipe(next_item)
            extract_path = Path(extract_file_path)
            
            if not extract_path.exists():
                current_run.log_warning(f"⚠️  File from queue does not exist: {extract_path}, dequeuing and continuing")
                push_queue.dequeue()
                continue
            
            # Collect file for this extract (we'll process all files together when we see EXTRACT_COMPLETE marker)
            if extract_id not in extract_files_collected:
                extract_files_collected[extract_id] = []
            extract_files_collected[extract_id].append(extract_path)
            push_queue.dequeue()  # Remove from queue since we've collected it
            continue  # Continue to next queue item (wait for EXTRACT_COMPLETE marker)

        except Exception as e:
            error_msg = f"❌ Error processing queue item {next_item}: {e!s}"
            current_run.log_error(error_msg)
            logging.error(error_msg)
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            push_queue.dequeue()  # remove problematic item
            continue
    
    current_run.log_info("✅ Data push task finished.")
    return True


def process_extract_files(
    extract_id: str,
    file_paths: list[Path],
    pipeline_config: dict,
    pusher: DHIS2Pusher,  # type: ignore[type-arg]
    dhis2_client: DHIS2,
) -> None:
    """Process all files for an extract by concatenating them before pushing.
    
    Parameters
    ----------
    extract_id : str
        Identifier for the extract.
    file_paths : list[Path]
        List of file paths to process for this extract.
    pipeline_config : dict
        Pipeline configuration with EXTRACTS list.
    pusher : DHIS2Pusher
        DHIS2 pusher instance.
    dhis2_client : DHIS2
        DHIS2 client instance.
    """
    # Get extract config from pipeline_config
    extract_config = get_extract_config(pipeline_config, extract_id)
    if not extract_config:
        current_run.log_error(f"❌ Extract {extract_id} not found in pipeline_config")
        return
    
    target_data_element_uid = extract_config.get("TARGET_DATA_ELEMENT_UID")
    target_dataset_uid = extract_config.get("TARGET_DATASET_UID")
    
    if not target_data_element_uid:
        current_run.log_error(f"❌ No TARGET_DATA_ELEMENT_UID configured for {extract_id}")
        return
    
    # Process all files and collect DataFrames
    all_dataframes = []
    periods_processed = []
    
    for extract_path in file_paths:
        try:
            # Read parquet file
            df = pl.read_parquet(extract_path)
            
            # Extract period from DataFrame
            period = None
            if len(df) > 0 and "PERIOD" in df.columns:
                period = df["PERIOD"].unique().to_list()[0]
                periods_processed.append(period)
            
            # Validate file format (should have EXHAUSTIVITY_VALUE)
            if "EXHAUSTIVITY_VALUE" not in df.columns:
                current_run.log_warning(f"⚠️  File {extract_path.name} missing EXHAUSTIVITY_VALUE, skipping")
                continue
            
            # Map to DHIS2 format (COC/AOC not needed - DHIS2 uses defaults)
            df_mapped = df.with_columns([
                pl.lit(target_data_element_uid).alias("DX_UID"),
                pl.col("EXHAUSTIVITY_VALUE").cast(pl.Utf8).alias("VALUE"),
            ]).select([
                "DX_UID", "PERIOD", "ORG_UNIT", "VALUE"
            ])
            
            # Log statistics
            num_rows = len(df_mapped)
            num_values_1 = int(df_mapped.filter(pl.col("VALUE") == "1").height)
            pct = (num_values_1 / num_rows * 100) if num_rows > 0 else 0.0
            current_run.log_info(f"   ✓ Period {period}: {num_rows} ORG_UNITs, {pct:.1f}% exhaustivity")
            
            all_dataframes.append(df_mapped)

        except Exception as e:
            current_run.log_error(f"Error processing {extract_path.name}: {e!s}")
            logging.error(f"Error processing {extract_path.name}: {e!s}\n{traceback.format_exc()}")
            continue
    
    # Concatenate and push
    if not all_dataframes:
        current_run.log_warning(f"⚠️  No valid data to push for {extract_id}")
        return
    
    try:
        periods_str = sorted(set(periods_processed)) if periods_processed else "unknown"
        df_combined = pl.concat(all_dataframes, how="vertical_relaxed").sort("ORG_UNIT")
        
        # Filter by TARGET_DATASET_UID org units
        if target_dataset_uid and target_dataset_uid != "unknown":
            df_combined = filter_by_dataset_org_units(dhis2_client, df_combined, target_dataset_uid)
            if df_combined.is_empty():
                current_run.log_warning(f"⚠️  No data after filtering by {target_dataset_uid} for {extract_id}")
                return
        
        # Filter out invalid rows
        rows_before = len(df_combined)
        df_filtered = df_combined.filter(
            pl.col("DX_UID").is_not_null() &
            pl.col("PERIOD").is_not_null() &
            pl.col("ORG_UNIT").is_not_null() &
            pl.col("VALUE").is_not_null()
        )
        rows_after = len(df_filtered)
        
        if rows_before != rows_after:
            current_run.log_warning(f"⚠️  Filtered {rows_before - rows_after} invalid rows")
        
        # Statistics
        num_org_units = len(df_filtered["ORG_UNIT"].unique())
        num_values_1 = int(df_filtered.filter(pl.col("VALUE") == "1").height)
        pct = (num_values_1 / rows_after * 100) if rows_after > 0 else 0.0
        
        # Push
        current_run.log_info(
            f"🚀 Pushing {rows_after:,} rows for {extract_id} "
            f"(periods: {periods_str}) -> {target_dataset_uid}, "
            f"{num_org_units} ORG_UNITs, {pct:.1f}% exhaustivity"
        )
        
        pusher.push_data(df_data=df_filtered)
        current_run.log_info(f"✅ Push completed for {extract_id}")
        
    except Exception as e:
        current_run.log_error(f"❌ Error pushing {extract_id}: {e!s}")
        logging.error(f"Push error for {extract_id}: {e!s}\n{traceback.format_exc()}")


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


def filter_by_dataset_org_units(
    dhis2_client: DHIS2, data: pl.DataFrame, dataset_id: str
) -> pl.DataFrame:
    """Filter data to include only rows with org units in the specified dataset.
    
    Returns
    -------
    pl.DataFrame
        Filtered DataFrame containing only rows with ORG_UNITs present in the dataset.
    """
    url = f"{dhis2_client.api.url}/dataSets/{dataset_id}"
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except requests.exceptions.RequestException as e:
        current_run.log_warning(
            f"Network/HTTP error during payload fetch for dataset {dataset_id} "
            f"alignment: {e!s}. Returning original data."
        )
        return data
    except Exception as e:
        current_run.log_warning(
            f"Unexpected error during payload fetch for dataset {dataset_id} "
            f"alignment: {e!s}. Returning original data."
        )
        return data

    ds_uids = [ou["id"] for ou in dataset_payload.get("organisationUnits", [])]
    
    if not ds_uids:
        current_run.log_warning(f"No organisation units found for dataset {dataset_id}. Returning original data.")
        return data

    data_filtered = data.filter(pl.col("ORG_UNIT").is_in(ds_uids))
    current_run.log_info(
        f"Extract filtered by dataset {dataset_id} OUs ({len(ds_uids)} total), "
        f"rows removed {len(data) - len(data_filtered)}"
    )
    return data_filtered


if __name__ == "__main__":
    dhis2_exhaustivity()

