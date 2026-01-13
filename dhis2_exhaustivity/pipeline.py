import logging
import time
import traceback
from datetime import datetime
from pathlib import Path

import polars as pl
from d2d_library.db_queue import Queue
from d2d_library.dhis2_dataset_utils import dhis2_request, push_dataset_org_units
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
    validate_drug_mapping_files,
)

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SAN-123
#   -https://bluesquare.atlassian.net/browse/SAN-124
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs


# extract data from source DHIS2
@pipeline("dhis2_exhaustivity", timeout=3600)  # 1 hour
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2. If True, exhaustivity will be computed automatically.",
)
# push data to target DHIS2
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=False,
    help=(
        "Push data to target DHIS2. If True, exhaustivity will be computed first, "
        "then data will be pushed. Set to True only for production runs."
    ),
)
def dhis2_exhaustivity(
    run_extract_data: bool,
    run_push_data: bool,
):
    """Extract data elements from the PRS DHIS2 instance.

    Compute the exhaustivity value based on required columns completeness.
    The results are then pushed back to PRS DHIS2 to the target exhaustivity data elements.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"

    # Validate drug_mapping files (raises errors if validation fails)
    validate_drug_mapping_files(pipeline_path)

    # Initialize queue (do NOT reset - allows recovery of items from failed previous runs)
    # Items from a failed run will be processed in the next run
    # This MUST happen before any @task starts to avoid race conditions
    queue_dir = pipeline_path / "data"
    queue_dir.mkdir(parents=True, exist_ok=True)
    db_path = queue_dir / ".queue.db"
    push_queue = Queue(db_path)
    
    # Check if there are leftover items from a previous run
    leftover_count = push_queue.count()
    if leftover_count > 0:
        current_run.log_warning(
            f"⚠️  Found {leftover_count} item(s) in queue from previous run. "
            f"These will be processed (recovery from failed run)."
        )
    else:
        current_run.log_info("✅ Queue is empty (fresh start)")

    try:
        # 1) Sync dataset org units (mandatory - always runs)
        # Organisation units in target dataset must always be in sync with source dataset
        sync_ready = update_dataset_org_units(
            pipeline_path=pipeline_path,
        )

        # 2) Extract data from source DHIS2 (runs in parallel with sync - no dependency)
        extract_ready = extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
        )

        # 3) Compute exhaustivity from extracted data (automatic if extract or push is enabled)
        # If extract is enabled → compute after extraction
        # If push is enabled → compute before pushing (even if extract is disabled, in case of recovery)
        should_compute = run_extract_data or run_push_data
        compute_ready = compute_exhaustivity_data(
            pipeline_path=pipeline_path,
            run_task=should_compute,
            wait=extract_ready if run_extract_data else True,
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

    # Load pipeline configuration (extract_config.json + push_settings.json)
    config = load_pipeline_config(pipeline_path / "configuration")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=config["CONNECTIONS"]["SOURCE_DHIS2"], cache_dir=None
    )

    try:
        # Calculate periods: if STARTDATE/ENDDATE are set, use them; otherwise compute using default window
        # Same period calculation logic as other pipelines
        months_lag = config["EXTRACTION"].get("MONTHS_WINDOW", 3)  # default 3 months window
        if not config["EXTRACTION"].get("STARTDATE"):
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = config["EXTRACTION"].get("STARTDATE")
        if not config["EXTRACTION"].get("ENDDATE"):
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month
        else:
            end = config["EXTRACTION"].get("ENDDATE")
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

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
        f"Download MODE: {download_settings} - Extraction and Exhaustivity: {start} to {end}"
    )

    # Use DATA_ELEMENTS.EXTRACTS structure for consistency
    data_element_extracts = config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    # Fallback to EXTRACTS for backward compatibility
    if not data_element_extracts:
        data_element_extracts = config.get("EXTRACTS", [])
    
    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=data_element_extracts,
        extract_periods=get_periods(start, end),
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

    # loop over the available extract configurations
    for idx, extract in enumerate(data_element_extracts):
        # Support both EXTRACT_UID (new) and EXTRACT_ID (legacy) for backward compatibility
        extract_id = extract.get("EXTRACT_UID") or extract.get("EXTRACT_ID")
        data_element_uids = extract.get("UIDS", [])  # Get UIDs directly from config (no drug_mapping parsing here)
        dataset_uid = extract.get("SOURCE_DATASET_UID")  # Source dataset to get org units from

        if extract_id is None:
            current_run.log_warning(
                f"No 'EXTRACT_UID' or 'EXTRACT_ID' defined for extract position: {idx}. "
                f"This is required, extract skipped."
            )
            continue

        if dataset_uid is None:
            current_run.log_warning(f"No 'SOURCE_DATASET_UID' defined for extract: {extract_id}, extract skipped.")
            continue

        if len(data_element_uids) == 0:
            current_run.log_warning(f"No data elements (UIDS) defined for extract: {extract_id}, extract skipped.")
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
    """
    if not run_task:
        current_run.log_info("Exhaustivity computation task skipped.")
        return True
    
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

    # Load pipeline configuration (extract_config.json + push_settings.json)
    config = load_pipeline_config(pipeline_path / "configuration")
    
    # Get date range (same logic as extract_data - no DYNAMIC_DATE parameter)
    # Calculate periods: if STARTDATE/ENDDATE are set, use them; otherwise compute using default window
    months_lag = config["EXTRACTION"].get("MONTHS_WINDOW", 3)  # default 3 months window
    if not config["EXTRACTION"].get("STARTDATE"):
        start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
    else:
        start = config["EXTRACTION"].get("STARTDATE")
    if not config["EXTRACTION"].get("ENDDATE"):
        end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month
    else:
        end = config["EXTRACTION"].get("ENDDATE")
    
    # Note: Queue is reset at pipeline start (in main function) to avoid race conditions
    # Compute exhaustivity for all extracts
    exhaustivity_periods = get_periods(start, end)
    
    # Use DATA_ELEMENTS.EXTRACTS structure for consistency
    extracts_list = config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    # Fallback to EXTRACTS for backward compatibility
    if not extracts_list:
        extracts_list = config.get("EXTRACTS", [])
    
    current_run.log_info(
        f"Processing {len(extracts_list)} extracts "
        f"for {len(exhaustivity_periods)} periods"
    )
    
    for target_extract in extracts_list:
        # Support both EXTRACT_UID (new) and EXTRACT_ID (legacy) for backward compatibility
        extract_id = target_extract.get("EXTRACT_UID") or target_extract.get("EXTRACT_ID")
        if not extract_id:
            current_run.log_warning("Extract missing EXTRACT_UID/EXTRACT_ID, skipping")
            continue
        current_run.log_info(f"Computing exhaustivity for extract: {extract_id}")
        
        try:
            compute_exhaustivity_and_queue(
                pipeline_path=pipeline_path,
                extract_id=extract_id,
                exhaustivity_periods=exhaustivity_periods,
                push_queue=push_queue,
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


def aggregate_to_org_unit_level(period_exhaustivity: pl.DataFrame) -> pl.DataFrame:
    """Aggregate exhaustivity data to ORG_UNIT level.
    
    Value = 1 only if all COCs for that org/period are 1 (min aggregation).
    
    Parameters
    ----------
    period_exhaustivity : pl.DataFrame
        DataFrame with exhaustivity data at COC level (columns: PERIOD, ORG_UNIT, 
        CATEGORY_OPTION_COMBO, EXHAUSTIVITY_VALUE).
    
    Returns
    -------
    pl.DataFrame
        Aggregated DataFrame at ORG_UNIT level (columns: PERIOD, ORG_UNIT, EXHAUSTIVITY_VALUE).
    """
    return (
        period_exhaustivity
        .group_by(["PERIOD", "ORG_UNIT"])
        .agg([pl.col("EXHAUSTIVITY_VALUE").min().alias("EXHAUSTIVITY_VALUE")])
        .select([pl.col("PERIOD"), pl.col("ORG_UNIT"), pl.col("EXHAUSTIVITY_VALUE")])
        .drop("CATEGORY_OPTION_COMBO", strict=False)
    )


def calculate_org_unit_statistics(period_exhaustivity_org_level: pl.DataFrame) -> dict:
    """Calculate statistics for ORG_UNIT level exhaustivity.
    
    Parameters
    ----------
    period_exhaustivity_org_level : pl.DataFrame
        DataFrame with exhaustivity data at ORG_UNIT level.
    
    Returns
    -------
    dict
        Dictionary with keys: num_org_units, num_values_1, percentage_1.
    """
    num_org_units = len(period_exhaustivity_org_level)
    num_values_1 = int(period_exhaustivity_org_level["EXHAUSTIVITY_VALUE"].sum())
    percentage_1 = (num_values_1 / num_org_units * 100) if num_org_units > 0 else 0.0
    
    return {
        "num_org_units": num_org_units,
        "num_values_1": num_values_1,
        "percentage_1": percentage_1,
    }


def calculate_coc_statistics(period_exhaustivity: pl.DataFrame) -> dict:
    """Calculate COC-level statistics for exhaustivity.
    
    Aggregate by (PERIOD, COC, ORG_UNIT): exhaustivity = 1 only if all DX_UIDs for that COC are 1.
    
    Parameters
    ----------
    period_exhaustivity : pl.DataFrame
        DataFrame with exhaustivity data at COC level.
    
    Returns
    -------
    dict
        Dictionary with keys: coc_count_1_total, coc_count_0_total, coc_total, coc_percentage.
    """
    if "CATEGORY_OPTION_COMBO" not in period_exhaustivity.columns:
        return {
            "coc_count_1_total": 0,
            "coc_count_0_total": 0,
            "coc_total": 0,
            "coc_percentage": 0.0,
        }
    
    # Aggregate by (PERIOD, COC, ORG_UNIT): exhaustivity = 1 only if all DX_UIDs for that COC are 1
    period_exhaustivity_coc_level = (
        period_exhaustivity
        .group_by(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT"])
        .agg([pl.col("EXHAUSTIVITY_VALUE").min().alias("EXHAUSTIVITY_VALUE")])
    )
    
    coc_count_1_total = int(period_exhaustivity_coc_level.filter(pl.col("EXHAUSTIVITY_VALUE") == 1).height)
    coc_count_0_total = int(period_exhaustivity_coc_level.filter(pl.col("EXHAUSTIVITY_VALUE") == 0).height)
    coc_total = len(period_exhaustivity_coc_level)
    coc_percentage = (coc_count_1_total / coc_total * 100) if coc_total > 0 else 0.0
    
    return {
        "coc_count_1_total": coc_count_1_total,
        "coc_count_0_total": coc_count_0_total,
        "coc_total": coc_total,
        "coc_percentage": coc_percentage,
    }


def log_period_statistics(period: str, org_unit_stats: dict, coc_stats: dict) -> None:
    """Log statistics for a period.
    
    Parameters
    ----------
    period : str
        Period identifier (e.g., '202501').
    org_unit_stats : dict
        Statistics dictionary from calculate_org_unit_statistics.
    coc_stats : dict
        Statistics dictionary from calculate_coc_statistics.
    """
    current_run.log_info(
        f"📊 Period {period}: {org_unit_stats['num_org_units']} ORG_UNITs, "
        f"{org_unit_stats['percentage_1']:.1f}% exhaustivity | "
        f"COCs: {coc_stats['coc_count_1_total']} complètes (1), "
        f"{coc_stats['coc_count_0_total']} incomplètes (0) = "
        f"{coc_stats['coc_percentage']:.1f}% complètes"
    )


def compute_exhaustivity_and_queue(
    pipeline_path: Path,
    extract_id: str,
    exhaustivity_periods: list[str],
    push_queue: Queue,
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
    """
    # Load pipeline configuration (extract_config.json + push_settings.json)
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
    # Process each period individually (simplified: one period = one file = one computation)
    for period in exhaustivity_periods:
        # Check if extract file exists for this period
        extract_file = extracts_folder / f"data_{period}.parquet"
        if not extract_file.exists():
            current_run.log_info(
                f"Period {period}: no extract file found, skipping "
                f"(no org units will be pushed for this period)"
            )
            continue

        # Compute exhaustivity for this single period only
        period_exhaustivity_df = compute_exhaustivity(
            pipeline_path=pipeline_path,
            extract_id=extract_id,
            periods=[period],  # Single period only
            expected_dx_uids=expected_dx_uids,
            expected_org_units=expected_org_units,  # Pass all org units from dataset for zero-filling
            extract_config_item=extract_config_item,
            extracts_folder=extracts_folder,
        )
        
        if len(period_exhaustivity_df) == 0:
            # If no data for this period, skip it (don't create rows for org units without data)
            current_run.log_info(
                f"Period {period}: no data after computation, skipping "
                f"(no org units will be pushed for this period)"
            )
            continue

        # Aggregate to ORG_UNIT level: value = 1 only if all COCs for that org/period are 1
        period_exhaustivity_org_level = aggregate_to_org_unit_level(period_exhaustivity_df)
        
        # Calculate statistics for logging
        org_unit_stats = calculate_org_unit_statistics(period_exhaustivity_org_level)
        coc_stats = calculate_coc_statistics(period_exhaustivity_df)

        try:
            # Write aggregated file in the main processed folder
            aggregated_file = output_dir / f"exhaustivity_{period}.parquet"
            save_to_parquet(
                data=period_exhaustivity_org_level,
                filename=aggregated_file,
            )
            
            # Enqueue aggregated file for pushing (immediately, per period)
            push_queue.enqueue(f"{extract_id}|{aggregated_file}")
            
            # Clean log with only essential info
            log_period_statistics(period, org_unit_stats, coc_stats)
        except Exception as e:
            current_run.log_error(f"❌ Error saving exhaustivity data for {extract_id} - period {period}: {e!s}")
            logging.error(f"Exhaustivity saving error: {e!s}")
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            raise
    
    # No need for EXTRACT_COMPLETE marker - each period is pushed individually as it becomes available
    
    current_run.log_info("Exhaustivity computation finished.")
    return True


@dhis2_exhaustivity.task
def update_dataset_org_units(
    pipeline_path: Path,
) -> bool:
    """Updates the organisation units of datasets in the PRS DHIS2 instance.
    
    This is a mandatory step - organisation units in target dataset must always
    be in sync with source dataset.

    Returns
    -------
    bool
        True if update was performed or already in sync, False otherwise.
    """
    try:
        current_run.log_info("Starting update of dataset organisation units.")

        # Load pipeline configuration
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")
        config = load_pipeline_config(pipeline_path / "configuration")
        target_conn = config["CONNECTIONS"]["TARGET_DHIS2"]
        dry_run = config["PUSH_SETTINGS"].get("DRY_RUN", True)

        # Sync org units for each extract
        # Use DATA_ELEMENTS.EXTRACTS structure for consistency
        extracts_list = config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
        # Fallback to EXTRACTS for backward compatibility
        if not extracts_list:
            extracts_list = config.get("EXTRACTS", [])
        
        for extract in extracts_list:
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

    # Setup logging
    configure_logging(logs_path=pipeline_path / "logs" / "push", task_name="push_data")
    
    # Load pipeline configuration (extract_config.json + push_settings.json)
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

    # Loop over the queue (producer-consumer pattern)
    # Process each period individually as it becomes available: extract -> compute -> push (per period)
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

        try:
            # Read period file from queue
            extract_id, extract_file_path = split_on_pipe(next_item)
            
            # Validate queue item format
            if extract_id is None or extract_file_path is None:
                current_run.log_error(f"Invalid queue item format: {next_item}. Expected 'extract_id|file_path'")
                push_queue.dequeue()
                continue
            
            extract_path = Path(extract_file_path)
            
            if not extract_path.exists():
                current_run.log_warning(f"⚠️  File from queue does not exist: {extract_path}, dequeuing and continuing")
                push_queue.dequeue()
                continue
            
        except Exception as e:
            current_run.log_error(f"Failed to read extract from queue item: {next_item}. Error: {e}")
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            # Process this period file immediately (no collection, no waiting)
            process_period_file(
                extract_id=extract_id,
                file_path=extract_path,
                pipeline_config=config,
                pusher=pusher,
                dhis2_client=dhis2_client,
            )
            
            # Success → dequeue (only after successful processing)
            push_queue.dequeue()
            
        except Exception as e:
            current_run.log_error(f"Fatal error for extract {extract_id} ({extract_path}), stopping push process.")
            logging.error(f"Fatal error for extract {extract_id} ({extract_path}): {e!s}")
            raise  # crash on error
    
    current_run.log_info("✅ Data push task finished.")
    return True


def process_period_file(
    extract_id: str,
    file_path: Path,
    pipeline_config: dict,
    pusher: DHIS2Pusher,  # type: ignore[type-arg]
    dhis2_client: DHIS2,
) -> None:
    """Process a single period file and push it immediately.
    
    This function processes one period at a time: extract -> compute -> push (per period).
    
    Parameters
    ----------
    extract_id : str
        Identifier for the extract.
    file_path : Path
        Path to the period file to process.
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
    
    try:
        # Read parquet file
        df = pl.read_parquet(file_path)
        
        # Extract period from DataFrame
        period = None
        if len(df) > 0 and "PERIOD" in df.columns:
            period = df["PERIOD"].unique().to_list()[0]
        
        # Validate file format (should have EXHAUSTIVITY_VALUE)
        if "EXHAUSTIVITY_VALUE" not in df.columns:
            current_run.log_warning(f"⚠️  File {file_path.name} missing EXHAUSTIVITY_VALUE, skipping")
            return
        
        # Map to DHIS2 format with default COC/AOC
        default_coc = "HllvX50cXC0"
        default_aoc = "HllvX50cXC0"
        df_mapped = df.with_columns([
            pl.lit(target_data_element_uid).alias("DX_UID"),
            pl.col("EXHAUSTIVITY_VALUE").cast(pl.Utf8).alias("VALUE"),
            pl.lit(default_coc).alias("CATEGORY_OPTION_COMBO"),
            pl.lit(default_aoc).alias("ATTRIBUTE_OPTION_COMBO"),
        ]).select([
            "DX_UID", "PERIOD", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "ATTRIBUTE_OPTION_COMBO", "VALUE"
        ])
        
        # Filter by TARGET_DATASET_UID org units
        if target_dataset_uid and target_dataset_uid != "unknown":
            df_mapped = filter_by_dataset_org_units(dhis2_client, df_mapped, target_dataset_uid)
            if df_mapped.is_empty():
                current_run.log_warning(
                    f"⚠️  No data after filtering by {target_dataset_uid} for {extract_id} period {period}"
                )
                return
        
        # Filter out invalid rows
        rows_before = len(df_mapped)
        df_filtered = df_mapped.filter(
            pl.col("DX_UID").is_not_null() &
            pl.col("PERIOD").is_not_null() &
            pl.col("ORG_UNIT").is_not_null() &
            pl.col("VALUE").is_not_null() &
            pl.col("CATEGORY_OPTION_COMBO").is_not_null() &
            pl.col("ATTRIBUTE_OPTION_COMBO").is_not_null()
        )
        rows_removed = rows_before - len(df_filtered)
        if rows_removed > 0:
            current_run.log_warning(f"⚠️  Removed {rows_removed} invalid rows before pushing")
        
        if df_filtered.is_empty():
            current_run.log_warning(f"⚠️  No valid data to push for {extract_id} period {period} after filtering")
            return
        
        # Calculate exhaustivity percentage for logging
        num_values_1 = int(df_filtered.filter(pl.col("VALUE") == "1").height)
        exhaustivity_pct = (num_values_1 / len(df_filtered) * 100) if len(df_filtered) > 0 else 0.0
        
        # Get unique org units count
        num_org_units = df_filtered["ORG_UNIT"].n_unique()
        
        # Push data for this period
        current_run.log_info(
            f"🚀 Pushing {len(df_filtered):,} rows for {extract_id} "
            f"(period: {period}) -> {target_data_element_uid}, "
            f"{num_org_units} ORG_UNITs, {exhaustivity_pct:.1f}% exhaustivity"
        )
        pusher.push_data(df_data=df_filtered)
        current_run.log_info(f"✅ Push completed for {extract_id} period {period}")
        
    except Exception as e:
        current_run.log_error(f"❌ Error processing {file_path.name}: {e!s}")
        logging.error(f"Error processing {file_path.name}: {e!s}\n{traceback.format_exc()}")
        raise


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
    dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    
    # Check if dhis2_request returned an error (it handles all exceptions internally)
    if "error" in dataset_payload:
        current_run.log_warning(
            f"Error fetching dataset {dataset_id} for org unit alignment: {dataset_payload['error']}. "
            f"Returning original data."
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

