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

def dhis2_request(session, method: str, url: str, **kwargs):
    """Make a request to DHIS2 API and return JSON response."""
    if method.lower() == "get":
        r = session.get(url, **kwargs)
    elif method.lower() == "post":
        r = session.post(url, **kwargs)
    elif method.lower() == "put":
        r = session.put(url, **kwargs)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    r.raise_for_status()
    return r.json()

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
        compute_ready = compute_exhaustivity_data(
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
        # IMPORTANT: push_data must wait for both compute_exhaustivity_data AND update_dataset_org_units
        # to ensure exhaustivity files are created before pushing
        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=(compute_ready and sync_ready),
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

    # load configuration from root configuration folder
    extract_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "extract_config.json")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

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
                output_file = dhis2_extractor.analytics_data_elements.download_period(
                    data_elements=data_element_uids,
                    org_units=org_units,
                    period=period,
                    output_dir=pipeline_path / "data" / "extracts" / folder_name,
                )
                if output_file is None:
                    current_run.log_warning(f"Extract {extract_id} download returned None for period {period}. No file created.")
                elif not output_file.exists():
                    current_run.log_warning(f"Extract {extract_id} download returned path {output_file} but file does not exist for period {period}.")
                else:
                    current_run.log_info(f"Extract {extract_id} successfully created file for period {period}: {output_file}")

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
) -> bool:
    """Computes exhaustivity from extracted data and saves the result to processed/ folder.
    
    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    run_task : bool
        Whether to run the task or skip it.
    """
    if not run_task:
        current_run.log_info("Exhaustivity computation task skipped.")
        return True

    current_run.log_info("Exhaustivity computation task started.")
    
    # Initialize queue early (before try block) so we can always enqueue FINISH
    db_path = Path(__file__).parent / "configuration" / ".queue.db"
    push_queue = Queue(db_path)
    
    try:
        configure_logging(logs_path=pipeline_path / "logs" / "compute", task_name="compute_exhaustivity")
    except Exception as e:
        # If configure_logging fails (e.g., in test environment), continue anyway
        current_run.log_warning(f"Could not configure logging: {e!s}")

    # Load extract config from root configuration (not workspace copy)
    from utils import load_configuration
    extract_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "extract_config.json")
    
    # Get date range (same logic as extract_data)
    extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    
    # Reset queue at the start of each run to clear any old entries from previous runs
    # This ensures we don't process files from old runs with different naming conventions
    push_queue.reset()
    current_run.log_info("🧹 Queue reset at start of exhaustivity computation")
    
    # Compute exhaustivity for all extracts
    exhaustivity_periods = get_periods(start, end)
    current_run.log_info(f"Processing {len(extract_config['DATA_ELEMENTS'].get('EXTRACTS', []))} extracts for {len(exhaustivity_periods)} periods")
    for target_extract in extract_config["DATA_ELEMENTS"].get("EXTRACTS", []):
        extract_id = target_extract.get("EXTRACT_UID")
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
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback for {extract_id}:\n{traceback_str}")
            # Continue with next extract instead of crashing
            current_run.log_warning(f"⚠️  Skipping {extract_id} due to error, continuing with next extract")
    
    # Enqueue FINISH marker after all extracts are processed
    push_queue.enqueue("FINISH")
    current_run.log_info("✅ FINISH marker enqueued")


def compute_exhaustivity_and_queue(
    pipeline_path: Path,
    extract_id: str,
    exhaustivity_periods: list[str],
    push_queue: Queue,
) -> None:
    """Computes exhaustivity from extracted data and saves the result to processed/ folder.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    exhaustivity_periods : list[str]
        List of periods to process.
    """
    # Load config to get org units level for folder naming
    from utils import load_configuration
    extract_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "extract_config.json")
    
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
    
    # Clean summary file at the start for this extract
    
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
    if extract_config_item and extract_config_item.get("ORG_UNITS"):
        expected_org_units = extract_config_item.get("ORG_UNITS")
        current_run.log_info(
            f"Using {len(expected_org_units)} expected ORG_UNITs from extract_config.ORG_UNITS"
        )
    else:
        # Try to get from SOURCE_DATASET_UID in push_config (preferred method)
        try:
            from utils import load_configuration
            push_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
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
                config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
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
    # IMPORTANT: Ne pas utiliser expected_org_units pour créer une grille complète
    # On veut seulement les combinaisons qui existent réellement dans les parquet
    # Passer expected_org_units=None pour que compute_exhaustivity utilise seulement les org units des données
    current_run.log_info(
        f"Computing exhaustivity for {len(exhaustivity_periods)} periods: {exhaustivity_periods}. "
        f"Using only org units present in extracted data (no complete grid with all dataset org units)."
    )
    exhaustivity_df = compute_exhaustivity(
        pipeline_path=pipeline_path,
        extract_id=extract_id,
        periods=exhaustivity_periods,  # Pass all periods
        expected_dx_uids=expected_dx_uids,
        expected_org_units=None,  # IMPORTANT: None = utiliser seulement les org units présentes dans les données
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
    # Also include all requested periods to ensure we create files even if they have no data (exhaustivity=0)
    periods_to_save = sorted(set(periods_in_result + exhaustivity_periods))
    current_run.log_info(f"Periods to save: {periods_to_save}")

    # Pre-load org units from extracted files to allow zero-filling for empty periods
    all_org_units_for_extract: list[str] = []
    try:
        # Read ORG_UNIT column from all extract files (across all periods)
        extract_files = list(extracts_folder.glob("data_*.parquet"))
        if extract_files:
            dfs_ous = []
            for ef in extract_files:
                try:
                    dfs_ous.append(pl.read_parquet(ef, columns=["ORG_UNIT"]))
                except Exception:
                    continue
            if dfs_ous:
                ou_df = pl.concat(dfs_ous, how="vertical_relaxed")
                all_org_units_for_extract = ou_df["ORG_UNIT"].unique().to_list()
    except Exception as e:
        current_run.log_warning(f"Could not pre-load org units for zero-filling: {e}")
    
    for period in periods_to_save:
        # Filter for the current period
        period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
        
        if len(period_exhaustivity) == 0:
            # If no data for this period, create zero rows for all known org units
            if all_org_units_for_extract:
                period_exhaustivity = pl.DataFrame({
                    "PERIOD": [period] * len(all_org_units_for_extract),
                    "ORG_UNIT": all_org_units_for_extract,
                    "EXHAUSTIVITY_VALUE": [0] * len(all_org_units_for_extract),
                })
                current_run.log_info(
                    f"Period {period}: no data, filled {len(all_org_units_for_extract)} org units with EXHAUSTIVITY_VALUE=0"
                )
            else:
                current_run.log_warning(
                    f"No exhaustivity data computed for period {period}, and no org units available to fill with zeros."
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

        try:
            # Write aggregated file in the main processed folder (replaces old COC-level files)
            aggregated_file = output_dir / f"exhaustivity_{period}.parquet"
            save_to_parquet(
                data=period_exhaustivity_org_level,
                filename=aggregated_file,
            )
            
            # Enqueue aggregated file for pushing
            push_queue.enqueue(f"{extract_id}|{aggregated_file}")
            current_run.log_info(
                f"✅ Saved and enqueued aggregated exhaustivity for {extract_id} - period {period}: "
                f"{len(period_exhaustivity_org_level)} combis org → {aggregated_file.name}"
            )
        except Exception as e:
            current_run.log_error(f"❌ Error saving exhaustivity data for {extract_id} - period {period}: {e!s}")
            logging.error(f"Exhaustivity saving error: {e!s}")
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            raise
    
    # After all periods are saved for this extract, enqueue a marker to indicate extract is complete
    # This allows push_data to collect all files for an extract before concatenating and pushing
    push_queue.enqueue(f"EXTRACT_COMPLETE|{extract_id}")
    current_run.log_info(f"✅ All periods saved for extract {extract_id}, extract complete marker enqueued")
    
    current_run.log_info("Exhaustivity computation finished.")
    return True


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
        (DX_UID is NOT present in the processed data - it will be reconstructed from mappings)
    original_data : pl.DataFrame, optional
        Original data to determine expected DX_UIDs for each COC. If not provided, will try to use push_config.
    pipeline_path : Path, optional
        Path to the pipeline directory. Required if DX_UID needs to be reconstructed from mappings.
    extract_id : str, optional
        Extract ID. Used to match with push_config if needed.
    extract_config_item : dict, optional
        Extract configuration item. Used to determine relevant DX_UIDs.

    Returns
    -------
    pl.DataFrame
        A DataFrame formatted for DHIS2 import with columns:
        DATA_TYPE, DX_UID, PERIOD, ORG_UNIT, CATEGORY_OPTION_COMBO, VALUE, etc.
        Creates one entry per DX_UID expected for each COC.
    """
    from utils import load_configuration
    
    # Check required columns
    required_cols = ["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "EXHAUSTIVITY_VALUE"]
    missing_cols = [col for col in required_cols if col not in exhaustivity_df.columns]
    if missing_cols:
        current_run.log_error(
            f"Missing required columns in exhaustivity_df: {missing_cols}. "
            f"Expected columns: {required_cols}"
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
    
    # Check if DX_UID is already present (backward compatibility with old format)
    if "DX_UID" in exhaustivity_df.columns:
        current_run.log_info(
            f"Formatting exhaustivity data for import: {len(exhaustivity_df)} entries "
            f"(DX_UID already present, using directly)"
        )
        df_with_dx_uid = exhaustivity_df
    else:
        # DX_UID not present - need to reconstruct from mappings
        current_run.log_info(
            f"Formatting exhaustivity data for import: {len(exhaustivity_df)} entries "
            f"(DX_UID not present, reconstructing from mappings)"
        )
        
        # Build expected_dx_uids_by_coc from mappings (same logic as compute_exhaustivity)
        expected_dx_uids_by_coc: dict[str, list[str]] = {}
        extract_mappings = {}
        
        # 1) Try to get mappings from push_config first
        if pipeline_path:
            try:
                push_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
                push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
                
                # Try to find matching extract by EXTRACT_UID
                matching_extract = None
                matching_extract = next(
                    (e for e in push_extracts if e.get("EXTRACT_UID") == extract_id),
                    None
                )
                
                # If found but has empty mappings, or not found, try alternative names
                if not matching_extract or not matching_extract.get("MAPPINGS"):
                    if extract_id and ("Fosa" in extract_id or "fosa" in extract_id.lower()):
                        alt_extract = next(
                            (e for e in push_extracts if e.get("EXTRACT_UID") == "level_fosa"),
                            None
                        )
                        if alt_extract and alt_extract.get("MAPPINGS"):
                            matching_extract = alt_extract
                    elif extract_id and ("BCZ" in extract_id or "zs" in extract_id.lower()):
                        alt_extract = next(
                            (e for e in push_extracts if e.get("EXTRACT_UID") == "level_zs"),
                            None
                        )
                        if alt_extract and alt_extract.get("MAPPINGS"):
                            matching_extract = alt_extract
                
                if matching_extract:
                    push_mappings = matching_extract.get("MAPPINGS", {})
                    if push_mappings:
                        found_via_alternative = matching_extract.get("EXTRACT_UID") in ["level_fosa", "level_zs"]
                        
                        if extract_config_item and extract_config_item.get("UIDS") and not found_via_alternative:
                            extract_uids = set(extract_config_item.get("UIDS", []))
                            for uid, mapping in push_mappings.items():
                                target_uid = mapping.get("UID", "")
                                if uid in extract_uids or target_uid in extract_uids:
                                    extract_mappings[uid] = mapping
                        else:
                            extract_mappings.update(push_mappings)
                        
                        if extract_mappings:
                            via_info = f" (found via {matching_extract.get('EXTRACT_UID')})" if found_via_alternative else ""
                            current_run.log_info(f"Loaded {len(extract_mappings)} mappings from push_config for {extract_id}{via_info}")
            except Exception as e:
                current_run.log_warning(f"Could not load mappings from push_config: {e!s}")
        
        # 2) If no mappings in push_config, try extract_config as fallback
        if not extract_mappings and extract_config_item:
            extract_mappings = extract_config_item.get("MAPPINGS", {})
            if extract_mappings:
                current_run.log_info(f"Loaded {len(extract_mappings)} mappings from extract_config for {extract_id}")
        
        # 3) Build expected_dx_uids_by_coc from mappings (indexed by COC SOURCE, using DX_UID SOURCE)
        # Note: The processed data contains COC SOURCE values (from compute_exhaustivity),
        # so we need to index by COC SOURCE and use DX_UID SOURCE.
        # The mappings will be applied later in process_extract_files to transform SOURCE → TARGET.
        if extract_mappings:
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids_source = set(extract_config_item.get("UIDS", []))
            else:
                relevant_dx_uids_source = None
            
            expected_dx_uids_by_coc_sets: dict[str, set[str]] = {}
            for dx_uid_source, mapping in extract_mappings.items():
                if relevant_dx_uids_source and dx_uid_source not in relevant_dx_uids_source:
                    continue
                
                coc_map = mapping.get("CATEGORY_OPTION_COMBO", {}) or {}
                # IMPORTANT: The processed data contains COC TARGET values (same as extracted data),
                # so we must index by COC TARGET, not COC SOURCE
                # The mapping structure is: COC SOURCE -> COC TARGET, so we use TARGET as index
                for src_coc, target_coc in coc_map.items():
                    if target_coc is None:
                        continue
                    target_coc_str = str(target_coc).strip()
                    if not target_coc_str:
                        continue
                    # Index by COC TARGET (processed data has TARGET values), use DX_UID SOURCE
                    expected_dx_uids_by_coc_sets.setdefault(target_coc_str, set()).add(str(dx_uid_source))
            
            expected_dx_uids_by_coc = {
                coc: sorted(list(dx_uids)) for coc, dx_uids in expected_dx_uids_by_coc_sets.items()
            }
            current_run.log_info(
                f"Expected DX_UIDs per COC loaded from mappings (indexed by COC TARGET): {len(expected_dx_uids_by_coc)} COCs"
            )
        
        # 4) Fallback: if no mappings available, derive from original_data
        if not expected_dx_uids_by_coc and original_data is not None and len(original_data) > 0:
            if extract_config_item and extract_config_item.get("UIDS"):
                relevant_dx_uids = set(extract_config_item.get("UIDS", []))
                original_data_filtered = original_data.filter(pl.col("DX_UID").is_in(list(relevant_dx_uids)))
            else:
                original_data_filtered = original_data
            
            if len(original_data_filtered) > 0:
                coc_dx_uids_df = (
                    original_data_filtered.group_by("CATEGORY_OPTION_COMBO")
                    .agg(pl.col("DX_UID").unique().sort().alias("DX_UIDs"))
                )
                expected_dx_uids_by_coc = {
                    row["CATEGORY_OPTION_COMBO"]: row["DX_UIDs"] if isinstance(row["DX_UIDs"], list) else [row["DX_UIDs"]]
                    for row in coc_dx_uids_df.iter_rows(named=True)
                }
                current_run.log_info(
                    f"Expected DX_UIDs per COC derived from original_data (fallback): {len(expected_dx_uids_by_coc)} COCs"
                )
        
        # IMPORTANT: For exhaustivity, we need 1 data point per (PERIOD, ORG_UNIT, COC) 
        # with the TARGET dataElement (not the source DX_UIDs which multiply entries).
        # The target dataElement is obtained from mappings after transformation.
        # For now, we keep 1 row per (PERIOD, COC, ORG_UNIT) and let the mapping 
        # transform COC source → COC target and add the target dataElement.
        
        # Get target dataElement UID from mappings
        # For exhaustivity, there should be ONE target dataElement per extract
        target_data_element = None
        
        # Try to get matching_extract for TARGET_DATA_ELEMENT_UID lookup
        matching_extract_for_target = None
        if pipeline_path:
            try:
                push_config_check = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
                push_extracts_check = push_config_check.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
                matching_extract_for_target = next(
                    (e for e in push_extracts_check if e.get("EXTRACT_UID") == extract_id),
                    None
                )
            except:
                pass
        
        if extract_mappings:
            # Try to get target dataElement from TARGET_DATA_ELEMENT_UID in config
            if matching_extract_for_target and matching_extract_for_target.get("TARGET_DATA_ELEMENT_UID"):
                target_data_element = matching_extract_for_target.get("TARGET_DATA_ELEMENT_UID")
                current_run.log_info(f"Using TARGET_DATA_ELEMENT_UID from config: {target_data_element}")
            else:
                # Fallback: use first mapping's target UID (assuming all map to same target)
                first_mapping = next(iter(extract_mappings.values()), None)
                if first_mapping:
                    target_data_element = first_mapping.get("UID")
                    if target_data_element:
                        current_run.log_info(f"Using target dataElement from first mapping: {target_data_element}")
        
        if not target_data_element:
            current_run.log_warning(
                "Could not determine target dataElement from mappings. "
                "Will use placeholder - mapping should set correct dataElement."
            )
            # Use a placeholder that will be replaced by mapping
            target_data_element = "PLACEHOLDER_NEEDS_MAPPING"
        
        # Keep 1 row per (PERIOD, COC, ORG_UNIT) - DO NOT multiply by DX_UIDs
        # Add target dataElement as DX_UID (will be mapped later)
        df_with_dx_uid = exhaustivity_df.with_columns([
            pl.lit(target_data_element).cast(pl.Utf8).alias("DX_UID")
        ])
        
        current_run.log_info(
            f"Formatted for import: {len(exhaustivity_df)} combinations → {len(df_with_dx_uid)} entries "
            f"(1 data point per PERIOD/COC/ORG_UNIT with target dataElement: {target_data_element})"
        )
    
    # Format for DHIS2 import (like format_for_import in CMM pipeline)
    df_final = df_with_dx_uid.with_columns([
        pl.lit("DATA_ELEMENT").alias("DATA_TYPE"),
        pl.col("DX_UID").cast(pl.Utf8).alias("DX_UID"),
        pl.col("CATEGORY_OPTION_COMBO").cast(pl.Utf8).alias("CATEGORY_OPTION_COMBO"),
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
        config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
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
    # load configuration from root configuration folder
    config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
    extract_config_full = load_configuration(config_path=Path(__file__).parent / "configuration" / "extract_config.json")
    dhis2_client = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)

    # Initialize queue (same pattern as dhis2_cmm_push)
    db_path = Path(__file__).parent / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    # Push parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 2000)
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

    # Dictionary to collect files per extract (maintains current output behavior: concatenate before push)
    extract_files_collected: dict[str, list[Path]] = {}

    # Loop over the queue (same pattern as dhis2_cmm_push)
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
            current_run.log_info(f"Push data process: waiting for exhaustivity files to be computed... (queue empty, {queue_count} items)")
            # Queue SQLite handles synchronization between processes
            # Check periodically (same pattern as dhis2_cmm_push)
            time.sleep(60 * int(push_wait))
            continue
        
        # Log what we found in the queue
        current_run.log_info(f"🔍 Found item in queue: {next_item[:100]}... (queue has {queue_count} items)")

        try:
            # Check if this is an extract complete marker
            if next_item.startswith("EXTRACT_COMPLETE|"):
                # Extract complete marker - we should have collected all files for this extract
                _, extract_id = split_on_pipe(next_item)
                push_queue.dequeue()  # remove marker
                # If we have files collected for this extract, process them now
                if extract_id in extract_files_collected and len(extract_files_collected[extract_id]) > 0:
                    current_run.log_info(f"📦 Extract {extract_id} complete, processing {len(extract_files_collected[extract_id])} file(s)")
                    # Process all files for this extract (concatenate and push)
                    process_extract_files(
                        extract_id=extract_id,
                        file_paths=extract_files_collected[extract_id],
                        pipeline_path=pipeline_path,
                        extract_config_full=extract_config_full,
                        dispatch_map=dispatch_map,
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
            current_run.log_info(f"📦 Collected file for extract {extract_id}: {extract_path.name} (total: {len(extract_files_collected[extract_id])} file(s))")
            continue  # Continue to next queue item (wait for EXTRACT_COMPLETE marker)

        except Exception as e:
            error_msg = f"❌ Error processing queue item {next_item}: {e!s}"
            current_run.log_error(error_msg)
            logging.error(error_msg)
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            push_queue.dequeue()  # remove problematic item
            continue
    
    current_run.log_info(f"✅ Data push task finished.")
    return True


def process_extract_files(
    extract_id: str,
    file_paths: list[Path],
    pipeline_path: Path,
    extract_config_full: dict,
    dispatch_map: dict,
    pusher,
    dhis2_client,
) -> None:
    """Process all files for an extract by concatenating them before pushing.
    
    This maintains the current output behavior: all files for an extract are concatenated
    into a single DataFrame before pushing, rather than pushing file by file.
    
    Parameters
    ----------
    extract_id : str
        Identifier for the extract.
    file_paths : list[Path]
        List of file paths to process for this extract.
    pipeline_path : Path
        Path to the pipeline directory.
    extract_config_full : dict
        Full extract configuration.
    dispatch_map : dict
        Mapping of data types to their respective mapping functions.
    pusher : DHIS2Pusher
        DHIS2 pusher instance.
    dhis2_client : DHIS2Client
        DHIS2 client instance.
    """
    from utils import load_configuration
    
    # Get extract config item
    extracts_list = extract_config_full.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    extract_config_item = next(
        (e for e in extracts_list if e.get("EXTRACT_UID") == extract_id),
        None
    )
    
    # Determine folder name
    org_units_level = extract_config_item.get("ORG_UNITS_LEVEL") if extract_config_item else None
    if org_units_level is not None:
        folder_name = f"Extract lvl {org_units_level}"
    else:
        folder_name = f"Extract {extract_id}"
    extracts_folder = pipeline_path / "data" / "extracts" / folder_name
    
    # Get TARGET_DATA_ELEMENT_UID from push_config for this extract
    push_config = load_configuration(config_path=Path(__file__).parent / "configuration" / "push_config.json")
    push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    push_extract_config = next((e for e in push_extracts if e.get("EXTRACT_UID") == extract_id), {})
    target_data_element_uid = push_extract_config.get("TARGET_DATA_ELEMENT_UID")
    
    if not target_data_element_uid:
        current_run.log_warning(f"⚠️  No TARGET_DATA_ELEMENT_UID found in push_config for {extract_id}")
    else:
        current_run.log_info(f"🎯 Using TARGET_DATA_ELEMENT_UID: {target_data_element_uid} for {extract_id}")
    
    # Process all files and collect DataFrames
    all_dataframes = []
    periods_processed = []
    
    for extract_path in file_paths:
        try:
            short_path = f"{extract_path.parent.name}/{extract_path.name}"
            filename = extract_path.name
            
            # Read with polars
            exhaustivity_df_pl = pl.read_parquet(extract_path)
            
            # Extract period from DataFrame
            period = None
            if len(exhaustivity_df_pl) > 0 and "PERIOD" in exhaustivity_df_pl.columns:
                period = exhaustivity_df_pl["PERIOD"].unique().to_list()[0]
                periods_processed.append(period)
            
            # Check if this is an org-level aggregated exhaustivity file (PERIOD, ORG_UNIT, EXHAUSTIVITY_VALUE only)
            is_org_level_aggregated = (
                "EXHAUSTIVITY_VALUE" in exhaustivity_df_pl.columns and
                "CATEGORY_OPTION_COMBO" not in exhaustivity_df_pl.columns
            )
            
            if is_org_level_aggregated:
                # Simple format: just add DX_UID from config and rename EXHAUSTIVITY_VALUE to VALUE
                # Use DHIS2 default COC/AOC
                DEFAULT_COC = "HllvX50cXC0"  # default categoryOptionCombo
                DEFAULT_AOC = "HllvX50cXC0"  # default attributeOptionCombo
                
                if not target_data_element_uid:
                    current_run.log_error(f"❌ Cannot process {short_path}: no TARGET_DATA_ELEMENT_UID configured")
                    continue
                
                df_mapped = exhaustivity_df_pl.with_columns([
                    pl.lit(target_data_element_uid).alias("DX_UID"),
                    pl.col("EXHAUSTIVITY_VALUE").cast(pl.Utf8).alias("VALUE"),
                    pl.lit(DEFAULT_COC).alias("CATEGORY_OPTION_COMBO"),
                    pl.lit(DEFAULT_AOC).alias("ATTRIBUTE_OPTION_COMBO"),
                ]).select([
                    "DX_UID", "PERIOD", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "ATTRIBUTE_OPTION_COMBO", "VALUE"
                ])
                
                current_run.log_info(f"   ✓ Formatted org-level aggregated: {short_path} → {len(df_mapped)} rows, DX_UID={target_data_element_uid}")
            else:
                # Legacy format with CATEGORY_OPTION_COMBO - use old logic
                # Read original data if available (for fallback)
                original_data = None
                if period:
                    period_file = extracts_folder / f"data_{period}.parquet"
                    if period_file.exists():
                        try:
                            original_data = pl.read_parquet(period_file)
                        except Exception:
                            pass
                
                # Check if file is already formatted for DHIS2 import
                if "VALUE" in exhaustivity_df_pl.columns and "EXHAUSTIVITY_VALUE" not in exhaustivity_df_pl.columns:
                    df_final = exhaustivity_df_pl
                    required_cols = ["DATA_TYPE", "DX_UID", "PERIOD", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "ATTRIBUTE_OPTION_COMBO", "VALUE", "RATE_TYPE", "DOMAIN_TYPE"]
                    for col in required_cols:
                        if col not in df_final.columns:
                            df_final = df_final.with_columns([pl.lit(None).cast(pl.Utf8).alias(col)])
                else:
                    df_final = format_for_exhaustivity_import(
                        exhaustivity_df_pl,
                        original_data=original_data,
                        pipeline_path=pipeline_path,
                        extract_id=extract_id,
                        extract_config_item=extract_config_item,
                    )
                
                if df_final.is_empty():
                    current_run.log_warning(f"⚠️  Empty DataFrame after formatting for {extract_id} (file: {short_path}), skipping")
                    continue

                # Apply mapping
                file_already_formatted = "VALUE" in exhaustivity_df_pl.columns and "EXHAUSTIVITY_VALUE" not in exhaustivity_df_pl.columns
                
                if file_already_formatted:
                    df_mapped = df_final
                    if "DX_UID" in df_mapped.columns and df_mapped["DX_UID"].dtype == pl.List:
                        current_run.log_warning(f"⚠️  DX_UID is a List in {short_path}, exploding to create one row per DX_UID")
                        rows_before = len(df_mapped)
                        df_mapped = df_mapped.explode("DX_UID")
                        df_mapped = df_mapped.with_columns([
                            pl.col("DX_UID").cast(pl.Utf8).alias("DX_UID")
                        ])
                        rows_after = len(df_mapped)
                        current_run.log_info(f"   Exploded: {rows_before} → {rows_after} rows")
                else:
                    cfg_list, mapper_func = dispatch_map["DATA_ELEMENT"]
                    extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})

                    if not extract_config:
                        current_run.log_warning(f"Extract config not found in push_config for {extract_id}, pushing without mappings")
                    else:
                        target_dataset = extract_config.get("TARGET_DATASET_UID", "unknown")
                        current_run.log_info(f"   Using mappings for {extract_id} → TARGET_DATASET_UID: {target_dataset}")
                    
                    df_mapped = mapper_func(df=df_final, extract_config=extract_config)
            
            # Check if mapped DataFrame is empty
            if df_mapped.is_empty() if hasattr(df_mapped, 'is_empty') else len(df_mapped) == 0:
                current_run.log_warning(f"⚠️  Mapped DataFrame is empty for {extract_id} (file: {short_path}), skipping")
                continue
            
            # Add to list for concatenation
            all_dataframes.append(df_mapped)
            current_run.log_info(f"   ✓ Processed {short_path}: {len(df_mapped)} rows")

        except Exception as e:
            error_msg = f"Fatal error for extract {extract_id} ({extract_path.name}): {e!s}"
            current_run.log_error(f"Fatal error for extract {extract_id} ({extract_path.name}), continuing with next file.")
            logging.error(error_msg)
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
            continue
    
    # Concatenate all dataframes for this extract and push once (maintains current output behavior)
    if all_dataframes:
        try:
            # Format periods list for logging
            periods_str = sorted(set(periods_processed)) if periods_processed else 'unknown'
            
            # Concatenate all dataframes
            df_combined = pl.concat(all_dataframes, how="vertical_relaxed")
            
            # Sort by ORG_UNIT for faster DHIS2 processing (same as dhis2_dataset_sync)
            df_combined = df_combined.sort("ORG_UNIT")
            
            # Get TARGET_DATASET_UID for logging (from push_config)
            cfg_list, _ = dispatch_map["DATA_ELEMENT"]
            extract_config_for_log = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})
            target_dataset_uid = extract_config_for_log.get("TARGET_DATASET_UID", "unknown")
            
            # Filter by TARGET_DATASET_UID org units before pushing
            if target_dataset_uid:
                df_combined = filter_by_dataset_org_units(dhis2_client, df_combined, target_dataset_uid)
                if df_combined.is_empty():
                    current_run.log_warning(f"⚠️  DataFrame is empty after filtering by TARGET_DATASET_UID {target_dataset_uid} for {extract_id}, skipping.")
                    return
            
            # Filter out invalid data BEFORE pushing to avoid processing invalid rows
            # Invalid = missing required fields (DX_UID, PERIOD, ORG_UNIT, CATEGORY_OPTION_COMBO, VALUE)
            required_cols = ["DX_UID", "PERIOD", "ORG_UNIT", "CATEGORY_OPTION_COMBO", "VALUE"]
            rows_before_filter = len(df_combined)
            
            # Filter out rows with None/null in required columns
            df_filtered = df_combined.filter(
                pl.col("DX_UID").is_not_null() &
                pl.col("PERIOD").is_not_null() &
                pl.col("ORG_UNIT").is_not_null() &
                pl.col("CATEGORY_OPTION_COMBO").is_not_null() &
                pl.col("VALUE").is_not_null()
            )
            
            rows_after_filter = len(df_filtered)
            rows_filtered_out = rows_before_filter - rows_after_filter
            
            if rows_filtered_out > 0:
                current_run.log_warning(
                    f"⚠️  Filtered out {rows_filtered_out:,} invalid rows (missing required fields) "
                    f"before push ({rows_before_filter:,} → {rows_after_filter:,})"
                )
            
            # Push all data at once (DHIS2Pusher will automatically split into chunks of max_post)
            # IMPORTANT: Each extract pushes to its own TARGET_DATASET_UID via the mappings
            current_run.log_info(
                f"🚀 Pushing {rows_after_filter:,} valid rows for {extract_id} "
                f"(from {len(all_dataframes)} file(s), periods: {periods_str}) "
                f"→ TARGET_DATASET_UID: {target_dataset_uid}"
            )
            try:
                pusher.push_data(df_data=df_filtered)
                current_run.log_info(
                    f"✅ Push completed for {extract_id} "
                    f"({len(all_dataframes)} file(s), {len(df_combined)} rows total)"
                )
            except Exception as push_error:
                error_msg = f"❌ Error during push_data call for {extract_id}: {push_error!s}"
                current_run.log_error(error_msg)
                logging.error(error_msg)
                import traceback
                traceback_str = traceback.format_exc()
                logging.error(f"Full traceback:\n{traceback_str}")
                raise  # Re-raise to be caught by outer try/except
        except Exception as e:
            error_msg = f"❌ Error pushing data for {extract_id}: {e!s}"
            current_run.log_error(error_msg)
            logging.error(error_msg)
            import traceback
            traceback_str = traceback.format_exc()
            logging.error(f"Full traceback:\n{traceback_str}")
    else:
        current_run.log_warning(
            f"⚠️  No valid data to push for {extract_id}. "
            f"Checked {len(file_paths)} file(s), but all were empty or invalid after processing."
        )


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
    
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided) using polars
        df_uid = df_pl.filter(pl.col("DX_UID") == uid)
        
        # Only log warnings/errors, not verbose debug info
        if len(df_uid) > 0:
            if "CATEGORY_OPTION_COMBO" not in df_uid.columns:
                current_run.log_warning(f"⚠️  CATEGORY_OPTION_COMBO column not found in DataFrame for {uid}!")
        
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            coc_keys = list(coc_mappings_clean.keys())
            
            # Only filter and replace by COC if CATEGORY_OPTION_COMBO column exists
            if "CATEGORY_OPTION_COMBO" in df_uid.columns:
                rows_before_coc_filter = len(df_uid)
                df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                rows_after_coc_filter = len(df_uid)
                
                # Only log if there's a problem (rows filtered out)
                if rows_after_coc_filter == 0 and rows_before_coc_filter > 0:
                    current_run.log_warning(f"⚠️  COC filter removed all rows for {uid}! COCs in data don't match mapping keys.")
                elif rows_after_coc_filter < rows_before_coc_filter:
                    current_run.log_warning(
                        f"⚠️  COC filter removed {rows_before_coc_filter - rows_after_coc_filter} rows for {uid} "
                        f"({rows_before_coc_filter} -> {rows_after_coc_filter})"
                    )
                
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


def filter_by_dataset_org_units(dhis2_client: DHIS2, data: pl.DataFrame, dataset_id: str) -> pl.DataFrame:
    """Filters the provided data to include only rows with organisation units present in the specified DHIS2 dataset.
    """
    url = f"{dhis2_client.api.url}/dataSets/{dataset_id}"
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except requests.exceptions.RequestException as e:
        current_run.log_warning(f"Network/HTTP error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
        return data
    except Exception as e:
        current_run.log_warning(f"Unexpected error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
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
    
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided) using polars
        df_uid = df_pl.filter(pl.col("DX_UID") == uid)
        
        # Only log warnings/errors, not verbose debug info
        if len(df_uid) > 0:
            if "CATEGORY_OPTION_COMBO" not in df_uid.columns:
                current_run.log_warning(f"⚠️  CATEGORY_OPTION_COMBO column not found in DataFrame for {uid}!")
        
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            coc_keys = list(coc_mappings_clean.keys())
            
            # Only filter and replace by COC if CATEGORY_OPTION_COMBO column exists
            if "CATEGORY_OPTION_COMBO" in df_uid.columns:
                rows_before_coc_filter = len(df_uid)
                df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                rows_after_coc_filter = len(df_uid)
                
                # Only log if there's a problem (rows filtered out)
                if rows_after_coc_filter == 0 and rows_before_coc_filter > 0:
                    current_run.log_warning(f"⚠️  COC filter removed all rows for {uid}! COCs in data don't match mapping keys.")
                elif rows_after_coc_filter < rows_before_coc_filter:
                    current_run.log_warning(
                        f"⚠️  COC filter removed {rows_before_coc_filter - rows_after_coc_filter} rows for {uid} "
                        f"({rows_before_coc_filter} -> {rows_after_coc_filter})"
                    )
                
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


def filter_by_dataset_org_units(dhis2_client: DHIS2, data: pl.DataFrame, dataset_id: str) -> pl.DataFrame:
    """Filters the provided data to include only rows with organisation units present in the specified DHIS2 dataset.
    """
    url = f"{dhis2_client.api.url}/dataSets/{dataset_id}"
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except requests.exceptions.RequestException as e:
        current_run.log_warning(f"Network/HTTP error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
        return data
    except Exception as e:
        current_run.log_warning(f"Unexpected error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
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
    
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided) using polars
        df_uid = df_pl.filter(pl.col("DX_UID") == uid)
        
        # Only log warnings/errors, not verbose debug info
        if len(df_uid) > 0:
            if "CATEGORY_OPTION_COMBO" not in df_uid.columns:
                current_run.log_warning(f"⚠️  CATEGORY_OPTION_COMBO column not found in DataFrame for {uid}!")
        
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            coc_keys = list(coc_mappings_clean.keys())
            
            # Only filter and replace by COC if CATEGORY_OPTION_COMBO column exists
            if "CATEGORY_OPTION_COMBO" in df_uid.columns:
                rows_before_coc_filter = len(df_uid)
                df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                rows_after_coc_filter = len(df_uid)
                
                # Only log if there's a problem (rows filtered out)
                if rows_after_coc_filter == 0 and rows_before_coc_filter > 0:
                    current_run.log_warning(f"⚠️  COC filter removed all rows for {uid}! COCs in data don't match mapping keys.")
                elif rows_after_coc_filter < rows_before_coc_filter:
                    current_run.log_warning(
                        f"⚠️  COC filter removed {rows_before_coc_filter - rows_after_coc_filter} rows for {uid} "
                        f"({rows_before_coc_filter} -> {rows_after_coc_filter})"
                    )
                
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


def filter_by_dataset_org_units(dhis2_client: DHIS2, data: pl.DataFrame, dataset_id: str) -> pl.DataFrame:
    """Filters the provided data to include only rows with organisation units present in the specified DHIS2 dataset.
    """
    url = f"{dhis2_client.api.url}/dataSets/{dataset_id}"
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except requests.exceptions.RequestException as e:
        current_run.log_warning(f"Network/HTTP error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
        return data
    except Exception as e:
        current_run.log_warning(f"Unexpected error during payload fetch for dataset {dataset_id} alignment: {e!s}. Returning original data.")
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
    
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided) using polars
        df_uid = df_pl.filter(pl.col("DX_UID") == uid)
        
        # Only log warnings/errors, not verbose debug info
        if len(df_uid) > 0:
            if "CATEGORY_OPTION_COMBO" not in df_uid.columns:
                current_run.log_warning(f"⚠️  CATEGORY_OPTION_COMBO column not found in DataFrame for {uid}!")
        
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            coc_keys = list(coc_mappings_clean.keys())
            
            # Only filter and replace by COC if CATEGORY_OPTION_COMBO column exists
            if "CATEGORY_OPTION_COMBO" in df_uid.columns:
                rows_before_coc_filter = len(df_uid)
                df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
                rows_after_coc_filter = len(df_uid)
                
                # Only log if there's a problem (rows filtered out)
                if rows_after_coc_filter == 0 and rows_before_coc_filter > 0:
                    current_run.log_warning(f"⚠️  COC filter removed all rows for {uid}! COCs in data don't match mapping keys.")
                elif rows_after_coc_filter < rows_before_coc_filter:
                    current_run.log_warning(
                        f"⚠️  COC filter removed {rows_before_coc_filter - rows_after_coc_filter} rows for {uid} "
                        f"({rows_before_coc_filter} -> {rows_after_coc_filter})"
                    )
                
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

