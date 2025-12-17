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
from openhexa.toolbox.dhis2.dataframe import get_datasets, get_organisation_units
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
        if run_push_data and run_ds_sync:
            current_run.log_warning("Dataset statuses sync is not implemented yet.")

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
    extract_config = load_configuration(config_path=pipeline_path / "config_files" / "extract_config.json")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

    # initialize queue
    db_path = pipeline_path / "config_files" / ".queue.db"
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
    full_pyramid = get_organisation_units(dhis2_extractor.dhis2_client)

    # loop over the available extract configurations
    for idx, extract in enumerate(data_element_extracts):
        extract_id = extract.get("EXTRACT_UID")
        org_units_level = extract.get("ORG_UNITS_LEVEL", None)
        data_element_uids = extract.get("UIDS", [])
        dataset_uid = extract.get("DATASET_UID")

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

        # get org units from the dataset directly if dataset_uid is provided, otherwise use org_units_level
        if dataset_uid is not None:
            source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_uid]))
            org_units = source_dataset["organisation_units"].explode().to_list()
            dataset_name = source_dataset["name"][0] if len(source_dataset) > 0 else "Unknown"
        else:
            org_units = full_pyramid.filter(pl.col("level") == org_units_level)["id"].to_list()
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

        # Create folder name based on org units level or extract type
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        elif "Fosa" in extract_id:
            folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            folder_name = "Extract lvl 3"
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
                current_run.log_warning(
                    f"Extract {extract_id} download failed for period {period}, continuing with next period."
                )
                logging.error(f"Extract {extract_id} - period {period} error: {e!s}")
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
    configure_logging(logs_path=pipeline_path / "logs" / "compute", task_name="compute_exhaustivity")

    # Load extract config to get extracts and date range
    from utils import load_configuration
    extract_config = load_configuration(config_path=pipeline_path / "config_files" / "extract_config.json")
    
    # Get date range (same logic as extract_data)
    extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    
    # Initialize queue
    db_path = pipeline_path / "config_files" / ".queue.db"
    push_queue = Queue(db_path)
    
    # Compute exhaustivity for all extracts
    exhaustivity_periods = get_periods(start, end)
    for target_extract in extract_config["DATA_ELEMENTS"].get("EXTRACTS", []):
        compute_exhaustivity_and_queue(
            pipeline_path=pipeline_path,
            extract_id=target_extract.get("EXTRACT_UID"),
            exhaustivity_periods=exhaustivity_periods,
            push_queue=push_queue,
        )


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
    extract_config = load_configuration(config_path=pipeline_path / "config_files" / "extract_config.json")
    
    # Find the extract configuration to determine folder name
    extract_config_item = None
    for extract_item in extract_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
        if extract_item.get("EXTRACT_UID") == extract_id:
            extract_config_item = extract_item
            break
    
    # Create folder name based on org units level or extract type
    if extract_config_item:
        org_units_level = extract_config_item.get("ORG_UNITS_LEVEL")
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        elif "Fosa" in extract_id:
            folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            folder_name = "Extract lvl 3"
        else:
            folder_name = f"Extract {extract_id}"
    else:
        # Fallback to extract_id if config not found
        if "Fosa" in extract_id:
            folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            folder_name = "Extract lvl 3"
        else:
            folder_name = f"Extract {extract_id}"
    
    extracts_folder = pipeline_path / "data" / "extracts" / folder_name
    output_dir = pipeline_path / "data" / "processed" / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean summary file at the start for this extract
    
    try:
        # Get expected DX_UIDs and ORG_UNITs from extract configuration
        # Load extract config to get expected data elements
        from utils import load_configuration
        extract_config = load_configuration(config_path=pipeline_path / "config_files" / "extract_config.json")
        
        # Find the extract configuration
        extract_config_item = None
        for extract_item in extract_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
            if extract_item.get("EXTRACT_UID") == extract_id:
                extract_config_item = extract_item
                break
        
        expected_dx_uids = None
        expected_org_units = None
        
        if extract_config_item:
            expected_dx_uids = extract_config_item.get("UIDS", [])
            # Get expected org units: prefer from extract_config ORG_UNITS if available,
            # otherwise from extracted data (all unique ORG_UNITs across all periods)
            if extract_config_item.get("ORG_UNITS"):
                expected_org_units = extract_config_item.get("ORG_UNITS")
                current_run.log_info(
                    f"Using {len(expected_org_units)} expected ORG_UNITs from extract_config"
                )
            else:
                # Fallback: get from extracted data
                try:
                    # Read all parquet files to get all org units
                    all_period_files = list(extracts_folder.glob("data_*.parquet"))
                    if all_period_files:
                        all_data = pl.concat([pl.read_parquet(f) for f in all_period_files])
                        expected_org_units = all_data["ORG_UNIT"].unique().to_list()
                        current_run.log_info(
                            f"Using {len(expected_org_units)} ORG_UNITs from extracted data "
                            f"(no ORG_UNITS in extract_config)"
                        )
                except Exception as e:
                    current_run.log_warning(f"Could not determine expected org units: {e}")
                    expected_org_units = None
        
        # Compute exhaustivity values for ALL periods at once
        # This ensures we detect missing periods and create a complete grid
        # Returns DataFrame with PERIOD, CATEGORY_OPTION_COMBO, ORG_UNIT, EXHAUSTIVITY_VALUE
        current_run.log_info(f"Computing exhaustivity for {len(exhaustivity_periods)} periods: {exhaustivity_periods}")
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
        for period in exhaustivity_periods:
            # Filter for the current period
            period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
            
            if len(period_exhaustivity) == 0:
                current_run.log_warning(f"No exhaustivity data computed for period {period}")
                continue

            # Save exhaustivity data in simplified format: only COC, PERIOD, ORG_UNIT, EXHAUSTIVITY_VALUE
            period_exhaustivity_simplified = period_exhaustivity.select([
                "PERIOD",
                "CATEGORY_OPTION_COMBO",
                "ORG_UNIT",
                "EXHAUSTIVITY_VALUE"
            ])

            try:
                save_to_parquet(
                    data=period_exhaustivity_simplified.to_pandas(),
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
) -> pd.DataFrame:
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
    pd.DataFrame
        A DataFrame formatted for DHIS2 import with columns:
        DATA_TYPE, DX_UID, PERIOD, ORG_UNIT, VALUE, etc.
        Creates one entry per DX_UID expected for each COC.
    """
    # Determine expected DX_UIDs for each COC
    # Priority: 1) push_config, 2) original_data, 3) exhaustivity_df COCs (fallback)
    expected_dx_uids_by_coc: dict[str, list[str]] = {}
    
    # 1) Try to load from push_config (source of truth)
    if pipeline_path:
        try:
            from utils import load_configuration
            push_config = load_configuration(config_path=pipeline_path / "config_files" / "push_config.json")
            push_extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
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
            "VALUE": [],
            "ATTRIBUTE_OPTION_COMBO": [],
            "RATE_TYPE": [],
            "DOMAIN_TYPE": [],
        }).to_pandas()
    
    # Expand exhaustivity_df: for each (PERIOD, COC, ORG_UNIT, EXHAUSTIVITY_VALUE),
    # create entries for all expected DX_UIDs for that COC
    current_run.log_info(
        f"Formatting exhaustivity data for import: {len(exhaustivity_df)} entries, "
        f"{len(expected_dx_uids_by_coc)} COCs with mappings"
    )
    # Optimized: use Polars join instead of nested loops for expansion
    # Create a DataFrame mapping COC to expected DX_UIDs
    coc_dx_uids_list = [
        {"CATEGORY_OPTION_COMBO": coc, "DX_UID": dx_uid}
        for coc, dx_uids in expected_dx_uids_by_coc.items()
        for dx_uid in dx_uids
    ]
    
    if not coc_dx_uids_list:
        current_run.log_warning("No expected DX_UIDs found for any COC, cannot expand exhaustivity data.")
        return pl.DataFrame({
            "DATA_TYPE": [],
            "DX_UID": [],
            "PERIOD": [],
            "ORG_UNIT": [],
            "VALUE": [],
            "ATTRIBUTE_OPTION_COMBO": [],
            "RATE_TYPE": [],
            "DOMAIN_TYPE": [],
        }).to_pandas()
    
    coc_dx_uids_df = pl.DataFrame(coc_dx_uids_list)
    
    # Join exhaustivity_df with coc_dx_uids_df to expand
    # This creates one row per (PERIOD, COC, ORG_UNIT, DX_UID) combination
    expanded_df = exhaustivity_df.join(
        coc_dx_uids_df,
        on="CATEGORY_OPTION_COMBO",
        how="inner"  # Only keep COCs that have expected DX_UIDs
    )
    
    # Log warnings for COCs without expected DX_UIDs (only once)
    cocs_in_exhaustivity = set(exhaustivity_df["CATEGORY_OPTION_COMBO"].unique().to_list())
    cocs_with_mappings = set(expected_dx_uids_by_coc.keys())
    missing_cocs = cocs_in_exhaustivity - cocs_with_mappings
    if missing_cocs:
        current_run.log_warning(
            f"No expected DX_UIDs found for {len(missing_cocs)} COC(s) in exhaustivity data, "
            f"skipping expansion for these COCs."
        )
    
    # Format for DHIS2 import using polars
    # Each row represents an exhaustivity value for a (PERIOD, DX_UID, ORG_UNIT) combination
    df_final = expanded_df.with_columns([
        pl.lit("DATA_ELEMENT").alias("DATA_TYPE"),
        pl.col("EXHAUSTIVITY_VALUE").cast(pl.Int64).alias("VALUE"),
        pl.lit(None).alias("ATTRIBUTE_OPTION_COMBO"),
        pl.lit(None).alias("RATE_TYPE"),
        pl.lit("AGGREGATED").alias("DOMAIN_TYPE"),
    ]).select([
        "DATA_TYPE",
        "DX_UID",
        "PERIOD",
        "ORG_UNIT",
        "VALUE",
        "ATTRIBUTE_OPTION_COMBO",
        "RATE_TYPE",
        "DOMAIN_TYPE",
    ])
    
    # Convert to pandas only at the end for DHIS2Pusher compatibility
    return df_final.to_pandas()


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
        config = load_configuration(config_path=pipeline_path / "config_files" / "push_config.json")
        prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")
        dhis2_client = connect_to_dhis2(connection_str=prs_conn, cache_dir=None)

        # Collect all dataset mappings from extracts that have TARGET_DATASET_UID
        # This includes both regular extracts and exhaustivity extracts
        dataset_mappings = []
        for extract in config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
            source_ds = extract.get("SOURCE_DATASET_UID")
            target_ds = extract.get("TARGET_DATASET_UID")
            extract_id = extract.get("EXTRACT_UID", "unknown")
            
            # For exhaustivity extracts, we only need TARGET_DATASET_UID (no source dataset sync needed)
            # For regular extracts, we need both SOURCE_DATASET_UID and TARGET_DATASET_UID
            if target_ds:
                if "exhaustivity" in extract_id.lower():
                    # Exhaustivity extracts: use the target dataset directly (no source to copy from)
                    # We still add it to the list but with source=None to indicate it's an exhaustivity dataset
                    dataset_mappings.append({
                        "source": None,  # No source dataset for exhaustivity
                        "target": target_ds,
                        "extract_id": extract_id
                    })
                elif source_ds:
                    # Regular extracts: copy org units from source to target
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
        for mapping in dataset_mappings:
            # Skip exhaustivity extracts that don't have a source dataset
            # (they will be handled separately if needed)
            if mapping["source"] is None:
                current_run.log_info(
                    f"Skipping org units sync for exhaustivity extract '{mapping['extract_id']}': "
                    f"no source dataset specified (target: {mapping['target']})"
                )
                continue
                
            current_run.log_info(
                f"Syncing org units for '{mapping['extract_id']}': "
                f"{mapping['source']} -> {mapping['target']}"
            )
            push_dataset_org_units(
                dhis2_client=dhis2_client,
                source_dataset_id=mapping["source"],
                target_dataset_id=mapping["target"],
                dry_run=config["SETTINGS"].get("DRY_RUN", True),
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
):
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return True

    current_run.log_info("Starting data push.")

    # setup
    configure_logging(logs_path=pipeline_path / "logs" / "push", task_name="push_data")
    config = load_configuration(config_path=pipeline_path / "config_files" / "push_config.json")
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
            # Read extract
            extract_id, extract_file_path = split_on_pipe(next_period)
            extract_path = Path(extract_file_path)
            # Read with polars, convert to pandas only when needed
            extract_data_pl = pl.read_parquet(extract_path)
            extract_data = extract_data_pl.to_pandas()  # Convert to pandas for apply_analytics_data_element_extract_config
            short_path = f"{extract_path.parent.name}/{extract_path.name}"
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
                # Convert back to polars for processing
                exhaustivity_df_pl = pl.from_pandas(extract_data)
                
                # Load extract config to get expected DX_UIDs
                from utils import load_configuration
                extract_config_full = load_configuration(config_path=pipeline_path / "config_files" / "extract_config.json")
                extract_config_item = next(
                    (e for e in extract_config_full.get("DATA_ELEMENTS", {}).get("EXTRACTS", []) 
                     if e.get("EXTRACT_UID") == extract_id), 
                    None
                )
                
                # Read original data if available (for fallback)
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
                df_final = format_for_exhaustivity_import(
                    exhaustivity_df_pl.select(["PERIOD", "CATEGORY_OPTION_COMBO", "ORG_UNIT", "EXHAUSTIVITY_VALUE"]),
                    original_data=original_data,
                    pipeline_path=pipeline_path,
                    extract_id=extract_id,
                    extract_config_item=extract_config_item,
                )
                
                # Apply mapping and push data
                cfg_list, mapper_func = dispatch_map["DATA_ELEMENT"]
                extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})
                df_mapped = mapper_func(df=df_final, extract_config=extract_config)
                pusher.push_data(df_data=df_mapped)
            
            else:
                # Handle other data types (DATA_ELEMENT, REPORTING_RATE, INDICATOR)
                # Determine data type from DATA_TYPE column
                if "DATA_TYPE" not in extract_data.columns:
                    current_run.log_warning(f"No DATA_TYPE column in extract: {short_path}. Skipping.")
                    push_queue.dequeue()  # remove unknown item
                    continue
                
                data_type = extract_data["DATA_TYPE"].unique()[0]
                
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
            current_run.log_error(f"Fatal error for extract {extract_id} ({short_path}), stopping push process.")
            logging.error(f"Fatal error for extract {extract_id} ({short_path}): {e!s}")
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


def apply_analytics_data_element_extract_config(df: pd.DataFrame, extract_config: dict) -> pd.DataFrame:
    """Applies data element mappings to the extracted data.

    It also filters data elements based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract_config : dict
        This is a dictionary containing the extract mappings.

    Returns
    -------
    pd.DataFrame
        DataFrame with mapped data elements.
    """
    if len(extract_config) == 0:
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df

    extract_mappings = extract_config.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data element mappings.")
        return df

    # Convert to polars for processing
    df_pl = pl.from_pandas(df)

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
            df_uid = df_uid.filter(pl.col("CATEGORY_OPTION_COMBO").is_in(coc_keys))
            # Replace values using polars replace
            df_uid = df_uid.with_columns(
                pl.col("CATEGORY_OPTION_COMBO").replace(coc_mappings_clean)
            )

        if aoc_mappings:
            aoc_mappings = {k: v for k, v in aoc_mappings.items() if v is not None}  # Do not replace with None
            aoc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()}
            aoc_keys = list(aoc_mappings_clean.keys())
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
        # Return empty pandas DataFrame with same columns
        return pd.DataFrame(columns=df.columns)

    # Concatenate using polars
    df_filtered_pl = pl.concat(chunks)

    # Apply UID mappings using polars replace
    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered_pl = df_filtered_pl.with_columns(
            pl.col("DX_UID").replace(uid_mappings_clean)
        )

    # Convert back to pandas for compatibility
    df_filtered = df_filtered_pl.to_pandas()

    # Fill missing AOC (PRS default)
    df_filtered["ATTRIBUTE_OPTION_COMBO"] = df_filtered.loc[:, "ATTRIBUTE_OPTION_COMBO"].replace({None: "HllvX50cXC0"})

    return df_filtered


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

