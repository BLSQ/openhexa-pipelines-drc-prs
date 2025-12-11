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

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SAN-123
#   -https://bluesquare.atlassian.net/browse/SAN-124
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs

# extract data from source DHIS2
@pipeline("dhis2_exhaustivity")
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2.",
)

# push data to target DHIS2
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
def dhis2_exhaustivity(run_extract_data: bool, run_push_data: bool):
    """Extract data elements from the PRS DHIS2 instance.

    Compute the exhaustivity value based on required columns completeness.
    The results are then pushed back to PRS DHIS2 to the target exhaustivity data elements.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"

    try:
        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
        )

        sync_ready = update_dataset_org_units(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
        )

        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=sync_ready,
        )

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

    # Collect the downloaded files and compute exhaustivity for all extracts.
    # Use the same date range as extraction (start to end)
    for target_extract in extract_config["DATA_ELEMENTS"].get("EXTRACTS", []):
        compute_exhaustivity_and_queue(
            pipeline_path=pipeline_path,
            extract_id=target_extract.get("EXTRACT_UID"),
            exhaustivity_periods=get_periods(start, end),
            push_queue=push_queue,
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

        # run data elements extraction per period
        for period in extract_periods:
            try:
                dhis2_extractor.analytics_data_elements.download_period(
                    data_elements=data_element_uids,
                    org_units=org_units,
                    period=period,
                    output_dir=pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}",
                )

            except Exception as e:
                current_run.log_warning(
                    f"Extract {extract_id} download failed for period {period}, skipping to next extract."
                )
                logging.error(f"Extract {extract_id} - period {period} error: {e!s}")
                break  # skip to next extract

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
    extracts_folder = pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}"
    output_dir = pipeline_path / "data" / "exhaustivity"
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        for period in exhaustivity_periods:
            current_run.log_info(f"Computing exhaustivity for period: {period}.")
            
            # For exhaustivity, we need to read the data for the current period
            period_file = extracts_folder / f"data_{period}.parquet"
            
            if not period_file.exists():
                current_run.log_warning(f"Parquet file not found for period {period} in {extracts_folder}")
                # If no data at all for this period, create entries with exhaustivity = 0 for all expected DX_UIDs
                # Load extract config to get expected data elements
                from utils import load_configuration
                extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
                
                # Find the extract configuration
                extract_config_item = None
                for extract_item in extract_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
                    if extract_item.get("EXTRACT_UID") == extract_id:
                        extract_config_item = extract_item
                        break
                
                if extract_config_item and extract_config_item.get("UIDS"):
                    expected_dx_uids = extract_config_item.get("UIDS", [])
                    # Create entries with exhaustivity = 0 for all DX_UIDs (no ORG_UNIT in this case)
                    missing_data_df = pl.DataFrame({
                        "PERIOD": [period] * len(expected_dx_uids),
                        "DX_UID": expected_dx_uids,
                        "ORG_UNIT": [None] * len(expected_dx_uids),  # No org unit when no data
                        "EXHAUSTIVITY_VALUE": [0] * len(expected_dx_uids),
                    })
                    
                    # Format for DHIS2 import
                    df_final = format_for_exhaustivity_import(missing_data_df)
                    
                    try:
                        save_to_parquet(
                            data=df_final,
                            filename=output_dir / f"exhaustivity_{period}.parquet",
                        )
                        push_queue.enqueue(f"{extract_id}|{output_dir / f'exhaustivity_{period}.parquet'}")
                        current_run.log_info(f"Created exhaustivity entries with value 0 for {len(expected_dx_uids)} DX_UIDs (no data for period {period})")
                    except Exception as e:
                        logging.error(f"Exhaustivity saving error: {e!s}")
                        current_run.log_error(f"Error saving exhaustivity parquet file for period {period}.")
                else:
                    current_run.log_warning(f"No expected DX_UIDs found in config for extract {extract_id}, skipping period {period}")
                continue

            # Get expected DX_UIDs and ORG_UNITs from extract configuration
            # Load extract config to get expected data elements
            from utils import load_configuration
            extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
            
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
                # Get expected org units from the extracted data (all unique ORG_UNITs across all periods)
                # This ensures we include all org units that have data for any period
                try:
                    # Read all parquet files to get all org units
                    all_period_files = list(extracts_folder.glob("data_*.parquet"))
                    if all_period_files:
                        all_data = pl.concat([pl.read_parquet(f) for f in all_period_files])
                        expected_org_units = all_data["ORG_UNIT"].unique().to_list()
                except Exception as e:
                    current_run.log_warning(f"Could not determine expected org units: {e}")
                    expected_org_units = None
            
            # Compute exhaustivity values for this period (returns DataFrame with PERIOD, DX_UID, ORG_UNIT, EXHAUSTIVITY_VALUE)
            exhaustivity_df = compute_exhaustivity(
                pipeline_path=pipeline_path,
                extract_id=extract_id,
                periods=[period],
                expected_dx_uids=expected_dx_uids,
                expected_org_units=expected_org_units,
            )

            # Filter for the current period (in case multiple periods were processed)
            period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
            
            if len(period_exhaustivity) == 0:
                current_run.log_warning(f"No exhaustivity data computed for period {period}")
                continue

            # Format for DHIS2 import
            df_final = format_for_exhaustivity_import(period_exhaustivity)

            try:
                save_to_parquet(
                    data=df_final,
                    filename=output_dir / f"exhaustivity_{period}.parquet",
                )
                push_queue.enqueue(f"{extract_id}|{output_dir / f'exhaustivity_{period}.parquet'}")
            except Exception as e:
                logging.error(f"Exhaustivity saving error: {e!s}")
                current_run.log_error(f"Error saving exhaustivity parquet file for period {period}.")
    finally:
        current_run.log_info("Exhaustivity computation finished.")
        push_queue.enqueue("FINISH")


def format_for_exhaustivity_import(exhaustivity_df: pl.DataFrame) -> pd.DataFrame:
    """Formats the exhaustivity DataFrame for DHIS2 import.

    Parameters
    ----------
    exhaustivity_df : pl.DataFrame
        DataFrame with columns: PERIOD, DX_UID, ORG_UNIT, EXHAUSTIVITY_VALUE

    Returns
    -------
    pd.DataFrame
        A DataFrame formatted for DHIS2 import with columns:
        DATA_TYPE, DX_UID, PERIOD, ORG_UNIT, VALUE, etc.
    """
    # Format for DHIS2 import using polars
    # Each row represents an exhaustivity value for a (PERIOD, DX_UID, ORG_UNIT) combination
    df_final = exhaustivity_df.with_columns([
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

        # Previously the source dataset has been sync with SNIS by pipeline dhis2_dataset_sync.
        # Sync OU: PRS C- SIGL FOSA-Import SNIS SANRU (wMCnDAQfGZN) -> PRS Exhaustivity - OpenHexa (uuoQdHIDMTB)
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")
        config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

        push_dataset_org_units(
            dhis2_client=connect_to_dhis2(connection_str=prs_conn, cache_dir=None),
            source_dataset_id="wMCnDAQfGZN",  # PRS C- SIGL FOSA-Import SNIS
            target_dataset_id="uuoQdHIDMTB",  # PRS Exhaustivity - OpenHexa
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
):
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return

    current_run.log_info("Starting data push.")

    # setup
    configure_logging(logs_path=pipeline_path / "logs" / "push", task_name="push_data")
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
            # Determine data type
            data_type = extract_data["DATA_TYPE"].unique()[0]

            current_run.log_info(f"Pushing data for extract {extract_id}: {short_path}.")
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
