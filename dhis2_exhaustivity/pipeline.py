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
    read_parquet_extract,
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

# exhaustivity calculation (cursor stuff here)
@parameter(
    code="exhaustivity_value",
    name="Exhaustivity Value",
    type=int,
    default=0,
    help="Computed exhaustivity value after extracting data (1 if all required fields are filled, else 0).",
)
def compute_and_log_exhaustivity(pipeline_path: Path, run_task: bool = True) -> int:
    """
    Computes exhaustivity based on extracted data after extraction is complete.

    Args:
        pipeline_path (Path): The root path for the pipeline.
        run_task (bool): Whether to run the computation.

    Returns:
        int: Exhaustivity value (1 or 0)
    """
    if not run_task:
        current_run.log_info("Exhaustivity calculation skipped.")
        return 0

    data_file = pipeline_path / "output" / "extract_data.parquet"
    if not data_file.exists():
        current_run.log_error(f"Extracted data file {data_file} not found for exhaustivity calculation.")
        return 0

    df = pd.read_parquet(data_file)
    value = compute_exhaustivity(df)
    current_run.log_info(f"Exhaustivity value computed: {value}")
    return value


# push data to target DHIS2
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
def dhis2_cmm_push(run_extract_data: bool, run_push_data: bool):
    """Extract data elements from the PRS DHIS2 instance.

    Compute the CMM (Consomation mensuelle moyenne) over a 6-month rolling window
     the results are then push back to PRS DHIS2 to the target CMM data elements.
    NOTE: Theres no need for organisation unit alignment between source and target.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_cmm_push"

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


@dhis2_cmm_push.task
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
        months_lag = extract_config["SETTINGS"].get("NUMBER_MONTHS_WINDOW", 3)  # default 3 months window
        if not extract_config["SETTINGS"].get("STARTDATE"):
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = extract_config["SETTINGS"].get("STARTDATE")
        if not extract_config["SETTINGS"].get("ENDDATE"):
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month.
        else:
            end = extract_config["SETTINGS"].get("ENDDATE")
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

    # Adjust the start date to consider the 6 months windows necessary for computation
    cmm_window = extract_config["SETTINGS"].get("CMM_MONTHS_WINDOW", 6)
    start_cmm = (datetime.strptime(start, "%Y%m") - relativedelta(months=cmm_window)).strftime(
        "%Y%m"
    )  # consider 6 months in the past to compute current CMM

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
        f"Download MODE: {extract_config['SETTINGS']['MODE']} from: {start_cmm} ({cmm_window} cmm window) to {end}"
    )

    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        extract_periods=get_periods(start_cmm, end),
    )

    # Collect the downloaded files and compute CMM.
    target_extract = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])[0]
    compute_cmm_and_queue(
        pipeline_path=pipeline_path,
        extract_id=target_extract.get("EXTRACT_UID"),
        data_elements=target_extract.get("UIDS", []),
        cmm_window=cmm_window,
        cmm_periods=get_periods(start, end),
        push_queue=push_queue,
    )


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    extract_periods: list[str],
):
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    current_run.log_info("Starting data element extracts.")
    source_datasets = get_datasets(dhis2_extractor.dhis2_client)

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

        if not dataset_uid:
            current_run.log_warning(f"No dataset id defined for extract: {extract_id}, extract skipped.")
            continue

        # get org units from the dataset directly
        source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_uid]))
        org_units = source_dataset["organisation_units"].explode().to_list()

        current_run.log_info(
            f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
            f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
            f"(dataset: {source_dataset['name'][0]})."
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


def check_validate_and_save_extract(data_elements: list[str], repo_path: Path, period: str, output: Path) -> bool:
    """Validate and save extracted data elements for a given period from an existing repository.

    Parameters
    ----------
    data_elements : list[str]
        List of data element UIDs to filter.
    repo_path : Path
        Path to the repository containing the data files.
    period : str
        The period string (e.g., '202301') to locate the data file.
    output : Path
        Output directory where the filtered data will be saved.

    Returns
    -------
    bool
        True if data was found and saved, False otherwise.
    """
    if not (repo_path / f"data_{period}.parquet").is_file():
        return False
    df = read_parquet_extract(repo_path / f"data_{period}.parquet")
    output.mkdir(parents=True, exist_ok=True)
    df_selection = df[df["DX_UID"].isin(data_elements)].copy()
    if df_selection.empty:
        current_run.log_info(f"No data found for period {period} in the selected data elements. Warning.")
        return False
    num_found = df_selection["DX_UID"].nunique()
    df_selection.to_parquet(output / f"data_{period}.parquet", index=False)
    current_run.log_info(
        f"Extract for period {period} created from existing data. "
        f"Data elements found: {num_found}/{len(data_elements)}."
    )
    return True


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


def compute_cmm_and_queue(
    pipeline_path: Path,
    extract_id: str,
    data_elements: list[str],
    cmm_window: int,
    cmm_periods: list[str],
    push_queue: Queue,
) -> None:
    """Computes CMM from extracted data and enqueues the result for pushing.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    extract_id : str
        Identifier for the data extract.
    data_elements : list[str]
        List of data element UIDs to consider for CMM computation.
    cmm_window : int
        Number of months to consider for CMM computation.
    cmm_periods : list[str]
        List of periods to process.
    push_queue : Queue
        Queue to enqueue the CMM result file for pushing.
    """
    extracts_folder = pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}"
    output_dir = pipeline_path / "data" / "cmm"
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        for period in cmm_periods:
            current_run.log_info(f"Computing CMM for period: {period} using a {cmm_window}-month window.")
            start_cmm = (datetime.strptime(period, "%Y%m") - relativedelta(months=cmm_window)).strftime("%Y%m")
            end_cmm = (datetime.strptime(period, "%Y%m") - relativedelta(months=1)).strftime("%Y%m")
            target_periods = get_periods(start_cmm, end_cmm)

            files_to_read = {
                p: (extracts_folder / f"data_{p}.parquet") if (extracts_folder / f"data_{p}.parquet").exists() else None
                for p in target_periods
            }
            missing_extracts = [k for k, v in files_to_read.items() if not v]

            if len(missing_extracts) == len(target_periods):
                raise FileNotFoundError(f"No parquet files found for {target_periods} in {extracts_folder}")

            if missing_extracts:
                current_run.log_warning(
                    f"Expected {len(target_periods)} parquet files for CMM computation period {period}, "
                    f"but missing files for periods: {missing_extracts}."
                )

            try:
                df = pl.concat([pl.read_parquet(f) for f in files_to_read.values() if f is not None])
            except Exception as e:
                raise RuntimeError(f"Error reading parquet files for CMM computation: {e!s}") from e

            # PRS specific filter (!)
            df = df.filter((pl.col("CATEGORY_OPTION_COMBO") == "cjeG5HSWRIU") & (pl.col("DX_UID").is_in(data_elements)))
            df = df.with_columns(pl.col("VALUE").cast(pl.Float64))
            df_avg = df.group_by(["DX_UID", "ORG_UNIT", "CATEGORY_OPTION_COMBO"]).agg(
                [pl.col("VALUE").mean().alias("AVG_VALUE")]
            )

            # Format for DHIS2 import
            df_final = format_for_import(df_avg, period)

            try:
                save_to_parquet(
                    data=df_final.to_pandas(),  # convert to pandas for saving ¯\\_(ツ)_/¯
                    filename=output_dir / f"cmm_{period}.parquet",
                )
                push_queue.enqueue(f"{extract_id}|{output_dir / f'cmm_{period}.parquet'}")
            except Exception as e:
                logging.error(f"CMM saving error: {e!s}")
                current_run.log_error(f"Error saving CMM parquet file for period {period}.")
    finally:
        current_run.log_info("CMM computation finished.")
        push_queue.enqueue("FINISH")


def format_for_import(df: pl.DataFrame, period: str) -> pd.DataFrame:
    """Formats the aggregated CMM data for DHIS2 import by adding required columns and renaming fields.

    Parameters
    ----------
    df : pl.DataFrame
        The aggregated data as a Polars DataFrame.
    period : str
        The period string to assign to the output data.

    Returns
    -------
    pd.DataFrame
        A DataFrame formatted for DHIS2 import.
    """
    df_final = df.clone()  # avoid modifying the original
    return (
        df_final
        # Add new columns
        .with_columns(
            [
                pl.lit("DATA_ELEMENT").alias("DATA_TYPE"),
                pl.lit(None).alias("ATTRIBUTE_OPTION_COMBO"),
                pl.lit(None).alias("RATE_TYPE"),
                pl.lit("AGGREGATED").alias("DOMAIN_TYPE"),
                pl.lit(period).alias("PERIOD"),
            ]
        ).rename({"AVG_VALUE": "VALUE"})
    )


@dhis2_cmm_push.task
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
        # Sync OU: PRS C- SIGL FOSA-Import SNIS SANRU (wMCnDAQfGZN) -> PRS CMM - OpenHexa (uuoQdHIDMTB)
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")
        config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

        push_dataset_org_units(
            dhis2_client=connect_to_dhis2(connection_str=prs_conn, cache_dir=None),
            source_dataset_id="wMCnDAQfGZN",  # PRS C- SIGL FOSA-Import SNIS
            target_dataset_id="uuoQdHIDMTB",  # PRS CMM - OpenHexa
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


@dhis2_cmm_push.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
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
            extract_data = read_parquet_extract(parquet_file=extract_path)
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
            # df_mapped[[""]].drop_duplicates().head()
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

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(f"Applying data element mappings for extract: {extract_config.get('EXTRACT_UID')}.")
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection (filters by COC and AOC if provided)
        df_uid = df[df["DX_UID"] == uid].copy()
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            df_uid = df_uid[df_uid["CATEGORY_OPTION_COMBO"].isin(coc_mappings_clean.keys())]
            df_uid["CATEGORY_OPTION_COMBO"] = df_uid.loc[:, "CATEGORY_OPTION_COMBO"].replace(coc_mappings_clean)

        if aoc_mappings:
            aoc_mappings = {k: v for k, v in aoc_mappings.items() if v is not None}  # Do not replace with None
            aoc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()}
            df_uid = df_uid[df_uid["ATTRIBUTE_OPTION_COMBO"].isin(aoc_mappings_clean.keys())]
            df_uid["ATTRIBUTE_OPTION_COMBO"] = df_uid.loc[:, "ATTRIBUTE_OPTION_COMBO"].replace(aoc_mappings_clean)

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logging.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pd.DataFrame(columns=df.columns)

    df_filtered = pd.concat(chunks, ignore_index=True)

    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered["DX_UID"] = df_filtered.loc[:, "DX_UID"].replace(uid_mappings_clean)

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
    dhis2_cmm_push()
