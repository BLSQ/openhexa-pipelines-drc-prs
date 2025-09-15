import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from dateutil.relativedelta import relativedelta
from db_queue import Queue
from dhis2_extract_handlers import DHIS2Extractor
from dhis2_org_unit_aligner import DHIS2PyramidAligner
from dhis2_push_handlers import DHIS2Pusher
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
    select_descendants,
)


@pipeline("dhis2_dataset_sync", timeout=28800)  # 8 hours
@parameter(
    code="run_ou_sync",
    name="Run org units sync (recommended)",
    type=bool,
    default=True,
    help="Run organisation units alignment between source and target DHIS2.",
    required=True,
)
@parameter(
    code="run_extract_data",
    name="Extract data",
    type=bool,
    default=True,
    help="Extract data elements from source DHIS2.",
)
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
def dhis2_dataset_sync(run_ou_sync: bool = True, run_extract_data: bool = True, run_push_data: bool = True):
    """Main pipeline function for DHIS2 dataset synchronization.

    Parameters
    ----------
    run_ou_sync : bool
        If True, runs the organisation units sync task.
    run_extract_data : bool, optional
        If True, runs the data extraction task (default is True).
    run_push_data : bool, optional
        If True, runs the data push task (default is True).

    Raises
    ------
    Exception
        If an error occurs during the pipeline execution.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_dataset_sync"

    try:
        pyramid_ready = sync_organisation_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,
        )

        datasets_ready = sync_dataset_organisation_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,  # only run if OU sync was run
            wait=pyramid_ready,
        )

        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
            wait=datasets_ready,
        )

        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=datasets_ready,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_dataset_sync.task
def sync_organisation_units(
    pipeline_path: Path,
    run_task: bool = True,
) -> bool:
    """Pyramid extraction task.

    extracts and saves a pyramid dataframe for all levels (could be set via config in the future)

    Returns
    -------
    bool
        True: This is just a dummy flag to indicate the pyramid task is done.
    """
    if not run_task:
        current_run.log_info("Organisation units sync task skipped.")
        return True

    try:
        configure_logging(logs_path=pipeline_path / "logs" / "org_units", task_name="org_units_sync")
        # load configuration
        config_extract = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        config_sync = load_configuration(config_path=pipeline_path / "configuration" / "sync_config.json")
        config_push = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        source_conn = config_extract["SETTINGS"].get("SOURCE_DHIS2_CONNECTION")
        target_conn = config_push["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

        if not source_conn or not target_conn:
            raise ValueError("Missing DHIS2 connection details.")

        # Connect to DHIS2 instances
        # No cache for org units sync
        source_dhis2 = connect_to_dhis2(connection_str=source_conn, cache_dir=None)
        target_dhis2 = connect_to_dhis2(connection_str=target_conn, cache_dir=None)

        align_org_units(
            pipeline_path=pipeline_path,
            source_dhis2=source_dhis2,
            target_dhis2=target_dhis2,
            source_org_units_selection=config_sync["ORG_UNITS"]["SELECTION"].get("UIDS", []),
            include_children=config_sync["ORG_UNITS"]["SELECTION"].get("INCLUDE_CHILDREN", True),
            limit_level=config_sync["ORG_UNITS"]["SELECTION"].get("LIMIT_LEVEL", None),
            dry_run=config_push["SETTINGS"].get("DRY_RUN", True),
        )

    except Exception as e:
        raise Exception(f"Error during pyramid sync: {e}") from e
    return True


def align_org_units(
    pipeline_path: Path,
    source_dhis2: DHIS2,
    target_dhis2: DHIS2,
    source_org_units_selection: list,
    include_children: bool,
    limit_level: int | None,
    dry_run: bool = True,
) -> None:
    """Aligns organisation units between source and target DHIS2 connections.

    Parameters
    ----------
    pipeline_path : Path
        Path to the pipeline directory.
    source_dhis2 : DHIS2
        Client source DHIS2 instance.
    target_dhis2 : DHIS2
        Client target DHIS2 instance.
    source_org_units_selection : list
        List of organisation units to select from the source.
    include_children : bool
        Whether to include child organisation units.
    limit_level : int
        Select the source pyramid under this limit level.
    dry_run : bool, optional
        If True, performs a dry run without making changes (default is True).
    """
    extract_pyramid(
        dhis2_client=source_dhis2,
        limit_level=limit_level,
        org_units_selection=source_org_units_selection,
        include_children=include_children,
        output_dir=pipeline_path / "data" / "pyramid",
        filename="pyramid_data.parquet",
    )

    DHIS2PyramidAligner().align_to(
        target_dhis2=target_dhis2,
        source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
        dry_run=dry_run,
    )


@dhis2_dataset_sync.task
def sync_dataset_organisation_units(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
) -> bool:
    """Sync organisation units of datasets between source and target DHIS2 instances.

    WARNING: This step should only be executed AFTER organisation units alignment, we assume
    the organisation units have been properly aligned between source and target.

    Returns
    -------
    bool
        True: This is just a dummy flag to indicate the dataset OU sync task is done.
    """
    if not run_task:
        current_run.log_info("Dataset organisation units sync task skipped.")
        return True

    try:
        current_run.log_info("Starting dataset organisation units sync.")
        configure_logging(logs_path=pipeline_path / "logs" / "dataset_org_units", task_name="dataset_org_units_sync")

        # load configuration
        config_extract = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        config_sync = load_configuration(config_path=pipeline_path / "configuration" / "sync_config.json")
        config_push = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        source_conn = config_extract["SETTINGS"].get("SOURCE_DHIS2_CONNECTION")
        target_conn = config_push["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

        if not source_conn or not target_conn:
            raise ValueError("Missing DHIS2 connection details.")

        # Connect to DHIS2 instances
        # No cache for org units sync
        source_dhis2 = connect_to_dhis2(connection_str=source_conn, cache_dir=None)
        target_dhis2 = connect_to_dhis2(connection_str=target_conn, cache_dir=None)

        align_dataset_org_units(
            source_dhis2=source_dhis2,
            target_dhis2=target_dhis2,
            dataset_mappings=config_sync.get("DATASETS", {}),
            source_pyramid_path=pipeline_path / "data" / "pyramid" / "pyramid_data.parquet",
            dry_run=config_push["SETTINGS"].get("DRY_RUN", True),
        )
    except Exception as e:
        raise Exception(f"Error during dataset organisation units sync: {e}") from e
    return True


def align_dataset_org_units(
    source_dhis2: DHIS2,
    target_dhis2: DHIS2,
    dataset_mappings: dict,
    source_pyramid_path: Path,
    dry_run: bool = True,
) -> None:
    """Aligns organisation units of datasets between source and target DHIS2 connections."""
    if len(dataset_mappings) == 0:
        current_run.log_warning("No dataset IDs provided for sync. Dataset organisation units task skipped.")
        return

    logger = logging.getLogger(__name__)

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    source_pyramid = read_parquet_extract(source_pyramid_path)
    msg = f"Loading source pyramid for dataset sync. Shape: {source_pyramid.shape}"
    current_run.log_debug(msg)
    logger.info(msg)

    try:
        # Retrieve datasets metadata from source & target
        source_datasets = get_datasets(source_dhis2)
        target_datasets = get_datasets(target_dhis2)
    except Exception as e:
        current_run.log_error(f"Failed to fetch datasets: {e}")
        return

    # Select only datasets to sync
    source_datasets_selection = source_datasets.filter(pl.col("id").is_in(dataset_mappings.keys()))
    msg = f"Running updates for {source_datasets_selection.shape[0]} datasets."
    current_run.log_info(msg)
    logger.info(msg)  # should move to a parameter log

    # Compare source vs target datasets and update org units list if needed
    error_count = 0
    update_count = 0
    for source_ds in source_datasets_selection.iter_rows(named=True):
        current_run.log_debug(f"Processing dataset: {source_ds['name']} ({source_ds['id']})")
        source_ds_ou = source_ds["organisation_units"]

        # Use the aligned/filtered org units from the source pyramid to validate the OU to be pushed
        valid_ous = set(source_pyramid.id)
        source_ds_ou = [ou for ou in source_ds_ou if ou in valid_ous]

        target_ds = target_datasets.filter(pl.col("id") == dataset_mappings[source_ds["id"]])
        if target_ds.is_empty():
            current_run.log_warning(f"Dataset id: {dataset_mappings[source_ds['id']]} not found in DHIS2 target.")
            continue

        target_ds_ou = target_ds["organisation_units"].explode().to_list()
        if set(source_ds_ou) != set(target_ds_ou):
            update_count = update_count + 1
            msg = (
                f"Updating {source_ds['name']} ({source_ds['id']}) OU count: {len(source_ds_ou)} "
                f"> target {target_ds['name'].item()} ({target_ds['id'].item()}) OU count: {len(target_ds_ou)}"
            )
            current_run.log_info(msg)
            logger.info(msg)
            update_response = push_dataset_org_units(
                dhis2_client=target_dhis2,
                dataset_id=target_ds["id"].item(),
                new_org_units=source_ds_ou,
                dry_run=dry_run,  # dry_run=True -> No changes applied in DHIS2
            )
            if "error" in update_response:
                error_count = error_count + 1
                logger.error(f"Error updating dataset {source_ds['name']} org units: {update_response['error']}")
            else:
                msg = (
                    f"Dataset {target_ds['name'].item()} ({target_ds['id'].item()}) org units updated."
                    f"OU count:{len(source_ds_ou)}"
                )
                current_run.log_info(msg)
                logger.info(msg)

    if error_count > 0:
        current_run.log_warning(
            f"{error_count} errors occurred during dataset org units update. Check logs for details."
        )
    if update_count == 0:
        current_run.log_info("No updates applied for dataset organisation units. Source and target datasets aligned.")


def extract_pyramid(
    dhis2_client: DHIS2,
    limit_level: int,
    org_units_selection: list[str],
    include_children: bool,
    output_dir: Path,
    filename: str,
) -> None:
    """Extracts the source DHIS2 pyramid data and saves it as a Parquet file."""
    current_run.log_info("Retrieving source DHIS2 pyramid data")

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        # Retrieve all available levels..
        # levels = pl.DataFrame(dhis2_client.meta.organisation_unit_levels())
        # org_levels = levels.select("level").unique().sort(by="level").to_series().to_list()

        org_units = dhis2_client.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        org_units = pd.DataFrame(org_units)
        current_run.log_info(f"Organisation units extracted: {len(org_units.id.unique())}")

        if limit_level is None:
            current_run.log_info("OU limit level not set, selecting entire pyramid")
        else:
            org_units = org_units[org_units.level <= limit_level]  # filter by limit_level

        if len(org_units_selection) > 0:
            # Add parent_id column for easier filtering
            org_units["parent_id"] = org_units["parent"].apply(lambda x: x.get("id") if isinstance(x, dict) else None)
            org_units = select_descendants(org_units, org_units_selection)
            org_units = org_units.drop(columns=["parent_id"])
            if not include_children:
                org_units = org_units[org_units.id.isin(org_units_selection)]

        current_run.log_info(f"Selected organisation units: {len(org_units.id.unique())}.")

        # Save as Parquet
        pyramid_fname = output_dir / filename
        save_to_parquet(data=org_units, filename=pyramid_fname)
        current_run.log_info(f"DHIS2 pyramid data saved: {pyramid_fname}")

    except Exception as e:
        raise Exception(f"Error while extracting DHIS2 Pyramid: {e}") from e


@dhis2_dataset_sync.task
def extract_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
):
    """Extracts data elements from the source DHIS2 instance and saves them in parquet format."""
    if not run_task:
        current_run.log_info("Data elements extraction task skipped.")
        return

    current_run.log_info("Data elements extraction task started.")

    # load configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")

    # connect to source DHIS2 instance
    # No cache for data extraction
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    source_pyramid = read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet")

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)
    # push_queue.reset()

    try:
        months_lag = extract_config["SETTINGS"].get("NUMBER_MONTHS_WINDOW", 3)  # default 3 months window
        if not extract_config["SETTINGS"]["STARTDATE"]:
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")
        else:
            start = extract_config["SETTINGS"]["STARTDATE"]
        if not extract_config["SETTINGS"]["ENDDATE"]:
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")  # go back 1 month.
        else:
            end = extract_config["SETTINGS"]["ENDDATE"]
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e}") from e

    try:
        download_settings = extract_config["SETTINGS"].get("MODE", None)
        if download_settings is None:
            download_settings = "DOWNLOAD_REPLACE"
            current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

        # limits
        dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
        dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

        # Get periods
        start_period = period_from_string(start)
        end_period = period_from_string(end)
        extract_periods = (
            [str(p) for p in start_period.get_range(end_period)]
            if str(start_period) < str(end_period)
            else [str(start_period)]
        )

        # Setup extractor
        dhis2_extractor = DHIS2Extractor(dhis2_client=dhis2_client, download_mode=download_settings)
        current_run.log_info(f"Download MODE: {extract_config['SETTINGS']['MODE']} from: {start} to {end}")

        # Data Elements
        data_element_extracts = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
        if len(data_element_extracts) == 0:
            current_run.log_info("No data elements to extract.")
            return

        # loop over the available extract configurations
        for idx, extract in enumerate(data_element_extracts):
            org_units_level = extract.get("ORG_UNITS_LEVEL", None)
            extract_id = extract.get("EXTRACT_UID")
            data_element_uids = extract.get("UIDS", [])

            if org_units_level is None:
                current_run.log_warning(f"No 'ORG_UNITS_LEVEL' found for extract position : {idx}. Skipped.")
                continue

            # get org units from the filtered pyramid
            org_units = source_pyramid[source_pyramid["level"] == org_units_level]["id"].to_list()
            current_run.log_info(
                f"Starting extract {idx + 1} (ID: {extract_id}) "
                f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
                f"(level {org_units_level})."
            )

            # TODO: Handle extract frequency if needed in the future
            # extract_periods = Use start-end to compute all periods based on frequency

            # run data elements extraction per period
            if extract_id is None:
                current_run.log_warning(f"No 'EXTRACT_UID' found for extract position : {idx + 1}. This is required.")
                continue

            for period in extract_periods:
                extract_path = dhis2_extractor.data_elements.download_period(
                    data_elements=data_element_uids,
                    org_units=org_units,
                    period=period,
                    output_dir=pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}",
                )
                if extract_path is not None:
                    push_queue.enqueue(f"{extract_id}|{extract_path}")

            # We can continue adding other data extractions (indicators, reporting rates, events, etc..)

        current_run.log_info("Extract process finished.")
    except Exception as e:
        raise Exception(f"Extract task error : {e}") from e
    finally:
        push_queue.enqueue("FINISH")


@dhis2_dataset_sync.task
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
    try:
        while True:
            next_period = push_queue.peek()  # Don't remove it yet, just take a look at the next period
            if next_period == "FINISH":
                push_queue.dequeue()  # remove FINISH marker
                break

            if not next_period:
                current_run.log_info("Push data process: waiting for updates")
                time.sleep(60 * int(push_wait))  # wait for <push_wait> minutes
                continue

            try:
                extract_id, extract_path = split_on_pipe(next_period)
                extract_data = read_parquet_extract(parquet_file=extract_path)
            except Exception as e:
                current_run.log_warning(f"Error while reading the extracts file: {extract_path}. Error: {e}")
                continue

            extract = next((e for e in config["DATA_ELEMENTS"]["EXTRACTS"] if e["EXTRACT_UID"] == extract_id), {})

            # NOTE: The mapping MUST BE pipeline specific (this is Non-generic)
            df_mapped = apply_data_element_extract_config(df=extract_data, extract=extract)

            try:
                current_run.log_info(f"Push extract {extract_id if extract_id else ''}: {extract_path}.")
                pusher.data_elements.push_data(df_data=df_mapped)

                # The process finished, so we remove the period from the queue
                _ = push_queue.dequeue()
                current_run.log_info(f"Extracts pushed for extract {extract_id}: {extract_path}.")
            except Exception as e:
                current_run.log_warning(f"Error while pushing data for extract {extract_id} :{extract_path}. {e}")
                continue

    except Exception as e:
        raise Exception(f"Error connecting to target DHIS2: {e}") from e


def apply_data_element_extract_config(df: pd.DataFrame, extract: dict) -> pd.DataFrame:
    """Applies data element mappings to the extracted data.

    It also filters data elements based on category option combo (COC) if specified in the extract configuration.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract : dict
        This is a dictionary containing the extract mappings.

    Returns
    -------
    pd.DataFrame
        DataFrame with mapped data elements.
    """
    if len(extract) == 0:
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df

    extract_mappings = extract.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data element mappings.")
        return df

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(f"Applying data element mappings for extract: {extract.get('EXTRACT_UID')}.")
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection
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

    return df_filtered


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


def push_dataset_org_units(
    dhis2_client: DHIS2, dataset_id: str, new_org_units: list[str], dry_run: bool = True
) -> dict:
    """Updates the organisation units of a DHIS2 dataset.

    Parameters
    ----------
    dhis2_client : DHIS2
        DHIS2 client for the target instance.
    dataset_id : str
        The ID of the dataset to update.
    new_org_units : list[str]
        List of organisation unit IDs to assign to the dataset.
    dry_run : bool, optional
        If True, performs a dry run without making changes (default is True).

    Returns
    -------
    dict
        The response from the DHIS2 API, or an error payload.
    """
    endpoint = "dataSets"
    url = f"{dhis2_client.api.url}/{endpoint}/{dataset_id}"

    # Step 1: GET current dataset
    dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    if "error" in dataset_payload:
        return dataset_payload

    # Step 2: Update organisationUnits
    dataset_payload["organisationUnits"] = [{"id": ou_id} for ou_id in new_org_units]

    # Step 3: PUT updated dataset
    update_response = dhis2_request(
        dhis2_client.api.session, "put", url, json=dataset_payload, params={"dryRun": str(dry_run).lower()}
    )
    if "error" in update_response:
        return update_response

    return update_response


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
    dhis2_dataset_sync()
