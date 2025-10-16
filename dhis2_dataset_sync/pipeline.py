import logging
import time
from datetime import datetime
from itertools import product
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from d2d_library.db_queue import Queue
from d2d_library.dhis2_dataset_completion_handler import DatasetCompletionSync
from d2d_library.dhis2_extract_handlers import DHIS2Extractor
from d2d_library.dhis2_org_unit_aligner import DHIS2PyramidAligner
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
    select_descendants,
)


@pipeline("dhis2_dataset_sync", timeout=43200)  # 3600 * 12 hours
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
            run_task=run_ou_sync,  # only run if OU sync ran
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
        current_run.log_error(f"Failed to fetch datasets: {e!s}")
        return

    # Select only datasets to sync
    source_datasets_selection = source_datasets.filter(pl.col("id").is_in(dataset_mappings.keys()))

    # PRS project specific! (Dummy datasets mapping to push full pyramid)
    if dataset_mappings.get("FULL_PYRAMID"):
        # Retrieve all organisation units from the target DHIS2 and create a dummy
        # dataset mapping in the source datasets table with all OUS at level 5
        # Check this: https://rdc-prs.com/api/dataSets/dbf1uGX1XU3.json
        source_datasets_selection = handle_full_pyramid_mapping(
            target_dhis2=target_dhis2,
            source_ds_selection=source_datasets_selection,
        )

    # PRS project specific!, push the ZS to DS (only those ZS from Provinces 20/26 of interest).
    # Create a dummy ZS dataset in the "source datasets table" with all ZS OUS from the target DHIS2.
    # Check this: https://rdc-prs.com/api/dataSets/Om2WgL4TNEy.json
    if dataset_mappings.get("ZONES_SANTE"):
        source_datasets_selection = handle_zs_mapping(
            target_dhis2=target_dhis2,
            ds_id=dataset_mappings.get("ZONES_SANTE"),
            source_ds_selection=source_datasets_selection,
            source_pyramid=source_pyramid,
        )

    msg = f"Running updates for {source_datasets_selection.shape[0]} datasets."
    current_run.log_info(msg)
    logger.info(msg)

    # Compare source vs target datasets and update org units list if needed
    error_count = 0
    update_count = 0
    for source_ds in source_datasets_selection.iter_rows(named=True):
        current_run.log_debug(f"Processing dataset: {source_ds['name']} ({source_ds['id']})")
        source_ds_ou = source_ds["organisation_units"]

        if source_ds["id"] not in ["FULL_PYRAMID", "ZONES_SANTE"]:
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
                    f"Dataset {target_ds['name'].item()} ({target_ds['id'].item()}) org units updated. "
                    f"OU count: {len(source_ds_ou)}"
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
    configure_logging(logs_path=pipeline_path / "logs" / "extract", task_name="extract_data")

    # load configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")

    # connect to source DHIS2 instance (No cache for data extraction)
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    source_pyramid = read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet")

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

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

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    try:
        # Get periods
        start_period = period_from_string(start)
        end_period = period_from_string(end)
        extract_periods = (
            [str(p) for p in start_period.get_range(end_period)]
            if str(start_period) < str(end_period)
            else [str(start_period)]
        )
    except Exception as e:
        raise Exception(f"Error in start/end date configuration: {e!s}") from e

    download_settings = extract_config["SETTINGS"].get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

    # Setup extractor
    # See docs about return_existing_file impact.
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client, download_mode=download_settings, return_existing_file=False
    )
    current_run.log_info(f"Download MODE: {extract_config['SETTINGS']['MODE']} from: {start} to {end}")

    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        source_pyramid=source_pyramid,
        extract_periods=extract_periods,
        push_queue=push_queue,
    )


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
    target_dhis2 = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)
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
        dhis2_client=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
    )

    # Dataset completion syncer
    source_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    source_dhis2 = connect_to_dhis2(connection_str=source_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None)
    completion_syncer = DatasetCompletionSync(
        source_dhis2=source_dhis2, target_dhis2=target_dhis2, import_strategy=import_strategy, dry_run=dry_run
    )

    # Map data types to their respective mapping functions
    dispatch_map = {
        "DATA_ELEMENT": (config["DATA_ELEMENTS"]["EXTRACTS"], apply_data_element_extract_config),
        "REPORTING_RATE": (config["REPORTING_RATES"]["EXTRACTS"], apply_reporting_rates_extract_config),
        "INDICATOR": (config["INDICATORS"]["EXTRACTS"], apply_indicators_extract_config),
    }

    # loop over the queue
    while True:
        next_extract = push_queue.peek()
        if next_extract == "FINISH":
            push_queue.dequeue()  # remove marker if present
            break

        if not next_extract:
            current_run.log_info("Push data process: waiting for updates")
            time.sleep(60 * int(push_wait))
            continue

        try:
            # Read extract
            extract_id, extract_file_path = split_on_pipe(next_extract)
            extract_path = Path(extract_file_path)
            extract_data = read_parquet_extract(parquet_file=extract_path)
        except Exception as e:
            current_run.log_error(f"Failed to read extract from queue item: {next_extract}. Error: {e}")
            push_queue.dequeue()  # remove problematic item
            continue

        try:
            # Determine data type
            data_type = extract_data["DATA_TYPE"].unique()[0]
            period = extract_data["PERIOD"].unique()[0]

            # Set values of 'REPORTING_RATE' from '100' -> '1' (specific to DRC PRS project)
            mask = (extract_data.DATA_TYPE == "REPORTING_RATE") & (
                extract_data.RATE_TYPE.isin(["REPORTING_RATE", "REPORTING_RATE_ON_TIME"])
            )
            extract_data.loc[mask, "VALUE"] = extract_data.loc[mask, "VALUE"].apply(
                lambda x: str(int(float(x) / 100)) if pd.notna(x) else x
            )

            current_run.log_info(f"Pushing data for extract {extract_id}: {extract_path.name}.")
            if data_type not in dispatch_map:
                current_run.log_warning(f"Unknown DATA_TYPE '{data_type}' in extract: {extract_path.name}. Skipping.")
                push_queue.dequeue()  # remove unknown item
                continue

            # Get config and mapping function
            cfg_list, mapper_func = dispatch_map[data_type]
            extract_config = next((e for e in cfg_list if e["EXTRACT_UID"] == extract_id), {})

            # Apply mapping and push data
            df_mapped: pd.DataFrame = mapper_func(df=extract_data, extract_config=extract_config)
            # df_mapped[[""]].drop_duplicates().head()
            df_mapped = df_mapped.sort_values(by="ORG_UNIT")  # speed up DHIS2 processing
            pusher.push_data(df_data=df_mapped)

            # Success → dequeue
            push_queue.dequeue()
            current_run.log_info(f"Data push finished for extract: {extract_path.name}.")

            # Set dataset competion for all org units for this period
            handle_dataset_completion(
                completion_syncer,
                source_ds_id=extract_config.get("SOURCE_DATASET_UID"),
                target_ds_id=extract_config.get("TARGET_DATASET_UID"),
                period=period,
                org_units=df_mapped["ORG_UNIT"].unique(),
            )

        except Exception as e:
            current_run.log_error(f"Fatal error for extract {extract_id} ({extract_path.name}), stopping push process.")
            logging.error(f"Fatal error for extract {extract_id} ({extract_path.name}): {e!s}")
            raise  # crash on error


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    source_pyramid: pd.DataFrame,
    extract_periods: list[str],
    push_queue: Queue,
):
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    current_run.log_info("Starting data element extracts.")
    try:
        # loop over the available extract configurations
        for idx, extract in enumerate(data_element_extracts):
            extract_id = extract.get("EXTRACT_UID")
            org_units_level = extract.get("ORG_UNITS_LEVEL", None)
            data_element_uids = extract.get("UIDS", [])

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

            # get org units from the filtered pyramid
            org_units = source_pyramid[source_pyramid["level"] == org_units_level]["id"].to_list()
            current_run.log_info(
                f"Starting data elements extract ID: '{extract_id}' ({idx + 1}) "
                f"with {len(data_element_uids)} data elements across {len(org_units)} org units "
                f"(level {org_units_level})."
            )

            # run data elements extraction per period
            for period in extract_periods:
                try:
                    extract_path = dhis2_extractor.data_elements.download_period(
                        data_elements=data_element_uids,
                        org_units=org_units,
                        period=period,
                        output_dir=pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}",
                    )
                    if extract_path is not None:
                        push_queue.enqueue(f"{extract_id}|{extract_path}")

                except Exception as e:
                    current_run.log_warning(
                        f"Extract {extract_id} download failed for period {period}, skipping to next extract."
                    )
                    logging.error(f"Extract {extract_id} - period {period} error: {e!s}")
                    break  # skip to next extract

            current_run.log_info(f"Extract {extract_id} finished.")

    finally:
        push_queue.enqueue("FINISH")


def resolve_dataset_metrics(
    dataset_id: str,
    metrics: dict,
) -> list:
    """Resolves the metrics for a given dataset based on its definitions.

    Parameters
    ----------
    dataset_id : str
        The ID of the dataset for which metrics are to be resolved.
    metrics : list
        list of dataset metrics.

    Returns
    -------
    list
        List of metric identifiers for the specified dataset.
    """
    ds_metrics = []
    if len(metrics) > 0:
        ds_metrics = [f"{ds}.{metric}" for ds, metric in product([dataset_id], metrics)]
    return ds_metrics


def apply_data_element_extract_config(df: pd.DataFrame, extract_config: dict) -> pd.DataFrame:
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


def apply_reporting_rates_extract_config(
    df: pd.DataFrame,
    extract_config: dict,
) -> pd.DataFrame:
    """Handles the mappings of reporting rates."""
    raise NotImplementedError("Reporting rates mappings is not yet implemented.")


def apply_indicators_extract_config(
    df: pd.DataFrame,
    extract_config: dict,
):
    """Handles the mappings of reporting rates."""
    raise NotImplementedError("Indicator mappings is are not yet implemented.")


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

    # GET current dataset
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except requests.RequestException as e:
        return {"error": f"Network/HTTP error during dataset fetch: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error during dataset fetch: {e}"}

    if "error" in dataset_payload:
        return dataset_payload

    # Update organisationUnits
    if not new_org_units:
        return {"error": "No organisation units provided to assign to the dataset."}

    dataset_payload["organisationUnits"] = [{"id": ou_id} for ou_id in new_org_units]

    try:
        # PUT updated dataset
        update_response = dhis2_request(
            dhis2_client.api.session, "put", url, json=dataset_payload, params={"dryRun": str(dry_run).lower()}
        )
    except Exception as e:
        return {"error": f"Request error during dataset update: {e}"}

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


def handle_dataset_completion(
    syncer: DatasetCompletionSync, source_ds_id: str, target_ds_id: str, period: str, org_units: list[str]
) -> None:
    """Sets datasets as complete for the pushed periods.

    This is a placeholder function and should be implemented based on specific requirements.
    """
    if not source_ds_id:
        return
    if not target_ds_id:
        return

    current_run.log_info(
        f"Starting dataset '{target_ds_id}' completion process for period: {period} org units: {len(org_units)}."
    )
    try:
        syncer.sync(
            source_dataset_id=source_ds_id,
            target_dataset_id=target_ds_id,
            period=period,
            org_units=org_units,
            logging_interval=1000,
        )
    except Exception as e:
        current_run.log_error(f"Error setting dataset completion for dataset {target_ds_id}, period {period}")
        logging.error(f"Error setting dataset completion for dataset {target_ds_id}, period {period}: {e!s}")


def handle_full_pyramid_mapping(target_dhis2: DHIS2, source_ds_selection: pl.DataFrame) -> pl.DataFrame:
    """Handles the full pyramid dataset mapping by creating a dummy dataset in the source datasets.

    Parameters
    ----------
    target_dhis2 : DHIS2
        DHIS2 client for the target instance.
    source_ds_selection : pl.DataFrame
        DataFrame containing the selected source datasets.

    Returns
    -------
    pl.DataFrame
        Updated DataFrame with the full pyramid dataset included.
    """
    # Retrieve all organisation units from the PRS DHIS2 and create a dummy
    # dataset mapping in the source datasets table with all OUS at level 5 (around 24309 OUS)
    # we are not pushing nor computing rates for this dummy dataset. So the number of OUS is irrelevant.
    target_pyramid = target_dhis2.meta.organisation_units(fields="id,shortName,level")
    levels = [5]
    selected_ids = pl.DataFrame(target_pyramid).filter(pl.col("level").is_in(levels))["id"].unique().to_list()
    new_row = pl.DataFrame(
        {
            "id": ["FULL_PYRAMID"],
            "name": ["Dummy dataset"],
            "organisation_units": [selected_ids],
            "data_elements": [[]],
            "indicators": [[]],
            "period_type": ["Monthly"],
        }
    )

    return source_ds_selection.vstack(new_row)


def handle_zs_mapping(
    target_dhis2: DHIS2, ds_id: str, source_ds_selection: pl.DataFrame, source_pyramid: pd.DataFrame
) -> pl.DataFrame:
    """Handles the full pyramid dataset mapping by creating a dummy dataset in the source datasets.

    Parameters
    ----------
    target_dhis2 : DHIS2
        DHIS2 client for the target instance.
    ds_id : str
        The dataset ID to which the ZS org units will be added.
    source_ds_selection : pl.DataFrame
        DataFrame containing the selected source datasets.
    source_pyramid : pd.DataFrame
        DataFrame containing the source DHIS2 pyramid.

    Returns
    -------
    pl.DataFrame
        Updated DataFrame with the full pyramid dataset included.
    """
    # Retrieve all organisation units from the PRS DHIS2 and create a dummy
    # dataset mapping in the source datasets table with all OUS at level 5 (around 20365 OUS)
    # we are not pushing nor computing rates for this dummy dataset. So the number of OUS is irrelevant.
    level = 3
    zs_ou_ids = source_pyramid[source_pyramid["level"] == level]["id"].to_list()

    # GET current dataset from PRS DHIS2
    endpoint = "dataSets"
    url = f"{target_dhis2.api.url}/{endpoint}/{ds_id}"
    try:
        dataset_payload = dhis2_request(target_dhis2.api.session, "get", url)
    except requests.RequestException as e:
        return {"error": f"Network/HTTP error during payload fetch for dataset {ds_id} alignment: {e!s}"}
    except Exception as e:
        return {"error": f"Unexpected error during payload fetch for dataset {ds_id} alignment: {e!s}"}

    ds_uids = [ou["id"] for ou in dataset_payload["organisationUnits"]]
    new_org_units = list(set(zs_ou_ids) | set(ds_uids))  # push ZS from the 20 Provinces + current
    new_row = pl.DataFrame(
        {
            "id": ["ZONES_SANTE"],
            "name": ["zones de sante"],
            "organisation_units": [new_org_units],
            "data_elements": [[]],
            "indicators": [[]],
            "period_type": ["Monthly"],
        }
    )

    return source_ds_selection.vstack(new_row)


if __name__ == "__main__":
    dhis2_dataset_sync()
