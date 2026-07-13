import logging
import shutil
import typing
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from d2d_development.push import DHIS2Pusher
from d2d_library.dhis2_org_unit_aligner import DHIS2PyramidAligner
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from requests.exceptions import HTTPError, RequestException
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    get_dataset_version_timestamp,
    get_file_from_dataset,
    read_json_file,
    read_parquet_extract,
    save_json_file,
    save_to_parquet,
)

# Ticket(s) related to this pipeline:
#   -https://bluesquare.atlassian.net/browse/SANRUSSC24-32
#   -https://bluesquare.atlassian.net/browse/SAN-122
#   -https://bluesquare.atlassian.net/browse/SAN-125
#   -https://bluesquare.atlassian.net/browse/PATHEOC-410
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs


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
    code="run_push_data",
    name="Push data",
    type=bool,
    default=True,
    help="Push data to target DHIS2.",
)
@parameter(
    code="load_ds_files",
    name="Load dataset files",
    help="Load the files from the dataset.",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    code="force_run",
    name="Force run",
    help="Force the pipeline to run even if no new data is detected.",
    type=bool,
    default=False,
    required=False,
)
def dhis2_dataset_sync(run_ou_sync: bool, run_push_data: bool, load_ds_files: bool, force_run: bool) -> None:
    """Main pipeline function for DHIS2 dataset synchronization.

    Args:
        run_ou_sync: If True, runs the organisation units and dataset organisation units sync tasks.
        run_push_data: If True, runs the data push task.
        load_ds_files: If True, loads the dataset files from the specified dataset.
        force_run: If True, forces the pipeline to run even if no new dataset version is detected.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_dataset_sync"
    pipeline_config = pipeline_path / "configuration" / "push_config.json"

    # Load pipeline configuration
    config = read_json_file(pipeline_config)
    current_run.log_info(pipeline_config)
    dataset_id = config["SETTINGS"].get("OPENHEXA_DATASET_ID")

    # check updated data in dataset
    to_update = should_push_data(
        dataset_id=dataset_id,
        timestamp_path=pipeline_path / "configuration" / "last_update.json",
    )

    if to_update or force_run:
        current_run.log_info("New data version detected. Starting pipeline execution...")
        if load_ds_files:
            try:
                get_files_from_dataset(dataset_id=dataset_id, output_path=pipeline_path / "data")
            except Exception as e:
                current_run.log_error(f"Error loading dataset files: {e}")
                raise

        sync_organisation_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,
        )

        sync_dataset_organisation_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,  # only run if OU sync ran
        )

        push_data(
            pipeline_path=pipeline_path,
            dataset_id=dataset_id,
            run_task=run_push_data,
        )

        update_last_run_timestamp(
            timestamp_filename=pipeline_path / "configuration" / "last_update.json",
            dataset_id=dataset_id,
        )
    else:
        current_run.log_info("No new data version detected. Pipeline execution skipped.")


def should_push_data(dataset_id: str, timestamp_path: Path) -> bool:
    """Check if new data is available by comparing the latest dataset version timestamp.

    Args:
        dataset_id: The ID of the dataset to check for updates.
        timestamp_path: Path to the JSON file storing the last processed update timestamp.

    Returns:
        True if an update is needed, False if data is up to date or on error.
    """
    try:
        new_version_dt = get_dataset_version_timestamp(dataset_id=dataset_id)
    except Exception as e:
        current_run.log_error(f"Dataset {dataset_id} is not accessible, stopping pipeline execution. Details: {e}")
        return False

    # read last run timestamp from file
    try:
        last_update = read_json_file(timestamp_path)
        last_update_str = last_update.get("LAST_UPDATE", "")
        last_update_dt = datetime.strptime(last_update_str, "%Y%m%d_%H%M") if last_update_str else None
    except Exception as e:
        current_run.log_warning(f"Error reading last update timestamp. Running update by default. Details: {e}")
        return True  # If we can't read the last update, assume we need to update

    return not last_update_dt or new_version_dt > last_update_dt


def get_files_from_dataset(dataset_id: str, output_path: Path) -> None:
    """Load files from the dataset and save them to the pipeline's data directory.

    Args:
        dataset_id: The ID of the dataset to load files from.
        output_path: Directory where the downloaded files will be saved.

    Raises:
        ValueError: If `dataset_id` is not specified.
    """
    if dataset_id is None:
        raise ValueError("Missing OPENHEXA_DATASET_ID in configuration.")

    current_run.log_info(f"Loading data from dataset: {dataset_id}")

    # Load data files
    updates_files = get_file_from_dataset(dataset_id=dataset_id, filename="updates_collector.json")
    for key, fnames in updates_files.items():
        if key == "pyramid":
            current_run.log_info(f"Loading file: {fnames[0]}")  # only one file
            df_data = get_file_from_dataset(dataset_id=dataset_id, filename=fnames[0])
            save_to_parquet(data=df_data, filename=output_path / key / fnames[0])
            continue

        for fname in fnames:
            current_run.log_info(f"Loading file: {fname}")
            df_data = get_file_from_dataset(dataset_id=dataset_id, filename=fname)
            save_to_parquet(data=df_data, filename=output_path / "extracts" / "data_elements" / key / fname)


def sync_organisation_units(
    pipeline_path: Path,
    run_task: bool = True,
) -> None:
    """Pyramid alignment.

    This task creates/updates org units from source (SNIS) to PRS.

    Args:
        pipeline_path: Path to the pipeline directory.
        run_task: If False, skips the organisation units sync task.

    Raises:
        ValueError: If the target DHIS2 connection is missing from the configuration.
    """
    if not run_task:
        current_run.log_info("Organisation units sync task skipped.")
        return

    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="org_units_sync")

    # load configuration
    config_push = read_json_file(pipeline_path / "configuration" / "push_config.json")
    target_conn = config_push["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

    if not target_conn:
        raise ValueError("Missing DHIS2 connection details.")

    # Connect to DHIS2 target
    target_dhis2 = connect_to_dhis2(connection_str=target_conn, cache_dir=None)
    current_run.log_info(f"Connected to DHIS2: {target_conn}")

    try:
        align_org_units(
            pipeline_path=pipeline_path,
            target_dhis2=target_dhis2,
            logger=logger,
        )

    except Exception as e:
        raise Exception(f"Error during pyramid sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "org_units")


def align_org_units(
    pipeline_path: Path,
    target_dhis2: DHIS2,
    logger: logging.Logger | None = None,
) -> None:
    """Aligns organisation units between source and target DHIS2 connections.

    NOTE: Org units have already been filtered during extraction.
        (see pipeline: DRC DSNIS (workspace) - 'DHIS2 SNIS extracts for PRS sync').

    Args:
        pipeline_path: Path to the pipeline directory.
        target_dhis2: DHIS2 client for the target instance.
        logger: Logger instance for logging.
    """
    DHIS2PyramidAligner(logger).align_to(
        target_dhis2=target_dhis2,
        source_pyramid=read_parquet_extract(
            pipeline_path / "data" / "pyramid" / "snis_prs_pyramid.parquet"
        ).to_pandas(),  # NOTE: This needs rework to support polars under the hood.
        dry_run=False,  # NOTE: This has no effect, the org units will be created regardless if True or False
    )


def sync_dataset_organisation_units(
    pipeline_path: Path,
    run_task: bool = True,
) -> None:
    """Sync organisation units of datasets between source and target DHIS2 instances.

    WARNING: This step should only be executed AFTER organisation units alignment, we assume
    the organisation units have been properly aligned between source and target.

    Args:
        pipeline_path: Path to the pipeline directory.
        run_task: If False, skips the dataset organisation units sync task.
    """
    if not run_task:
        current_run.log_info("Dataset organisation units sync task skipped.")
        return

    current_run.log_info("Starting dataset organisation units sync.")
    logger, logs_file = configure_logging_flush(
        logs_path=Path("/home/jovyan/tmp/logs"), task_name="dataset_org_units_sync"
    )

    # load configuration
    config_extract = read_json_file(pipeline_path / "configuration" / "extract_config.json")
    config_sync = read_json_file(pipeline_path / "configuration" / "sync_config.json")
    config_push = read_json_file(pipeline_path / "configuration" / "push_config.json")
    source_conn = config_extract["SETTINGS"].get("SOURCE_DHIS2_CONNECTION")
    target_conn = config_push["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

    if not source_conn or not target_conn:
        raise ValueError("Missing DHIS2 connection details.")

    # Connect to DHIS2 instances
    # No cache for org units sync
    source_dhis2 = connect_to_dhis2(connection_str=source_conn, cache_dir=None)
    current_run.log_info(f"Connected to DHIS2: {source_conn}")
    target_dhis2 = connect_to_dhis2(connection_str=target_conn, cache_dir=None)
    current_run.log_info(f"Connected to DHIS2: {target_conn}")
    try:
        align_dataset_org_units(
            source_dhis2=source_dhis2,
            target_dhis2=target_dhis2,
            dataset_mappings=config_sync.get("DATASETS", {}),
            source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "snis_prs_pyramid.parquet"),
            logger=logger,
        )
    except Exception as e:
        raise Exception(f"Error during dataset organisation units sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "dataset_org_units")


def align_dataset_org_units(
    source_dhis2: DHIS2,
    target_dhis2: DHIS2,
    dataset_mappings: dict,
    source_pyramid: pl.DataFrame,
    logger: logging.Logger | None = None,
) -> None:
    """Aligns organisation units of datasets between source and target DHIS2 connections.

    Args:
        source_dhis2: DHIS2 client for the source instance.
        target_dhis2: DHIS2 client for the target instance.
        dataset_mappings: Mapping of source dataset IDs (or the special "FULL_PYRAMID"/"ZONES_SANTE"
            keys) to the corresponding target dataset ID(s).
        source_pyramid: The aligned/filtered source organisation unit pyramid, used to validate
            organisation units before pushing.
        logger: Logger instance for logging. Defaults to a module-level logger if None.
    """
    if len(dataset_mappings) == 0:
        current_run.log_warning("No dataset IDs provided for sync. Dataset organisation units task skipped.")
        return

    if logger is None:
        logger = logging.getLogger(__name__)

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    msg = f"Loading source pyramid for dataset sync. Shape: {source_pyramid.shape}"
    current_run.log_debug(msg)
    logger.info(msg)

    try:
        source_datasets = get_datasets(source_dhis2)
    except Exception as e:
        current_run.log_error(f"Failed to fetch source datasets: {e}")
        logger.error(f"Failed to fetch source datasets: {e}")
        return

    try:
        target_datasets = get_datasets(target_dhis2)
    except Exception as e:
        current_run.log_error(f"Failed to fetch target datasets: {e}")
        logger.error(f"Failed to fetch target datasets: {e}")
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
        current_run.log_debug(f"Processing dataset: {source_ds['name']} ({source_ds['id']}) from Sync config")
        source_ds_ou = source_ds["organisation_units"]

        if source_ds["id"] not in ["FULL_PYRAMID", "ZONES_SANTE"]:
            # Use the aligned/filtered org units from the source pyramid to validate the OU to be pushed
            valid_ous = set(source_pyramid["id"])
            source_ds_ou = [ou for ou in source_ds_ou if ou in valid_ous]

        target_ds_ids = dataset_mappings[source_ds["id"]]
        # safe guard against single string instead of list of strings
        if isinstance(target_ds_ids, str):
            target_ds_ids = [target_ds_ids]

        target_ds = target_datasets.filter(pl.col("id").is_in(target_ds_ids))
        if target_ds.is_empty():
            current_run.log_warning(f"Dataset id: {dataset_mappings[source_ds['id']]} not found in DHIS2 target.")
            continue

        for row in target_ds.iter_rows(named=True):
            target_ds_id = row["id"]
            target_ds_name = row["name"]
            target_ds_ou = row["organisation_units"]

            if set(source_ds_ou) != set(target_ds_ou):
                update_count = update_count + 1
                msg = (
                    f"Updating target {target_ds_name} (id: {target_ds_id}) OU count: {len(target_ds_ou)} with "
                    f"source dataset ({source_ds['name']}) OU count: {len(source_ds_ou)}"
                )
                current_run.log_info(msg)
                logger.info(msg)
                update_response = push_dataset_org_units(
                    dhis2_client=target_dhis2,
                    dataset_id=target_ds_id,
                    new_org_units=source_ds_ou,
                )

                if "error" in update_response:
                    error_count = error_count + 1
                    logger.error(
                        f"Error updating dataset {target_ds_name} (id: {target_ds_id}) - "
                        f"Error: {update_response['error']}"
                    )
                else:
                    msg = f"Dataset {target_ds_name} (id: {target_ds_id}) updated OU count: {len(source_ds_ou)}"
                    current_run.log_info(msg)
                    logger.info(msg)

    if error_count > 0:
        current_run.log_warning(
            f"{error_count} errors occurred during dataset org units update. Check logs for details."
        )
    if update_count == 0:
        current_run.log_info("No updates applied for dataset organisation units. Source and target datasets aligned.")


def push_data(
    pipeline_path: Path,
    dataset_id: str,
    run_task: bool = True,
) -> None:
    """Pushes data elements to the target DHIS2 instance.

    Args:
        pipeline_path: Path to the pipeline directory.
        dataset_id: The ID of the dataset containing the extracts and update queue to push.
        run_task: If False, skips the data push task.
    """
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return
    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data")
    # logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="push_data")  # local
    config = read_json_file(pipeline_path / "configuration" / "push_config.json")
    target_dhis2 = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)
    current_run.log_info(f"Connected to DHIS2: {config['SETTINGS']['TARGET_DHIS2_CONNECTION']}")

    # Push parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)
    extract_type = "data_elements"  # only DATA_ELEMENT type extracts, we don't handle other types here
    extract_list = config["DATA_ELEMENTS"].get("EXTRACTS", {})

    # log parameters
    params_msg = f"Push data with parameters: import_strategy={import_strategy}, dry_run={dry_run}, max_post={max_post}"
    logger.info(params_msg)
    current_run.log_info(params_msg)

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
    )

    # Get the list of files to push from the dataset
    update_files = get_file_from_dataset(dataset_id=dataset_id, filename="updates_collector.json")
    update_files.pop("pyramid", None)  # Remove the pyramid node

    # loop over the extracts
    for extract_id, fnames in update_files.items():
        current_run.log_info(f"Running push for extract {extract_id}")
        # Get extract configuration
        extract_config = next((e for e in extract_list if e.get("EXTRACT_UID") == extract_id), None)

        if not extract_config:
            current_run.log_warning(f"No configuration found for extract {extract_id}. Skipping.")
            continue

        if extract_config.get("TARGET_DATASET_UID"):
            try:
                dataset_info = retrieve_dataset_info(target_dhis2, extract_config.get("TARGET_DATASET_UID"))
            except Exception as e:
                current_run.log_error(
                    f"Failed to retrieve dataset info for {extract_config.get('TARGET_DATASET_UID')}. Error: {e}"
                )
                continue

        # Loop over the files in the dataset
        for fname in fnames:
            extract_path = pipeline_path / "data" / "extracts" / extract_type / extract_id / fname

            try:
                # Read extract file
                extract_data = read_parquet_extract(parquet_file=extract_path).cast(pl.String)
            except Exception as e:
                current_run.log_error(f"Failed to read extract file: {extract_path}. Error: {e}")
                continue

            try:
                # Determine data type
                data_type = extract_data["data_type"].unique()[0]  # depends on the order (!)
                period = extract_data["period"].unique()[0]
            except Exception as e:
                current_run.log_error(
                    f"Failed to determine data type or period for extract: {extract_path.name}. Error: {e}"
                )
                continue

            current_run.log_info(f"Pushing data for extract {extract_id}: {extract_path.name}.")
            if data_type not in ["DATA_ELEMENT"]:  # We'll handle DATA_ELEMENT only
                current_run.log_warning(f"Unknown DATA_TYPE '{data_type}' in extract: {extract_path.name}. Skipping.")
                continue

            # filter by the org units which are part of the target dataset (if set)
            # NOTE: The extracts are already filtered by the selection of the 20 provinces
            # (see: configuration/sync_config.json), this additional filtering makes sure
            # there are only data points for those org units part of the target dataset (PRS).
            if extract_config.get("TARGET_DATASET_UID"):
                extract_data = filter_by_dataset_org_units(
                    target_dhis2=target_dhis2,
                    data=extract_data,
                    dataset_id=extract_config.get("TARGET_DATASET_UID"),
                    ds_org_units=dataset_info["organisationUnits"],
                )

            try:
                # Apply mapping and push data
                extract_mapped = apply_data_element_extract_config(
                    df=extract_data, extract_config=extract_config, logger=logger
                )
                extract_mapped = extract_mapped.sort("org_unit")  # speed up DHIS2 processing
                pusher.push_data(df_data=extract_mapped)
                current_run.log_info(f"Data push finished for extract: {extract_path.name}.")

                # NOTE: Save copy of the org units under a dataset folder
                # These files are used by the slave pipeline dhis2_dataset_sync_completions
                # to know which org units to sync for the completions extracts.
                prepare_dataset_sync_data(pipeline_path, extract_config, extract_mapped, period)
            except Exception as e:
                current_run.log_error(
                    f"Fatal error for extract {extract_id} ({extract_path.name}), stopping push process. Error: {e}"
                )
                raise  # crash on error
            finally:
                save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")


def save_logs(logs_file: Path, output_dir: Path) -> None:
    """Copies the log file to the output directory.

    Args:
        logs_file: Path to the log file to copy.
        output_dir: Directory where the log file will be copied to.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    if logs_file.is_file():
        dest_file = output_dir / logs_file.name
        shutil.copy(logs_file.as_posix(), dest_file.as_posix())


def prepare_dataset_sync_data(pipeline_path: Path, extract_config: dict, df_mapped: pl.DataFrame, period: str) -> None:
    """Handles backup of organisation units under dataset folder after push.

    Args:
        pipeline_path: Path to the pipeline directory.
        extract_config: Configuration dictionary for the extract being processed.
        df_mapped: The mapped extract data, expected to contain an "org_unit" column.
        period: Period string used to name the backup parquet file.
    """
    source_ds = extract_config.get("SOURCE_DATASET_UID")
    target_ds = extract_config.get("TARGET_DATASET_UID")
    if not source_ds or not target_ds:
        return

    extract_id = extract_config.get("EXTRACT_UID")
    # NOTE: This is a common path shared with the slave pipeline dhis2_dataset_sync_completions
    dataset_dir = pipeline_path / "data" / "dataset_sync" / extract_id
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Ensure the column exists
    if "org_unit" not in df_mapped.columns:
        current_run.log_warning("Mapping table must contain an 'org_unit' column. ds_sync backup skipped.")
        return

    unique_org_units_df = df_mapped.select("org_unit").unique().rename({"org_unit": "ORG_UNIT"})
    output_path = dataset_dir / f"ds_sync_{period}.parquet"
    unique_org_units_df.write_parquet(output_path)
    current_run.log_info(f"Dataset sync org units saved: {output_path}")


def filter_by_dataset_org_units(
    target_dhis2: DHIS2, data: pd.DataFrame | pl.DataFrame, dataset_id: str, ds_org_units: list
) -> pl.DataFrame:
    """Filters the provided data to include only rows with organisation units present in the specified DHIS2 dataset.

    Args:
        target_dhis2: DHIS2 client for the target instance.
        data: DataFrame containing the data to be filtered.
        dataset_id: The ID of the dataset whose organisation units will be used for filtering.
        ds_org_units: List of organisation unit dicts (each with an "id" key) belonging to the dataset.

    Returns:
        Filtered DataFrame containing only rows with organisation units present in the dataset.

    Raises:
        RuntimeError: If an error occurs while fetching the dataset payload, or if the DHIS2 API
            returns an error response.
        ValueError: If the dataset payload is malformed, has no organisation units, or if no data
            remains after filtering.
    """
    if isinstance(data, pd.DataFrame):
        data = pl.from_pandas(data)

    ds_uids = [ou["id"] for ou in ds_org_units]  # dataset_payload["organisationUnits"]]
    data_filtered = data.filter(pl.col("org_unit").is_in(ds_uids))
    current_run.log_info(
        f"Extract filtered by dataset {dataset_id} with OUs: {len(ds_uids)}. "
        f"Data points removed from import data: {data.shape[0] - data_filtered.shape[0]}"
    )

    if data_filtered.shape[0] == 0:
        raise ValueError(
            f"After filtering by dataset {dataset_id} org units, no data remains. "
            f"Check that source data org units match the target dataset's {len(ds_uids)} org units."
        )

    return data_filtered


def retrieve_dataset_info(dhis2_client: DHIS2, dataset_id: str) -> dict:
    """Retrieves the dataset information from the DHIS2 API.

    Args:
        dhis2_client: DHIS2 client for the target instance.
        dataset_id: The ID of the dataset to retrieve.

    Returns:
        The dataset payload, including its organisation units.

    Raises:
        RuntimeError: If an error occurs while fetching the dataset payload, or if the DHIS2 API
            returns an error response.
        ValueError: If the response is not a dictionary, or the dataset has no organisation units.
    """
    # GET current dataset from PRS DHIS2
    url = f"{dhis2_client.api.url}/dataSets/{dataset_id}?fields=id,organisationUnits[id]"
    current_run.log_info(f"Fetching dataset from: {url}")
    try:
        dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    except Exception as e:
        raise RuntimeError(f"Unexpected error during payload fetch for dataset {dataset_id}: {e!s}") from e

    if not isinstance(dataset_payload, dict):
        raise ValueError(f"Invalid response for dataset {dataset_id}. Expected a dictionary.")

    # Optional: catch error responses from dhis2_request
    if "error" in dataset_payload:
        raise RuntimeError(f"DHIS2 API returned an error for dataset {dataset_id}: {dataset_payload['error']}")

    # Check if organisationUnits exists in response
    if "organisationUnits" not in dataset_payload:
        raise ValueError(
            f"Dataset {dataset_id} response missing 'organisationUnits' field. "
            f"Cannot filter data without org unit information. "
            f"Available fields: {list(dataset_payload.keys())}"
        )

    # Check if organisationUnits is empty
    if not dataset_payload["organisationUnits"]:
        raise ValueError(
            f"Dataset {dataset_id} has no organisation units assigned. "
            f"Cannot proceed with data push - dataset must have org units configured."
        )

    return dataset_payload


def apply_data_element_extract_config(
    df: pd.DataFrame | pl.DataFrame, extract_config: dict, logger: logging.Logger | None = None
) -> pl.DataFrame:
    """Applies data element mappings to the extracted data.

    It also filters data elements based on category option combo (COC) if specified in the extract configuration.

    Args:
        df: DataFrame containing the extracted data.
        extract_config: Dictionary containing the extract mappings.
        logger: Logger instance for logging.

    Returns:
        DataFrame with mapped data elements.
    """
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df)

    if len(extract_config) == 0:
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df

    extract_mappings = extract_config.get("MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data element mappings.")
        return df

    if logger is None:
        logger = logging.getLogger(__name__)

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info(f"Applying data element mappings for extract: {extract_config.get('EXTRACT_UID')}.")
    chunks = []
    uid_mappings = {}
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("UID")
        coc_mappings = mapping.get("CATEGORY_OPTION_COMBO", {})
        aoc_mappings = mapping.get("ATTRIBUTE_OPTION_COMBO", {})

        # Build a mask selection
        df_uid = df.filter(pl.col("dx") == uid)
        if coc_mappings:
            coc_mappings = {k: v for k, v in coc_mappings.items() if v is not None}  # Do not replace with None
            coc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in coc_mappings.items()}
            df_uid = df_uid.filter(pl.col("category_option_combo").is_in(list(coc_mappings_clean.keys())))
            df_uid = df_uid.with_columns(pl.col("category_option_combo").replace(coc_mappings_clean))

        if aoc_mappings:
            aoc_mappings = {k: v for k, v in aoc_mappings.items() if v is not None}  # Do not replace with None
            aoc_mappings_clean = {str(k).strip(): str(v).strip() for k, v in aoc_mappings.items()}
            df_uid = df_uid.filter(pl.col("attribute_option_combo").is_in(list(aoc_mappings_clean.keys())))
            df_uid = df_uid.with_columns(pl.col("attribute_option_combo").replace(aoc_mappings_clean))

        if uid_mapping:
            uid_mappings[uid] = uid_mapping

        chunks.append(df_uid)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logger.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return df.clear()

    df_filtered = pl.concat(chunks)

    if uid_mappings:
        uid_mappings = {k: v for k, v in uid_mappings.items() if v is not None}  # Do not replace with None
        uid_mappings_clean = {str(k).strip(): str(v).strip() for k, v in uid_mappings.items()}
        df_filtered = df_filtered.with_columns(pl.col("dx").replace(uid_mappings_clean))

    # Complete with defaults
    return df_filtered.with_columns(
        pl.col("category_option_combo").fill_null("HllvX50cXC0"),
        pl.col("attribute_option_combo").fill_null("HllvX50cXC0"),
    )


def push_dataset_org_units(dhis2_client: DHIS2, dataset_id: str, new_org_units: list[str]) -> dict:
    """Updates the organisation units of a DHIS2 dataset.

    Args:
        dhis2_client: DHIS2 client for the target instance.
        dataset_id: The ID of the dataset to update.
        new_org_units: List of organisation unit IDs to assign to the dataset.

    Returns:
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
        update_response = dhis2_request(dhis2_client.api.session, "put", url, json=dataset_payload)
    except Exception as e:
        return {"error": f"Request error during dataset update: {e}"}

    return update_response


def dhis2_request(session: requests.Session, method: str, url: str, **kwargs: typing.Any) -> dict:
    """Wrapper around requests to handle DHIS2 GET/PUT with error handling.

    Args:
        session: Session object used to perform requests.
        method: HTTP method: "get" or "put".
        url: Full URL for the request.
        **kwargs: Additional arguments for `session.request` (json, params, etc.).

    Returns:
        Either the response JSON or an error payload with "error" and "status_code".
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


def handle_full_pyramid_mapping(target_dhis2: DHIS2, source_ds_selection: pl.DataFrame) -> pl.DataFrame:
    """Handles the full pyramid dataset mapping by creating a dummy dataset in the source datasets.

    Args:
        target_dhis2: DHIS2 client for the target instance.
        source_ds_selection: DataFrame containing the selected source datasets.

    Returns:
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


def handle_zs_mapping(source_ds_selection: pl.DataFrame, source_pyramid: pl.DataFrame) -> pl.DataFrame:
    """Handles the zones de sante (ZS) dataset mapping by creating a dummy dataset in the source datasets.

    Args:
        source_ds_selection: DataFrame containing the selected source datasets.
        source_pyramid: DataFrame containing the source DHIS2 pyramid.

    Returns:
        Updated DataFrame with the zones de sante dataset included.
    """
    # Add a dummy dataset "source_ds_selection" table with id "ZONES_SANTE"
    # The idea is to use this list of ZS to push it into the target Datasets in PRS
    # that should follow zones de sante (around 407 OUS)
    level = 3
    zs_ou_ids = source_pyramid.filter(pl.col("level") == level)["id"].to_list()
    new_org_units = list(set(zs_ou_ids))  # push ZS from the 20 Provinces + current
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


def update_last_run_timestamp(timestamp_filename: Path, dataset_id: str) -> None:
    """Updates the last run timestamp in the JSON file.

    Args:
        timestamp_filename: Path to the JSON file storing the last run timestamp.
        dataset_id: The ID of the dataset whose latest version timestamp will be saved.
    """
    timestamp = get_dataset_version_timestamp(dataset_id=dataset_id)
    try:
        save_json_file(
            file_path=timestamp_filename,
            contents={"LAST_UPDATE": timestamp.strftime("%Y%m%d_%H%M")},
        )
        current_run.log_info(f"Last run timestamp updated to: {timestamp.strftime('%Y%m%d_%H%M')}")
    except Exception as e:
        current_run.log_error(f"Error updating last run timestamp: {e}")


if __name__ == "__main__":
    dhis2_dataset_sync()
