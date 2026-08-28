import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from cmm_utils import (
    apply_formulas_to_extract,
    compute_mean_and_format_results,
    get_fosa_descendants_of_zs,
)
from d2d_development.push import DHIS2Pusher
from d2d_library.org_unit_aligner import DHIS2PyramidAligner
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    dhis2_request,
    get_dataset_version_timestamp,
    get_extract_periods,
    get_file_from_dataset,
    read_json_file,
    read_parquet_extract,
    resolve_dates_and_validate,
    save_json_file,
    save_logs,
    save_to_parquet,
)

# Ticket(s) related to this pipeline:
#   - https://bluesquare.atlassian.net/browse/SAN-126 (OLD Pipeline)
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs


@pipeline("dhis2_cmm_morbidity_ds")
@parameter(
    code="start_date",
    name="Start date (format: YYYYMM)",
    type=str,
    help=(
        "Start date for data extraction in YYYYMM format. "
        "If not set, it will default to current date minus NUMBER_MONTHS_WINDOW."
    ),
    required=False,
    default=None,
)
@parameter(
    code="end_date",
    name="End date (format: YYYYMM)",
    type=str,
    help=(
        "End date for data extraction in YYYYMM format. "
        "If not set, it will default to current date minus NUMBER_MONTHS_WINDOW."
    ),
    required=False,
    default=None,
)
@parameter(
    code="run_ou_sync",
    name="Run org units sync (recommended)",
    type=bool,
    help="Run organisation units alignment between source and target DHIS2.",
    default=True,
    required=True,
)
@parameter(
    code="run_push_data",
    name="Push data",
    type=bool,
    help="Push data to target DHIS2.",
    default=True,
    required=False,
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
def dhis2_cmm_morbidity_ds(
    start_date: str, end_date: str, run_ou_sync: bool, run_push_data: bool, load_ds_files: bool, force_run: bool
) -> None:
    """Pipeline to push data from a source DHIS2 instance to a target DHIS2 instance."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_cmm_morbidity_ds"

    # Load pipeline configuration
    config = read_json_file(pipeline_path / "configuration" / "push_config.json")
    dataset_id = config["SETTINGS"].get("OPENHEXA_DATASET_ID")

    # check updated data in dataset
    to_update = should_push_data(
        dataset_id=dataset_id,
        timestamp_path=pipeline_path / "configuration" / "last_update.json",
    )

    if to_update or force_run:
        if to_update:
            current_run.log_info("New data version detected. Starting pipeline execution...")
        if force_run:
            current_run.log_info("Force run enabled. Starting pipeline execution regardless of data version...")
        if load_ds_files:
            try:
                get_files_from_dataset(dataset_id=dataset_id, output_path=pipeline_path / "data")
            except Exception as e:
                current_run.log_error(f"Error loading dataset files: {e}")
                raise

        sync_organisation_units(
            pipeline_path=pipeline_path,
            config=config,
            run_task=run_ou_sync,
        )

        sync_organisation_unit_groups(
            pipeline_path=pipeline_path,
            config=config,
            sync_config=read_json_file(pipeline_path / "configuration" / "sync_config.json"),
            run_task=run_ou_sync,
        )

        files_to_push = compute_cmm_morbidity_indicators(
            pipeline_path=pipeline_path,
            start_date=start_date,
            end_date=end_date,
            config=config,
            cmm_config=read_json_file(pipeline_path / "configuration" / "cmm_config.json"),
        )

        push_data(
            pipeline_path=pipeline_path,
            config=config,
            files_to_push=files_to_push,
            run_task=run_push_data,
        )

        update_last_run_timestamp(
            timestamp_filename=pipeline_path / "configuration" / "last_update.json",
            dataset_id=dataset_id,
            run_task=run_push_data and not config.get("SETTINGS", {}).get("DRY_RUN", True),
        )
    else:
        current_run.log_info("No new data version detected. Pipeline execution skipped.")


def sync_organisation_units(
    pipeline_path: Path,
    config: dict,
    run_task: bool = True,
) -> None:
    """Pyramid alignment task."""
    if not run_task:
        current_run.log_info("Organisation units sync task skipped.")
        return True

    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="ou_sync")
    # logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="ou_sync")  ## Local

    # load configuration
    target_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")
    target_dhis2 = connect_to_dhis2(connection_str=target_conn, cache_dir=None)

    try:
        DHIS2PyramidAligner(logger).align_to(
            target_dhis2=target_dhis2,
            source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
        )
    except Exception as e:
        raise Exception(f"Error during pyramid sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "ou_sync")


def sync_organisation_unit_groups(
    pipeline_path: Path,
    config: dict,
    sync_config: dict,
    run_task: bool,
) -> None:
    """Updates the organisation units of groups in the PRS DHIS2 instance."""
    if not run_task:
        current_run.log_info("Update organisation unit groups task skipped.")
        return

    current_run.log_info("Starting update of organisation unit groups.")

    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="oug_sync")
    # logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="oug_sync")  ## local
    prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

    for oug_source, oug_target in sync_config.get("ORG_UNIT_GROUPS", {}).items():
        current_run.log_info(f"Syncing organisation unit group. Source: {oug_source} to target: {oug_target}")
        try:
            sync_org_units_groups(
                ou_groups=pl.read_parquet(pipeline_path / "data" / "org_unit_groups" / "org_unit_groups.parquet"),
                dhis2_client_target=connect_to_dhis2(connection_str=prs_conn),
                source_oug_id=oug_source,
                target_oug_id=oug_target,
                pyramid=pl.read_parquet(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
                logger=logger,
            )
        except Exception as e:
            current_run.log_error(f"Error syncing organisation unit group {oug_source} to {oug_target}: {e}")
            logger.error(f"Error syncing organisation unit group {oug_source} to {oug_target}: {e}")
            raise
        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "oug_sync")


def sync_org_units_groups(
    ou_groups: pl.DataFrame,
    dhis2_client_target: DHIS2,
    source_oug_id: str,
    target_oug_id: str,
    pyramid: pl.DataFrame | None = None,
    validation_level: int | None = 3,
    logger: logging.Logger | None = None,
) -> None:
    """Syncs organisation unit groups between source and target datasets in DHIS2.

    NOTE: This is PRS specific.

    Args:
        ou_groups: DataFrame containing organisation unit groups from the source dataset.
        dhis2_client_target: DHIS2 client for the target instance.
        source_oug_id: ID of the source organisation unit group to sync.
        target_oug_id: ID of the target organisation unit group to update.
        pyramid: Optional DataFrame containing the pyramid structure for validation.
        validation_level: Optional integer specifying the level of validation for filtering organisation units.
        logger: Optional logger for logging messages.
    """
    source_oug = ou_groups.filter(pl.col("id").is_in([source_oug_id]))
    source_ous = source_oug["organisation_units"].explode().to_list()

    # Step 1: GET current OUG from target
    url = f"{dhis2_client_target.api.url}/organisationUnitGroups/{target_oug_id}"
    oug_payload = dhis2_request(
        session=dhis2_client_target.api.session,
        method="get",
        url=url,
    )
    if "error" in oug_payload:
        current_run.log_error(f"Error retrieving organisation unit group {target_oug_id}: {oug_payload['error']}")
        logger.info(f"Error retrieving organisation unit group {target_oug_id}: {oug_payload['error']}")
        return

    target_ous = set([ou.get("id") for ou in oug_payload["organisationUnits"]])

    # filter both lists of ids if they are part of the target 20 provinces (PRS specific)
    # level 3 are zones de sante
    if pyramid is not None:
        valid_ous = pyramid.filter(pl.col("level") == validation_level)["id"].to_list()
        source_ous = [ou_id for ou_id in source_ous if ou_id in valid_ous]  # Filter
        target_ous = [ou_id for ou_id in target_ous if ou_id in valid_ous]  # Filter

    # here first check if the list of ids is different
    to_add = set(source_ous) - set(target_ous)  # missing in target
    to_remove = set(target_ous) - set(source_ous)  # extra in target
    diff_org_units = to_add | to_remove
    if len(diff_org_units) == 0:
        current_run.log_info("Source and target dataset organisation units are in sync, no update needed.")
        logger.info("Source and target dataset organisation units are in sync, no update needed.")
        return

    msg = (
        f"Found {len(diff_org_units)} different org units in target dataset '{oug_payload['name']}' ({target_oug_id})."
    )
    current_run.log_info(msg)
    logger.info(msg)

    # NOTE: Update organisationUnits (just push the source OUs)
    oug_payload["organisationUnits"] = [{"id": ou_id} for ou_id in source_ous]

    # PUT updated organisation units group
    update_response = dhis2_request(
        session=dhis2_client_target.api.session,
        method="put",
        url=url,
        json=oug_payload,
        # params={"dryRun": str(dry_run).lower()},
    )

    if "error" in update_response:
        msg = f"Error updating organisation units group {target_oug_id}: {update_response['error']}"
        current_run.log_error()
        logger.error(msg)
    else:
        msg = f"organisation unit group '{oug_payload['name']}' ({target_oug_id}) org units set: {len(source_ous)}"
        current_run.log_info(msg)
        logger.info(msg)


def compute_cmm_morbidity_indicators(
    pipeline_path: Path,
    start_date: str,
    end_date: str,
    config: dict,
    cmm_config: dict,
) -> list[Path]:
    """Computes CMM morbidity indicators based on the extracted data elements.

    Returns:
        A list of paths to the generated CMM morbidity indicator files.
    """
    data_source_path = pipeline_path / "data" / "extracts" / "data_elements"
    data_output_path = pipeline_path / "data" / "cmm_morbidity"

    extract_uid = cmm_config.get("CMM_SETTINGS", {}).get("EXTRACT_UID")
    cmm_window = cmm_config.get("CMM_SETTINGS", {}).get("CMM_WINDOW_MONTHS", 6)

    start, end = resolve_dates_and_validate(start_date, end_date, config)
    extract_periods = get_extract_periods(start, end)

    try:
        oug_id = cmm_config["CMM_SETTINGS"].get("OUG_URBAN", "cOK4Feyi0nP")
        current_run.log_info(f"Retrieving Organization Units for Urban Health Zones under OUG '{oug_id}'")
        level5_under_zs = get_fosa_descendants_of_zs(
            pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
            dhis2_client=connect_to_dhis2(config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")),
            oug_id=oug_id,
        )
    except Exception as e:
        current_run.log_error(f"Error retrieving FOSA descendants of urban Zones de sante: {e}")
        raise

    cmm_file_results = []
    formulas = cmm_config.get("CMM_SETTINGS", {}).get("FORMULAS", {})
    for period in extract_periods:
        cmm_start = (datetime.strptime(period, "%Y%m") - relativedelta(months=cmm_window)).strftime("%Y%m")
        cmm_end = (datetime.strptime(period, "%Y%m") - relativedelta(months=1)).strftime("%Y%m")
        cmm_periods = get_extract_periods(cmm_start, cmm_end)
        current_run.log_info(
            f"Computing period: {period} - window: {cmm_window} ({cmm_periods[0]} to {cmm_periods[-1]})"
        )

        # retrieve the corresponding cmm extract per period
        cmm_results = []
        for cmm_period in cmm_periods:
            extract_path = data_source_path / extract_uid / f"data_{cmm_period}.parquet"

            try:
                extract_data = read_parquet_extract(extract_path)
            except FileNotFoundError:
                current_run.log_warning(
                    f"Extract data file not found: {extract_path.name}, skipping CMM period {period}."
                )
                cmm_results = []
                break  # skip to next period

            # To numeric
            extract_data = extract_data.with_columns(pl.col("value").cast(pl.Float64, strict=False))

            try:
                # Calculate CMM indicators for period
                period_results = apply_formulas_to_extract(extract_data, formulas, ou_urban=level5_under_zs)
            except Exception as e:
                current_run.log_error(f"Error computing morbidity indicators for period {cmm_period}: {e}")
                raise

            cmm_results.append(period_results)

        if cmm_results == []:
            break  # skip to next extract

        cmm_result_period = pl.concat(cmm_results)
        cmm_morbidity = compute_mean_and_format_results(cmm_result_period, period)

        # save results
        extract_path = data_output_path / extract_uid / f"cmm_morbidity_{period}.parquet"
        extract_path.parent.mkdir(parents=True, exist_ok=True)
        cmm_morbidity.write_parquet(extract_path)
        cmm_file_results.append(extract_path)
        current_run.log_info(f"CMM morbidity indicators saved: {extract_path.name}")

    return cmm_file_results


def push_data(
    pipeline_path: Path,
    config: dict,
    files_to_push: list[Path],
    run_task: bool = True,
) -> None:
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")
        return

    if len(files_to_push) == 0:
        current_run.log_info("No files to push, skipping data push.")
        return

    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data")
    # logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="push_data")  ## Local
    target_dhis2 = connect_to_dhis2(connection_str=config["SETTINGS"]["TARGET_DHIS2_CONNECTION"])

    # Push parameters
    import_strategy = config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = config["SETTINGS"].get("DRY_RUN", True)
    max_post = config["SETTINGS"].get("MAX_POST", 500)

    # log parameters
    logger.info(f"Import strategy: {import_strategy} - Dry Run: {dry_run} - Max Post elements: {max_post}")
    current_run.log_info(
        f"Pushing data with parameters import_strategy: {import_strategy}, dry_run: {dry_run}, max_post: {max_post}"
    )

    # Set up DHIS2 pusher
    pusher = DHIS2Pusher(
        dhis2_client=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        max_post=max_post,
        logger=logger,
        cache_path=pipeline_path / "cache" / "cmm_push",
    )

    # loop over the queue
    extract_mappings = config.get("CMM_MAPPINGS", {})
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract mappings provided, skipping data push.")
        return

    for cmm_file in files_to_push:
        try:
            extract_data = read_parquet_extract(cmm_file)
        except Exception as e:
            current_run.log_error(f"Failed to read extract from queue item: {cmm_file.name}. Error: {e}")
            continue

        try:
            current_run.log_info(f"Pushing data for extract {cmm_file.name}.")
            # NOTE: cmm specific mappings
            df_mapped = apply_cmm_indicators_extract_config(extract_data, extract_mappings, logger=logger)
            pusher.push_data(df_data=df_mapped)
            current_run.log_info(f"Data push finished for extract: {cmm_file.name}.")
        except Exception as e:
            current_run.log_error(
                f"Fatal error for cmm results push '{cmm_file.name}', stopping push process. Error: {e!s}"
            )
            raise  # crash on error

        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")


def apply_cmm_indicators_extract_config(
    df: pl.DataFrame, extract_mappings: dict, logger: logging.Logger | None = None
) -> pl.DataFrame:
    """Applies data element mappings to the CMM indicators.

    Args:
        df: DataFrame containing the extracted data.
        extract_mappings: Dictionary containing the extract mappings.
        logger: Optional logger for logging messages.

    Returns:
        DataFrame with the applied data element mappings.
    """
    if len(extract_mappings) == 0:
        current_run.log_warning("No extract details provided, skipping data element mappings.")
        return df

    # Loop over the configured data element mappings to filter by COC/AOC if provided
    current_run.log_info("Applying data element mappings.")
    chunks = []
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("DX_UID")
        coc_mapping = mapping.get("CATEGORY_OPTION_COMBO")
        aoc_mapping = mapping.get("ATTRIBUTE_OPTION_COMBO")

        # select indicator data
        df_indicator = df.filter(pl.col("indicator") == uid)
        if coc_mapping:
            df_indicator = df_indicator.with_columns(pl.lit(coc_mapping.strip()).alias("category_option_combo"))
        if aoc_mapping:
            df_indicator = df_indicator.with_columns(pl.lit(aoc_mapping.strip()).alias("attribute_option_combo"))
        if uid_mapping:
            df_indicator = df_indicator.with_columns(pl.lit(uid_mapping.strip()).alias("dx"))

        chunks.append(df_indicator)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logger.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pl.DataFrame(schema=df.schema)

    return (
        pl.concat(chunks)
        .with_columns(pl.when(pl.col("value").abs() >= 1e-9).then(pl.col("value")).otherwise(0).round(4).alias("value"))
        .sort(by="org_unit")
        .with_columns(pl.col("value").cast(pl.Utf8))  # push class handles only string values.
    )


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
            ou_data = get_file_from_dataset(dataset_id=dataset_id, filename=fnames[0])
            save_to_parquet(data=ou_data, filename=output_path / key / fnames[0])
            continue

        if key == "org_unit_groups":
            current_run.log_info(f"Loading file: {fnames[0]}")  # only one file
            oug_data = get_file_from_dataset(dataset_id=dataset_id, filename=fnames[0])
            save_to_parquet(data=oug_data, filename=output_path / key / fnames[0])
            continue

        for fname in fnames:
            current_run.log_info(f"Loading file: {fname}")
            df_data = get_file_from_dataset(dataset_id=dataset_id, filename=fname)
            save_to_parquet(data=df_data, filename=output_path / "extracts" / "data_elements" / key / fname)


def update_last_run_timestamp(timestamp_filename: Path, dataset_id: str, run_task: bool) -> None:
    """Updates the last run timestamp in the JSON file.

    Args:
        timestamp_filename: Path to the JSON file storing the last run timestamp.
        dataset_id: The ID of the dataset whose latest version timestamp will be saved.
        run_task: Boolean indicating whether to run the update task or skip it.
    """
    if not run_task:
        current_run.log_info("Last run timestamp update task skipped.")
        return

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
    dhis2_cmm_morbidity_ds()
