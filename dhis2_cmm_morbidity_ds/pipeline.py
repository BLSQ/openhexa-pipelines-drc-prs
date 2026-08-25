import logging
from datetime import datetime
from pathlib import Path

# import pandas as pd
import polars as pl
from d2d_development.push import DHIS2Pusher
from d2d_library.dhis2.org_unit_aligner import DHIS2PyramidAligner
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups
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
    required=False,
    help=(
        "Start date for data extraction in YYYYMM format. "
        "If not set, it will default to current date minus NUMBER_MONTHS_WINDOW."
    ),
    default=None,
)
@parameter(
    code="end_date",
    name="End date (format: YYYYMM)",
    type=str,
    required=False,
    help=(
        "End date for data extraction in YYYYMM format. "
        "If not set, it will default to current date minus NUMBER_MONTHS_WINDOW."
    ),
    default=None,
)
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
        current_run.log_info("New data version detected. Starting pipeline execution...")
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

        # compute_cmm_morbidity_indicators(
        #     pipeline_path=pipeline_path,
        #     start_date=start_date,
        #     end_date=end_date,
        #     config=config,
        #     cmm_config=read_json_file(pipeline_path / "configuration" / "cmm_config.json"),
        # )

        # push_data(
        #     pipeline_path=pipeline_path,
        #     config=config,
        #     start_date=start_date,
        #     end_date=end_date,
        #     run_task=run_push_data,
        # )

        # update_last_run_timestamp(
        #     timestamp_filename=pipeline_path / "configuration" / "last_update.json",
        #     dataset_id=dataset_id,
        # )
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

    # logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="org_units_sync")
    logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="org_units_sync")  ## Local

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
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "org_units_sync")


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

    # logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="oug_sync")
    logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="oug_sync")  ## local
    prs_conn = config["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

    for oug_source, oug_target in sync_config.get("ORG_UNIT_GROUPS", {}).items():
        current_run.log_info(f"Syncing organisation unit group. Source: {oug_source} to target: {oug_target}")
        try:
            sync_org_units_groups(
                ou_groups=read_json_file(pipeline_path / "data" / "org_unit_groups" / "org_unit_groups.parquet"),
                dhis2_client_target=connect_to_dhis2(connection_str=prs_conn),
                source_oug_id=oug_source,
                target_oug_id=oug_target,
                pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
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
) -> dict:
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

    Returns:
        A dictionary containing the response from the DHIS2 API after attempting to update the
          target organisation unit group.
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
        return oug_payload

    target_ous = set([ou.get("id") for ou in oug_payload["organisationUnits"]])

    # filter both lists of ids if they are part of the target 20 provinces (PRS specific)
    # level 3 are zones de sante
    if pyramid is not None:
        valid_ous = pyramid[pyramid.level == validation_level]["id"].to_list()
        source_ous = [ou_id for ou_id in source_ous if ou_id in valid_ous]
        target_ous = set([ou_id for ou_id in target_ous if ou_id in valid_ous])

    # here first check if the list of ids is different
    to_add = set(source_ous) - set(target_ous)  # missing in target
    to_remove = set(target_ous) - set(source_ous)  # extra in target
    diff_org_units = to_add | to_remove
    if len(diff_org_units) == 0:
        current_run.log_info("Source and target dataset organisation units are in sync, no update needed.")
        return {"status": "skipped", "message": "No update needed, org units are identical."}

    current_run.log_info(
        f"Found {len(diff_org_units)} different org units in target dataset '{oug_payload['name']}' ({target_oug_id})."
    )

    # Update organisationUnits (just push the source OUs)
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
        current_run.log_info(f"Error updating organisation units group {target_oug_id}: {update_response['error']}")
        logger.error(f"Error updating organisation units group {target_oug_id}: {update_response['error']}")
    else:
        msg = f"organisation unit group '{oug_payload['name']}' ({target_oug_id}) org units set: {len(source_ous)}"
        current_run.log_info(msg)
        logger.info(msg)

    return update_response


def compute_cmm_morbidity_indicators(
    pipeline_path: Path,
    start_date: str,
    end_date: str,
    config: dict,
    cmm_config: dict,
):
    """Computes CMM morbidity indicators based on the extracted data elements."""
    data_source_path = pipeline_path / "data" / "extracts" / "data_elements"
    data_output_path = pipeline_path / "data" / "cmm_morbidity"

    extract_uid = cmm_config.get("CMM_SETTINGS", {}).get("EXTRACT_UID")
    cmm_window = cmm_config.get("CMM_SETTINGS", {}).get("CMM_WINDOW_MONTHS", 6)

    start, end = resolve_dates_and_validate(start_date, end_date, config)
    extract_periods = get_extract_periods(start, end)

    try:
        level5_under_zs = get_fosa_descendants_of_zs(
            source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
            dhis2_client=config["SETTINGS"].get("TARGET_DHIS2_CONNECTION"),
            oug_id=cmm_config["CMM_SETTINGS"].get("OUG_URBAN", "cOK4Feyi0nP"),
        )
    except Exception as e:
        current_run.log_error(f"Error retrieving FOSA descendants of urban Zones de sante: {e}")
        raise

    for period in extract_periods:
        formulas = cmm_config.get("CMM_SETTINGS", {}).get("FORMULAS", {})
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
        current_run.log_info(f"CMM morbidity indicators saved: {extract_path.name}")


def get_formulas_for_extract(extract_uid: str, cmm_extracts: list) -> list:
    """Returns the list of rules for the matching extract UID.

    Args:
        extract_uid: The UID of the extract.
        cmm_extracts: The list of all cmm extract formulas.

    Returns:
        A list of rules corresponding to the given extract UID.
    """
    for rule in cmm_extracts:
        if rule.get("EXTRACT_UID") == extract_uid:
            return rule.get("FORMULAS", [])
    return []


def get_fosa_descendants_of_zs(pyramid: pl.DataFrame, dhis2_client: DHIS2, oug_id: str) -> list:
    """Retrieves the list of FOSA organisation units that are descendants of urban Zones de sante.

    Args:
        pyramid: The organisation units pyramid as a Polars DataFrame.
        dhis2_client: The DHIS2 client instance.
        oug_id: The organisation unit group ID for urban Zones de sante.

    Returns:
        List of level 5 organisation unit IDs that are descendants of urban Zones de sante.
    """
    current_run.log_info(f"Retrieving Organization Units for Urban Health Zones under OUG '{oug_id}'")
    ou_groups = get_organisation_unit_groups(dhis2_client)
    zs_urban = ou_groups.filter(pl.col("id") == oug_id)
    zs_urban_list = zs_urban["organisation_units"].explode().to_list()
    parent_map = dict(
        zip(
            pyramid["id"],
            pyramid["parent"].apply(lambda x: x["id"] if isinstance(x, dict) else None),
            strict=True,
        )
    )
    level5 = pyramid[pyramid["level"] == 5]["id"]

    def get_zs_parent(ou: str) -> str | None:
        """Climb 5 → 4 → 3.

        Returns:
          level 3 parent of level 5 org unit.
        """
        p4 = parent_map.get(ou)
        if not p4:
            return None
        return parent_map.get(p4)

    return [ou for ou in level5 if get_zs_parent(ou) in zs_urban_list]


def apply_formulas_to_extract(
    data: pl.DataFrame,
    formulas: list,
    ou_urban: list,
) -> pl.DataFrame:
    """Applies the given rules to the extract data and computes the results.

    Args:
        data: The extract data as a Polars DataFrame.
        formulas: The list of rules to apply.
        ou_urban: A list of org units which are considered Urban.

    Returns:
        The resulting DataFrame after applying the rules.
    """
    results = []
    for indicator, formula in formulas.items():
        expr = build_expr(formula, ou_urban=ou_urban)

        df = (
            data.group_by(["PERIOD", "ORG_UNIT"])
            .agg(expr.sum().alias("VALUE"))
            .with_columns(pl.lit(indicator).alias("indicator"))
        )

        results.append(df)

    return pl.concat(results)


def build_expr(node: dict, ou_urban: list) -> pl.Expr:
    """Recursively builds a Polars expression from a formula node.

    Args:
        node: A dictionary representing a formula node, which can be a data element, sum, multiply, or constant.
        ou_urban: A list of org units which are considered Urban.

    Returns:
        A Polars expression representing the computation defined by the node.
    """
    if ou_urban is None:
        ou_urban = []

    # Leaf: data element
    if "dataElement" in node:
        return (
            pl.when(
                (pl.col("DX_UID") == node["dataElement"])
                & (pl.col("CATEGORY_OPTION_COMBO") == node["categoryOptionCombo"])
            )
            .then(pl.col("VALUE"))
            .otherwise(0)
        )

    node_type = node["type"]

    if node_type == "sum":
        return sum(build_expr(item, ou_urban) for item in node["items"])

    if node_type == "multiply":
        return build_expr(node["left"], ou_urban) * build_expr(node["right"], ou_urban)

    if node_type == "constant":
        return pl.lit(node["value"])

    if node_type == "if":
        cond = build_condition(node["condition"], ou_check=ou_urban)
        return pl.when(cond).then(build_expr(node["then"], ou_urban)).otherwise(build_expr(node["else"], ou_urban))

    raise NotImplementedError(f"Unsupported node type: {node_type}")


def build_condition(cond: dict, ou_check: list) -> pl.Expr | None:
    """Build a Polars expression for a given condition.

    Args:
        cond: The condition dictionary.
        ou_check: A list of org units.

    Returns:
        The Polars expression representing the condition, or None if not applicable.
    """
    if cond["type"] == "orgUnitInGroupDescendant":
        return pl.col("ORG_UNIT").is_in(ou_check)
    return None


def compute_mean_and_format_results(period_results: pl.DataFrame, period: str) -> pl.DataFrame:
    """Computes the mean of indicator values for each organisation unit and formats the results for output.

    Args:
        period_results: DataFrame containing per-period indicator values to aggregate.
        period: The period string to stamp on the formatted output rows.

    Returns:
        Formatted DataFrame with mean values and required columns for output.
    """
    return (
        period_results.group_by(["ORG_UNIT", "indicator"])
        .agg(pl.col("VALUE").mean().alias("VALUE"))
        .with_columns(
            [
                pl.lit("CMM_INDICATOR").alias("DATA_TYPE"),
                pl.lit(period).alias("PERIOD"),
                pl.lit(None).alias("DX_UID"),
                pl.lit(None).alias("CATEGORY_OPTION_COMBO"),
                pl.lit(None).alias("ATTRIBUTE_OPTION_COMBO"),
                pl.col("indicator").str.to_uppercase().alias("INDICATOR"),
            ]
        )
        .select(
            [
                "DATA_TYPE",
                "DX_UID",
                "PERIOD",
                "CATEGORY_OPTION_COMBO",
                "ATTRIBUTE_OPTION_COMBO",
                "ORG_UNIT",
                "VALUE",
                "INDICATOR",
            ]
        )
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


if __name__ == "__main__":
    dhis2_cmm_morbidity_ds()
