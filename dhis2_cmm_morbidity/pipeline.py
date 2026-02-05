import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from d2d_library.db_queue import Queue
from d2d_library.dhis2.extract import DHIS2Extractor
from d2d_library.dhis2.org_unit_aligner import DHIS2PyramidAligner
from d2d_library.dhis2.push import DHIS2Pusher
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_unit_groups
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    dhis2_request,
    get_periods,
    load_configuration,
    read_parquet_extract,
    save_logs,
    save_to_parquet,
    select_descendants,
)

# Ticket(s) related to this pipeline:
#   - https://bluesquare.atlassian.net/browse/SAN-126
# github repo:
#   -https://github.com/BLSQ/openhexa-pipelines-drc-prs


@pipeline("dhis2_cmm_morbidity", timeout=21600)  # 3600 * 6 hours
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
def dhis2_cmm_morbidity(run_ou_sync: bool, run_extract_data: bool, run_push_data: bool):
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
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_cmm_morbidity"

    try:
        pyramid_ready = sync_organisation_units(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,
        )

        ou_groups_ready = sync_organisation_unit_groups(
            pipeline_path=pipeline_path,
            run_task=run_ou_sync,  # only run if OU sync ran
            wait=pyramid_ready,
        )

        extract_data(
            pipeline_path=pipeline_path,
            run_task=run_extract_data,
            wait=ou_groups_ready,
        )

        push_data(
            pipeline_path=pipeline_path,
            run_task=run_push_data,
            wait=ou_groups_ready,
        )

    except Exception as e:
        current_run.log_error(f"An error occurred: {e}")
        raise


@dhis2_cmm_morbidity.task
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
        logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="org_units_sync")

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
            logger=logger,
        )

    except Exception as e:
        raise Exception(f"Error during pyramid sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "org_units")

    return True


def align_org_units(
    pipeline_path: Path,
    source_dhis2: DHIS2,
    target_dhis2: DHIS2,
    source_org_units_selection: list,
    include_children: bool,
    limit_level: int | None,
    dry_run: bool = True,
    logger: logging.Logger | None = None,
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
    logger : logging.Logger, optional
        Logger instance for logging (default is None).
    """
    extract_pyramid(
        dhis2_client=source_dhis2,
        limit_level=limit_level,
        org_units_selection=source_org_units_selection,
        include_children=include_children,
        output_dir=pipeline_path / "data" / "pyramid",
        filename="pyramid_data.parquet",
    )

    DHIS2PyramidAligner(logger).align_to(
        target_dhis2=target_dhis2,
        source_pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
        dry_run=dry_run,
    )


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


@dhis2_cmm_morbidity.task
def sync_organisation_unit_groups(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
) -> bool:
    """Updates the organisation units of datasets in the PRS DHIS2 instance.

    NOTE: This is PRS specific.

    Returns
    -------
    bool
        True if the update was performed, False if skipped.
    """
    if not run_task:
        current_run.log_info("Update organisation unit groups task skipped.")
        return True

    try:
        current_run.log_info("Starting update of organisation unit groups.")

        configure_logging_flush(logs_path=pipeline_path / "logs" / "org_unit_groups", task_name="sync_oug")
        config_extract = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        config_push = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
        config_sync = load_configuration(config_path=pipeline_path / "configuration" / "sync_config.json")
        snis_conn = config_extract["SETTINGS"].get("SOURCE_DHIS2_CONNECTION")
        prs_conn = config_push["SETTINGS"].get("TARGET_DHIS2_CONNECTION")

        oug_to_sync = config_sync.get("ORG_UNIT_GROUPS", {})
        for oug_name, oug_target_list in oug_to_sync.items():
            for oug_target in oug_target_list:
                current_run.log_info(f"Syncing organisation unit group. Source: {oug_name} to target: {oug_target}")
                sync_org_units_groups(
                    dhis2_client_source=connect_to_dhis2(connection_str=snis_conn, cache_dir=None),
                    dhis2_client_target=connect_to_dhis2(connection_str=prs_conn, cache_dir=None),
                    source_oug_id=oug_name,
                    target_oug_id=oug_target,
                    dry_run=config_push["SETTINGS"].get("DRY_RUN", True),
                    pyramid=read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet"),
                )

    except Exception as e:
        current_run.log_error("An error occurred during dataset org units update. Process stopped.")
        logging.error(f"An error occurred during dataset org units update: {e}")
        raise

    return True


def sync_org_units_groups(
    dhis2_client_source: DHIS2,
    dhis2_client_target: DHIS2,
    source_oug_id: str,
    target_oug_id: str,
    dry_run: bool = True,
    pyramid: pl.DataFrame | None = None,
    validation_level: int | None = 3,
) -> dict:
    """Syncs organisation unit groups between source and target datasets in DHIS2.

    NOTE: This is PRS specific.

    Parameters
    ----------
    dhis2_client_source : DHIS2
        DHIS2 client for the source instance.
    dhis2_client_target : DHIS2
        DHIS2 client for the target instance.
    source_oug_id : str
        The ID of the dataset from where to retrieve the org unit ids.
    target_oug_id : str
        The ID of the dataset to be updated.
    dry_run : bool, optional
        If True, performs a dry run without making changes (default is True).
    pyramid : pl.DataFrame | None, optional
        Optional pyramid dataframe to validate org units (default is None).
    validation_level : int | None, optional
        Level to validate org units against the pyramid (default is 3).

    Returns
    -------
    dict
        The response from the DHIS2 API, or an error payload.
    """
    oug_source = get_organisation_unit_groups(dhis2_client_source)
    source_oug = oug_source.filter(pl.col("id").is_in([source_oug_id]))
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
        params={"dryRun": str(dry_run).lower()},
    )

    if "error" in update_response:
        current_run.log_info(f"Error updating organisation units group {target_oug_id}: {update_response['error']}")
        logging.error(f"Error updating organisation units group {target_oug_id}: {update_response['error']}")
    else:
        msg = f"organisation unit group '{oug_payload['name']}' ({target_oug_id}) org units set: {len(source_ous)}"
        current_run.log_info(msg)
        logging.info(msg)

    return update_response


@dhis2_cmm_morbidity.task
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
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    cmm_config = load_configuration(config_path=pipeline_path / "configuration" / "cmm_config.json")
    dhis2_client = connect_to_dhis2(
        connection_str=extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None
    )

    # NOTE: We need the filtered source pyramid to validate the org units (aligned org units).
    source_pyramid = read_parquet_extract(pipeline_path / "data" / "pyramid" / "pyramid_data.parquet")

    # initialize queue
    db_path = pipeline_path / "configuration" / ".queue.db"
    push_queue = Queue(db_path)

    download_settings = extract_config["SETTINGS"].get("MODE", None)
    if download_settings is None:
        download_settings = "DOWNLOAD_REPLACE"
        current_run.log_warning(f"No 'MODE' found in extraction settings. Set default: {download_settings}")

    # limits
    dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 100
    dhis2_client.data_value_sets.MAX_ORG_UNITS = 100

    # Setup extractor (See docs about return_existing_file impact)
    dhis2_extractor = DHIS2Extractor(
        dhis2_client=dhis2_client, download_mode=download_settings, return_existing_file=False
    )

    current_run.log_info(f"Download MODE: {download_settings}")
    handle_data_element_extracts(
        pipeline_path=pipeline_path,
        dhis2_extractor=dhis2_extractor,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        source_pyramid=source_pyramid,
    )

    level5_under_zs = get_fosa_descendants_of_zs(
        source_pyramid, dhis2_client, oug_id=cmm_config.get("OUG_URBAN", "cOK4Feyi0nP")
    )

    compute_cmm_morbidity_indicators(
        pipeline_path=pipeline_path,
        data_element_extracts=extract_config["DATA_ELEMENTS"].get("EXTRACTS", []),
        cmm_extracts=cmm_config.get("EXTRACTS", []),
        org_units_urban=level5_under_zs,
        push_queue=push_queue,
    )


def get_fosa_descendants_of_zs(pyramid: pl.DataFrame, dhis2_client: DHIS2, oug_id: str) -> list:
    """Retrieves the list of FOSA organisation units that are descendants of urban Zones de sante.

    Parameters
    ----------
    pyramid : pl.DataFrame
        The organisation units pyramid as a Polars DataFrame.
    dhis2_client : DHIS2
        The DHIS2 client instance.
    oug_id : str
        The organisation unit group ID for urban Zones de sante.

    Returns
    -------
    list
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


def handle_data_element_extracts(
    pipeline_path: Path,
    dhis2_extractor: DHIS2Extractor,
    data_element_extracts: list,
    source_pyramid: pd.DataFrame,
):
    """Handles data elements extracts based on the configuration."""
    if len(data_element_extracts) == 0:
        current_run.log_info("No data elements to extract.")
        return

    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="extract_data")
    current_run.log_info("Starting data element extracts.")
    try:
        # loop over the available extract configurations
        for idx, extract in enumerate(data_element_extracts):
            extract_id = extract.get("EXTRACT_UID")
            org_units_level = extract.get("ORG_UNITS_LEVEL", None)
            data_element_uids = extract.get("UIDS", [])
            start = extract.get("START_PERIOD", [])
            end = extract.get("END_PERIOD", [])

            # get periods
            start, end = resolve_extraction_window(extract)
            cmm_window = extract.get("CMM_WINDOW_MONTHS", 6)
            start_cmm = (datetime.strptime(start, "%Y%m") - relativedelta(months=cmm_window)).strftime("%Y%m")
            extract_periods = get_periods(start_cmm, end)

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
                f"for {len(data_element_uids)} data elements across {len(org_units)} org units "
                f"(level {org_units_level}) for period: {start_cmm} - {end}."
            )

            # run data elements extraction per period
            for period in extract_periods:
                try:
                    dhis2_extractor.data_elements.download_period(
                        data_elements=data_element_uids,
                        org_units=org_units,
                        period=period,
                        output_dir=pipeline_path / "data" / "extracts" / "data_elements" / f"extract_{extract_id}",
                    )

                except Exception as e:
                    current_run.log_warning(
                        f"Extract {extract_id} download failed for period {period}, skipping to next extract."
                    )
                    logger.error(f"Extract {extract_id} - period {period} error: {e}")
                    break  # skip to next extract

            current_run.log_info(f"Extract {extract_id} finished.")

    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "extract")


def compute_cmm_morbidity_indicators(
    pipeline_path: Path,
    data_element_extracts: list,
    cmm_extracts: list,
    org_units_urban: list,
    push_queue: Queue,
):
    """Computes CMM morbidity indicators based on the extracted data elements."""
    data_source_path = pipeline_path / "data" / "extracts" / "data_elements"
    data_output_path = pipeline_path / "data" / "cmm_morbidity"

    try:
        for extract in data_element_extracts:
            start, end = resolve_extraction_window(extract)
            extract_periods = get_periods(start, end)
            extract_uid = extract.get("EXTRACT_UID")
            cmm_window = extract.get("CMM_WINDOW_MONTHS", 6)

            for period in extract_periods:
                formulas = get_formulas_for_extract(extract_uid, cmm_extracts)

                cmm_start = (datetime.strptime(period, "%Y%m") - relativedelta(months=cmm_window)).strftime("%Y%m")
                cmm_end = (datetime.strptime(period, "%Y%m") - relativedelta(months=1)).strftime("%Y%m")
                current_run.log_info(
                    f"Computing CMM period: {period} - window extracts: {cmm_window} ({cmm_start} to {cmm_end})"
                )

                # retrieve the corresponding cmm extract per period
                cmm_periods = get_periods(cmm_start, cmm_end)
                cmm_results = []
                for cmm_period in cmm_periods:
                    extract_path = data_source_path / f"extract_{extract_uid}" / f"data_{cmm_period}.parquet"

                    try:
                        extract_data = read_parquet_extract(extract_path, engine="polars")
                    except FileNotFoundError:
                        current_run.log_warning(
                            f"Extract data file not found: {extract_path.name}, skipping CMM period {period}."
                        )
                        cmm_results = []
                        break

                    # To numeric
                    extract_data = extract_data.with_columns(pl.col("VALUE").cast(pl.Float64, strict=False))

                    # Calculate CMM indicators for period
                    period_results = apply_formulas_to_extract(extract_data, formulas, ou_urban=org_units_urban)
                    cmm_results.append(period_results)

                if cmm_results == []:
                    break  # skip to next extract

                cmm_result_period = pl.concat(cmm_results)
                cmm_morbidity = compute_mean_and_format_results(cmm_result_period, period)

                # save results
                extract_path = data_output_path / f"extract_{extract_uid}" / f"cmm_morbidity_{period}.parquet"
                extract_path.parent.mkdir(parents=True, exist_ok=True)
                cmm_morbidity.write_parquet(extract_path)
                current_run.log_info(f"CMM morbidity indicators saved: {extract_path.name}")

                # queue for push
                push_queue.enqueue(f"{extract_uid}|{extract_path}")

    except Exception as e:
        current_run.log_error(f"Error computing CMM morbidity indicators: {e}")
        raise
    finally:
        push_queue.enqueue("FINISH")


def resolve_extraction_window(settings: dict) -> tuple[str, str]:
    """Returns (start_yyyymm, end_yyyymm) based on settings dict.

    Returns
    -------
    tuple[str, str]
        A tuple containing the start and end dates in 'YYYYMM' format.
    """
    try:
        months_lag = settings.get("NUMBER_MONTHS_WINDOW", 3)
        if settings.get("START_PERIOD"):
            start = settings["START_PERIOD"]
        else:
            start = (datetime.now() - relativedelta(months=months_lag)).strftime("%Y%m")

        if settings.get("END_PERIOD"):
            end = settings["END_PERIOD"]
        else:
            end = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")

        return start, end

    except Exception as e:
        raise ValueError(f"Invalid STARTDATE / ENDDATE configuration: {e}") from e


def get_formulas_for_extract(extract_uid: str, cmm_extracts: list) -> list:
    """Returns the list of rules for the matching extract UID.

    Parameters
    ----------
    extract_uid : str
        The UID of the extract.
    cmm_extracts : list
        The list of all cmm extract formulas.

    Returns
    -------
    list
        A list of rules corresponding to the given extract UID.
    """
    for rule in cmm_extracts:
        if rule.get("EXTRACT_UID") == extract_uid:
            return rule.get("FORMULAS", [])
    return []


def apply_formulas_to_extract(
    extract_data: pl.DataFrame,
    extract_formulas: list,
    ou_urban: list,
) -> pl.DataFrame:
    """Applies the given rules to the extract data and computes the results.

    Parameters
    ----------
    extract_data : pl.DataFrame
        The extract data as a Polars DataFrame.
    extract_formulas : list
        The list of rules to apply.
    ou_urban : list, optional
        A list of org units which are considered Urban.

    Returns
    -------
    pl.DataFrame
        The resulting DataFrame after applying the rules.
    """
    results = []
    for indicator, formula in extract_formulas.items():
        expr = build_expr(formula, ou_urban=ou_urban)

        df = (
            extract_data.group_by(["PERIOD", "ORG_UNIT"])
            # .agg(expr.alias("VALUE"))
            .agg(expr.sum().alias("VALUE"))
            .with_columns(pl.lit(indicator).alias("indicator"))
        )

        results.append(df)

    return pl.concat(results)


def build_expr(node: dict, ou_urban: list) -> pl.Expr:
    """Recursively builds a Polars expression from a formula node.

    Parameters
    ----------
    node : dict
        A dictionary representing a formula node, which can be a data element, sum, multiply, or constant.
    ou_urban : list
        A list of org units which are considered Urban.

    Returns
    -------
    pl.Expr
        A Polars expression representing the computation defined by the node.

    Raises
    ------
    NotImplementedError
        If the node type is not supported.
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

    Parameters
    ----------
    cond : dict
        The condition dictionary.
    ou_check : list
        A list of org units.

    Returns
    -------
    pl.Expr | None
        The Polars expression representing the condition, or None if not applicable.
    """
    if cond["type"] == "orgUnitInGroupDescendant":
        return pl.col("ORG_UNIT").is_in(ou_check)
    return None


def compute_mean_and_format_results(period_results: pl.DataFrame, period: str) -> pl.DataFrame:
    """Computes the mean of indicator values for each organisation unit and formats the results for output.

    Returns
    -------
    pl.DataFrame
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


@dhis2_cmm_morbidity.task
def push_data(
    pipeline_path: Path,
    run_task: bool = True,
    wait: bool = True,
) -> None:
    """Pushes data elements to the target DHIS2 instance."""
    if not run_task:
        current_run.log_info("Data push task skipped.")

    current_run.log_info("Starting data push.")

    # setup
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="push_data")
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
    )

    # Map data types to their respective mapping functions
    # NOTE: cmm specific mappings
    dispatch_map = {
        "CMM_INDICATOR": (config["CMM_INDICATORS"]["EXTRACTS"], apply_cmm_indicators_extract_config),
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
            pusher.push_data(df_data=df_mapped)

            # Success → dequeue
            push_queue.dequeue()
            current_run.log_info(f"Data push finished for extract: {extract_path.name}.")

        except Exception as e:
            current_run.log_error(
                f"Fatal error for extract {extract_id} ({extract_path.name}), stopping push process. Error: {e!s}"
            )
            raise  # crash on error

        finally:
            save_logs(logs_file, output_dir=pipeline_path / "logs" / "push")


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


def apply_cmm_indicators_extract_config(
    df: pd.DataFrame, extract_config: dict, logger: logging.Logger | None = None
) -> pd.DataFrame:
    """Applies data element mappings to the CMM indicators.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the extracted data.
    extract_config : dict
        This is a dictionary containing the extract mappings.
    logger : logging.Logger, optional
        Logger instance for logging (default is None).

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
    for uid, mapping in extract_mappings.items():
        uid_mapping = mapping.get("DX_UID")
        coc_mapping = mapping.get("CATEGORY_OPTION_COMBO")
        aoc_mapping = mapping.get("ATTRIBUTE_OPTION_COMBO")

        # select indicator data
        df_indicator = df[df["INDICATOR"] == uid].copy()
        if coc_mapping:
            df_indicator["CATEGORY_OPTION_COMBO"] = coc_mapping.strip()

        if aoc_mapping:
            df_indicator["ATTRIBUTE_OPTION_COMBO"] = aoc_mapping.strip()

        if uid_mapping:
            df_indicator["DX_UID"] = uid_mapping.strip()

        chunks.append(df_indicator)

    if len(chunks) == 0:
        current_run.log_warning("No data elements matched the provided mappings, returning empty dataframe.")
        logger.warning("No data elements matched the provided mappings, returning empty dataframe.")
        return pd.DataFrame(columns=df.columns)

    df_mapped = pd.concat(chunks, ignore_index=True)
    df_mapped["VALUE"] = (
        df_mapped["VALUE"]
        .where(df_mapped["VALUE"].abs() >= 1e-9, 0)  # kill float noise
        .round(4)  # round to 4 decimal places
    )
    return df_mapped.sort_values(by="ORG_UNIT")


if __name__ == "__main__":
    dhis2_cmm_morbidity()
