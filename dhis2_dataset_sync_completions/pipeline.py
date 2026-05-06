import logging
from pathlib import Path

import pandas as pd
from d2d_library.dataset_completion import DatasetCompletionSync
from openhexa.sdk import current_run, parameter, pipeline, workspace
from utils import configure_logging_flush, connect_to_dhis2, load_configuration, read_parquet_extract, save_logs


@pipeline("dhis2_dataset_sync_completions", timeout=43200)
@parameter(
    "start_date",
    name="Start date",
    help="Start date for dataset completion sync in YYYY-MM-DD format.",
    default=None,
    required=False,
    type=str,
)
@parameter(
    "end_date",
    name="End date",
    help="End date for dataset completion sync in YYYY-MM-DD format.",
    default=None,
    required=False,
    type=str,
)
def dhis2_dataset_sync_completions(start_date: str, end_date: str):
    """Main pipeline function for DHIS2 dataset completion synchronization.

    NOTE: This pipeline must run after dhis2_dataset_sync, as it relies on the
        dataset sync extracts to determine which datasets and periods to set as complete.

    NOTE: This pipeline uses the configuration files of dhis2_dataset_sync pipeline.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_dataset_sync_completions"
    try:
        sync_dataset_statuses(pipeline_path=pipeline_path, start_date=start_date, end_date=end_date)
    except Exception as e:
        current_run.log_error(f"Dataset completion sync failed: {e}")
        raise


def sync_dataset_statuses(
    pipeline_path: Path,
    start_date: str,
    end_date: str,
):
    """Syncs dataset statuses between source and target DHIS2 instances.

    :param pipeline_path: Description
    :type pipeline_path: Path
    """
    current_run.log_info("Starting dataset statuses sync.")

    # Use dhis2_dataset_sync configuration paths
    dhis2_dataset_sync_path = pipeline_path.parent / "dhis2_dataset_sync"  # Master pipeline folder
    source_config_path = dhis2_dataset_sync_path / "configuration" / "extract_config.json"
    target_config_path = dhis2_dataset_sync_path / "configuration" / "push_config.json"

    # DHIS2 source
    current_run.log_info(f"Connecting to source DHIS2 instance config file: {source_config_path}.")
    source_config = load_configuration(config_path=source_config_path)
    source_dhis2 = connect_to_dhis2(connection_str=source_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"], cache_dir=None)

    # DHIS2 target
    current_run.log_info(f"Connecting to target DHIS2 instance config file: {target_config_path}.")
    target_config = load_configuration(config_path=target_config_path)
    target_dhis2 = connect_to_dhis2(connection_str=target_config["SETTINGS"]["TARGET_DHIS2_CONNECTION"], cache_dir=None)

    # setup logger
    logger, logs_file = configure_logging_flush(logs_path=Path("/home/jovyan/tmp/logs"), task_name="ds_sync")

    # Push parameters
    import_strategy = target_config["SETTINGS"].get("IMPORT_STRATEGY", "CREATE_AND_UPDATE")
    dry_run = target_config["SETTINGS"].get("DRY_RUN", True)

    # Create syncer instance
    completion_syncer = DatasetCompletionSync(
        source_dhis2=source_dhis2,
        target_dhis2=target_dhis2,
        import_strategy=import_strategy,
        dry_run=dry_run,
        logger=logger,
    )

    # Find the extract config based on the dataset sync folder name
    # Search for the extracts created by the master pipeline dhis2_dataset_sync
    ds_sync_dir = dhis2_dataset_sync_path / "data" / "dataset_sync"
    current_run.log_info(f"Searching for source extracts in folder: {ds_sync_dir}.")
    ds_extracts = filter_extract_configs_by_folder(ds_sync_dir, target_config)  # based on folder names

    try:
        for extract_config in ds_extracts:
            extract_identification = extract_config.get("EXTRACT_UID")
            if not extract_identification:
                raise ValueError(f"Extract configuration is missing 'EXTRACT_UID': {extract_config}")
            ds_extract_dir = ds_sync_dir / extract_identification
            # NOTE: Temp solution -> needs validation of periods
            if start_date and end_date:
                files = sorted(list(ds_extract_dir.glob("ds_sync_*.parquet")))
                files = filter_files_by_period(files=files, period_start=start_date, period_end=end_date)
                current_run.log_info(
                    f"Processing extract '{extract_identification}' with date "
                    f"filtering: {start_date} to {end_date} ({len(files)} files matched)."
                )
            else:
                sync_window = extract_config.get("SYNC_PERIOD_WINDOW", None)
                files = sorted(list(ds_extract_dir.glob("ds_sync_*.parquet")))
                files = files[-sync_window:] if sync_window else files
                current_run.log_info(
                    f"Processing extract '{extract_identification}': {len(files)} period files to synchronize."
                )

            # Create processed directory for this extract
            processed_dir = pipeline_path / "data" / "dataset_sync_processed" / extract_identification
            processed_dir.mkdir(parents=True, exist_ok=True)

            for file in files:
                period = file.stem.replace("ds_sync_", "")

                # Set dataset competion for all org units for this period
                handle_dataset_completion(
                    syncer=completion_syncer,
                    source_ds_id=extract_config.get("SOURCE_DATASET_UID"),
                    target_ds_id=extract_config.get("TARGET_DATASET_UID"),
                    dhis2_pyramid=read_parquet_extract(
                        dhis2_dataset_sync_path / "data" / "pyramid" / "pyramid_data.parquet"
                    ),
                    period=period,
                    ds_sync_fname=file,
                    ds_processed=processed_dir,
                    logger=logger,
                )
                # Save the reporting logs after each sync period
                save_logs(logs_file, output_dir=pipeline_path / "logs" / "ds_sync")

        current_run.log_info("Dataset statuses sync completed successfully.")
    except Exception as e:
        raise Exception(f"Error during dataset statuses sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "ds_sync")


def filter_files_by_period(
    files: list[Path],
    period_start: str,
    period_end: str,
) -> list[Path]:
    """Filter a list of files by period range extracted from the filename.

    Expects filenames in the format `<prefix>_<period>.parquet` where period
    is a string like '202601'.

    Parameters
    ----------
    files : list[Path]
        List of file paths to filter.
    period_start : str
        Start period (inclusive), e.g. '202601'.
    period_end : str
        End period (inclusive), e.g. '202603'.

    Returns
    -------
    list[Path]
        Files whose period falls within [period_start, period_end].
    """

    def extract_period(path: Path) -> str:
        return path.stem.split("_")[-1]

    return [f for f in files if period_start <= extract_period(f) <= period_end]


def handle_dataset_completion(
    syncer: DatasetCompletionSync,
    source_ds_id: str,
    target_ds_id: str,
    dhis2_pyramid: pd.DataFrame,
    period: str,
    ds_sync_fname: Path,
    ds_processed: Path,
    logger: logging.Logger,
) -> None:
    """Sets datasets as complete for the pushed periods.

    This is a placeholder function and should be implemented based on specific requirements.
    """
    if not source_ds_id:
        return
    if not target_ds_id:
        return
    if not ds_sync_fname:
        current_run.log_warning("No dataset sync file provided for completion sync, skipping.")
        return

    try:
        df = pd.read_parquet(ds_sync_fname)
        if "ORG_UNIT" not in df.columns:
            raise KeyError(f"'ORG_UNIT' column not found in {ds_sync_fname}")
        org_units_to_sync = df["ORG_UNIT"].unique().tolist()
    except Exception as e:
        msg = f"Error loading the dataset org units file to sync: {ds_sync_fname}. DS sync skipped."
        logger.error(msg + f" Error: {e}")
        current_run.log_info(msg)
        return

    pyramid_level = 2  # Province level, we want parent ids to build the completions table (cache completions)
    province_uids = dhis2_pyramid[dhis2_pyramid["level"] == pyramid_level]["id"].to_list()
    current_run.log_info(f"Building source completion table for {len(province_uids)} OUs level 2 period {period}.")
    try:
        syncer.sync(
            source_dataset_id=source_ds_id,
            target_dataset_id=target_ds_id,
            org_units=org_units_to_sync,
            parent_ou=province_uids,
            period=period,
            saving_interval=1000,
            ds_processed_path=ds_processed,
        )
    except Exception as e:
        current_run.log_error(f"Error setting completions for dataset {target_ds_id}, period {period} - Error: {e!s}.")
        raise


def filter_extract_configs_by_folder(ds_sync_path: Path, config: dict) -> list:
    """Retrieves the dataset extract configurations.

    Returns the config for whos EXTRACT_UID matches the folder names in the dataset_sync directory.

    Parameters
    ----------
    ds_sync_path : Path
        Path to the datasets directory.
    config : dict
        Configuration dictionary containing dataset extract information.

    Returns
    -------
    list
        List of extract configuration dictionaries matching the dataset_sync folder name.
    """
    folder_names = {f.name for f in ds_sync_path.glob("*") if f.is_dir()}

    return [
        extract
        for extract in config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
        if extract.get("EXTRACT_UID") in folder_names
    ]


if __name__ == "__main__":
    dhis2_dataset_sync_completions()
