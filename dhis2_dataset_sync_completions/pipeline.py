import logging
import shutil
from pathlib import Path

import pandas as pd
from d2d_library.dataset_completion import DatasetCompletionSync
from openhexa.sdk import current_run, pipeline, workspace
from utils import (
    configure_logging_flush,
    connect_to_dhis2,
    load_configuration,
    read_parquet_extract,
)


@pipeline("dhis2_dataset_sync_completions")
def dhis2_dataset_sync_completions():
    """Main pipeline function for DHIS2 dataset completion synchronization.

    NOTE: This pipeline must run after dhis2_dataset_sync, as it relies on the
        dataset sync extracts to determine which datasets and periods to set as complete.

    NOTE: This pipeline uses the configuration files of dhis2_dataset_sync pipeline.
    """
    pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_dataset_sync_completions"
    try:
        sync_dataset_statuses(pipeline_path=pipeline_path)
    except Exception as e:
        current_run.log_error(f"Dataset completion sync failed: {e}")
        raise


def sync_dataset_statuses(
    pipeline_path: Path,
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
    # logger, logs_file = configure_logging_flush(logs_path=pipeline_path / "logs", task_name="ds_sync") # local testing

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
                    completion_syncer,
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

    except Exception as e:
        raise Exception(f"Error during dataset statuses sync: {e}") from e
    finally:
        save_logs(logs_file, output_dir=pipeline_path / "logs" / "ds_sync")


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
            logging_interval=4000,
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


def save_logs(logs_file: Path, output_dir: Path) -> None:
    """Moves all .log files from logs_path to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if logs_file.is_file():
        dest_file = output_dir / logs_file.name
        shutil.copy(logs_file.as_posix(), dest_file.as_posix())


if __name__ == "__main__":
    dhis2_dataset_sync_completions()
