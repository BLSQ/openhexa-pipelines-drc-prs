import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run, workspace
from openhexa.toolbox.dhis2 import DHIS2


def connect_to_dhis2(connection_str: str, cache_dir: Path) -> DHIS2:
    """Establishes a connection to DHIS2 using the provided connection string and cache directory.

    Parameters
    ----------
    connection_str : str
        The connection string for DHIS2.
    cache_dir : Path
        The directory to use for caching DHIS2 data.

    Returns
    -------
    DHIS2
        An instance of the DHIS2 client.

    Raises
    ------
    Exception
        If there is an error while connecting to DHIS2.
    """
    try:
        connection = workspace.dhis2_connection(connection_str)
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 connection: {connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {connection_str}: {e}") from e


def load_pipeline_config(config_dir: Path) -> dict:
    """Load the main pipeline configuration.
    
    Loads extract_config.json and push_settings.json and merges them.
    
    Args:
        config_dir: Path to the configuration directory.
        
    Returns:
        dict: Merged configuration with CONNECTIONS, EXTRACTION, EXTRACTS, and PUSH_SETTINGS.
    """
    extract_config_path = config_dir / "extract_config.json"
    push_settings_path = config_dir / "push_settings.json"
    
    # Load extract config
    with Path.open(extract_config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    # Load push settings
    if push_settings_path.exists():
        with Path.open(push_settings_path, "r", encoding="utf-8") as f:
            config["PUSH_SETTINGS"] = json.load(f)
    else:
        config["PUSH_SETTINGS"] = {}
    
    current_run.log_info(f"Config loaded from {config_dir}.")
    return config


def get_extract_config(config: dict, extract_id: str) -> dict | None:
    """Get configuration for a specific extract.
    
    Args:
        config: Pipeline configuration dict.
        extract_id: The extract ID to find.
        
    Returns:
        Extract configuration dict or None if not found.
    """
    for extract in config.get("EXTRACTS", []):
        if extract.get("EXTRACT_ID") == extract_id:
            return extract
    return None


def save_to_parquet(data: pl.DataFrame, filename: Path) -> None:
    """Safely saves a DataFrame to a Parquet file using a temporary file and atomic replace.

    Args:
        data (pl.DataFrame): The DataFrame to save.
        filename (Path): The path where the Parquet file will be saved.
    """
    try:
        if not isinstance(data, pl.DataFrame):
            raise TypeError("The 'data' parameter must be a polars DataFrame.")

        # Write to a temporary file in the same directory
        with tempfile.NamedTemporaryFile(suffix=".parquet", dir=filename.parent, delete=False) as tmp_file:
            temp_filename = Path(tmp_file.name)
            data.write_parquet(temp_filename)

        # Atomically replace the old file with the new one
        temp_filename.replace(filename)

    except Exception as e:
        # Clean up the temp file if it exists
        if "temp_filename" in locals() and temp_filename.exists():
            temp_filename.unlink()
        raise RuntimeError(f"Failed to save parquet file to {filename}") from e


def configure_logging(logs_path: Path, task_name: str):
    """Configure logging for the pipeline.

    Parameters
    ----------
    logs_path : Path
        Directory path where log files will be stored.
    task_name : str
        Name of the task to include in the log filename.

    This function creates the log directory if it does not exist and sets up logging to a file.
    """
    # Configure logging
    logs_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=logs_path / f"{task_name}_{now}.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


def load_drug_mapping(config_dir: Path, mapping_file: str) -> tuple[dict, list]:
    """Load drug mapping file and convert to MAPPINGS format expected by the pipeline.
    
    The drug mapping file has this structure:
    {
        "Drug Name": {
            "stock_entry_fosa": {"UID": "xxx", "COC": "yyy"},
            "stock_start_fosa": {"UID": "xxx", "COC": "zzz"},
            ...
        },
        ...
    }
    
    This function converts it to the MAPPINGS format:
    {
        "UID_1": {
            "CATEGORY_OPTION_COMBO": {"coc1": "coc1", "coc2": "coc2", ...},
            "ATTRIBUTE_OPTION_COMBO": {}
        },
        ...
    }
    
    Parameters
    ----------
    config_dir : Path
        Path to the configuration directory containing the mapping file.
    mapping_file : str
        Name of the drug mapping file (e.g., "drug_mapping_fosa.json").
        
    Returns
    -------
    tuple[dict, list]
        A tuple containing:
        - MAPPINGS dict in the format expected by exhaustivity_calculation
        - List of all unique UIDs (for extract_config.UIDS)
    """
    mapping_path = config_dir / mapping_file
    
    if not mapping_path.exists():
        logging.warning(f"Drug mapping file not found: {mapping_path}")
        return {}, []
    
    try:
        with Path.open(mapping_path, "r", encoding="utf-8") as f:
            drug_mapping = json.load(f)
    except Exception as e:
        logging.error(f"Error loading drug mapping file {mapping_path}: {e}")
        return {}, []
    
    # Convert to MAPPINGS format
    # Group by UID, collect all COCs for each UID
    uid_to_cocs: dict[str, set] = {}
    
    for drug_name, indicators in drug_mapping.items():
        for indicator_name, indicator_data in indicators.items():
            uid = indicator_data.get("UID")
            coc = indicator_data.get("COC")
            
            if uid and coc:
                if uid not in uid_to_cocs:
                    uid_to_cocs[uid] = set()
                uid_to_cocs[uid].add(coc)
    
    # Build MAPPINGS format
    mappings = {}
    for uid, cocs in uid_to_cocs.items():
        # COC source -> COC target (same value, no transformation)
        coc_mapping = {coc: coc for coc in cocs}
        mappings[uid] = {
            "CATEGORY_OPTION_COMBO": coc_mapping,
            "ATTRIBUTE_OPTION_COMBO": {}
        }
    
    # Get list of unique UIDs
    uids = list(uid_to_cocs.keys())
    
    logging.info(f"Loaded drug mapping from {mapping_file}: {len(uids)} UIDs, {sum(len(c) for c in uid_to_cocs.values())} COC mappings")
    
    return mappings, uids


