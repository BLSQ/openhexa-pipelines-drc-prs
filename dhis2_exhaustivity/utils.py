import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import requests
from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from requests.exceptions import HTTPError, RequestException

# Try to import current_run for logging, fallback to standard logging
try:
    from openhexa.sdk import current_run as _current_run
    _has_current_run = True
except ImportError:
    _has_current_run = False
    _current_run = None


def log_msg(
    msg: str,
    level: str = "info",
    logger: logging.Logger | None = None,
    logger_name: str | None = None,
) -> None:
    """Unified logging function that uses current_run.log_* if available, otherwise standard logging.
    
    This function provides a consistent way to log messages across the pipeline,
    automatically using OpenHexa's current_run when available (visible in UI),
    or falling back to standard Python logging.
    
    Parameters
    ----------
    msg : str
        The message to log.
    level : str, optional
        Log level: "info", "warning", "error", "debug". Default is "info".
    logger : logging.Logger, optional
        Optional logger instance to use. If None, a logger will be created.
    logger_name : str, optional
        Name for the logger if logger is None. Defaults to module name.
    
    Examples
    --------
    >>> log_msg("Processing data...", level="info")
    >>> log_msg("Warning: missing data", level="warning")
    >>> log_msg("Error occurred", level="error", logger_name="my_module")
    """
    # Use current_run if available (OpenHexa UI)
    if _has_current_run and _current_run:
        if level == "info":
            _current_run.log_info(msg)
        elif level == "warning":
            _current_run.log_warning(msg)
        elif level == "error":
            _current_run.log_error(msg)
        elif level == "debug":
            # current_run doesn't have log_debug, use logging
            pass
        else:
            # Unknown level, default to info
            _current_run.log_info(msg)
    
    # Also log to standard logger (for background logs)
    if logger is None:
        logger = logging.getLogger(logger_name or __name__)
    
    level_lower = level.lower()
    if level_lower == "info":
        logger.info(msg)
    elif level_lower == "warning":
        logger.warning(msg)
    elif level_lower == "error":
        logger.error(msg)
    elif level_lower == "debug":
        logger.debug(msg)
    else:
        logger.info(msg)  # Default to info for unknown levels


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
        # Connection is implicit - no need to log every time
        return DHIS2(connection=connection, cache_dir=cache_dir)
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {connection_str}: {e}") from e


def validate_drug_mapping_files(pipeline_path: Path) -> None:
    """Validate drug_mapping files and raise errors if validation fails.
    
    With the new pipeline_config.json structure, UIDs are loaded dynamically
    from drug_mapping files at runtime. This function validates the files exist
    and contain valid data.
    
    Args:
        pipeline_path: Path to the pipeline directory.
    
    Raises:
        FileNotFoundError: If extract_config.json is not found.
        ValueError: If drug_mapping files are missing or invalid.
    """
    config_dir = pipeline_path / "configuration"
    extract_config_path = config_dir / "extract_config.json"
    
    if not extract_config_path.exists():
        raise FileNotFoundError(
            f"extract_config.json not found: {extract_config_path}. "
            "Cannot proceed without configuration file."
        )
    
    # Load configuration
    config = load_configuration(extract_config_path)
    
    log_msg("Validating drug_mapping files...", level="info", logger_name="utils")
    
    errors = []
    warnings = []
    
    extracts = config.get("EXTRACTS", [])
    if not extracts:
        raise ValueError("No EXTRACTS defined in extract_config.json")
    
    for extract in extracts:
        extract_id = extract.get("EXTRACT_ID") or extract.get("EXTRACT_UID", "Unknown")
        
        # If UIDS are provided directly, drug_mapping_file validation is optional
        if extract.get("UIDS"):
            log_msg(
                f"[{extract_id}] Using UIDS from config ({len(extract.get('UIDS', []))} UIDs) - OK",
                level="info",
                logger_name="utils"
            )
            continue
        
        # Get mapping file path for this extract
        mapping_file = get_mapping_file_for_extract(extract_id)
        mapping_path = config_dir / mapping_file
        
        if not mapping_path.exists():
            errors.append(
                f"[{extract_id}] Drug mapping file not found: {mapping_file} "
                f"(expected at {mapping_path})"
            )
            continue
        
        # Load and validate UIDs from mapping file
        try:
            with mapping_path.open("r", encoding="utf-8") as f:
                drug_mapping = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(
                f"[{extract_id}] Invalid JSON in {mapping_file}: {e}"
            )
            continue
        except Exception as e:
            errors.append(
                f"[{extract_id}] Error reading {mapping_file}: {e}"
            )
            continue
        
        # Extract UIDs from mapping
        uids = set()
        for drug_name, indicators in drug_mapping.items():
            if not isinstance(indicators, dict):
                warnings.append(
                    f"[{extract_id}] Invalid structure in {mapping_file}: "
                    f"'{drug_name}' is not a dictionary"
                )
                continue
            for _, indicator_data in indicators.items():
                if not isinstance(indicator_data, dict):
                    continue
                uid = indicator_data.get("UID")
                if uid:
                    uids.add(uid)
        
        uids_list = sorted(list(uids))
        
        if not uids_list:
            errors.append(
                f"[{extract_id}] No UIDs found in {mapping_file}. "
                "Mapping file appears to be empty or invalid."
            )
        else:
            log_msg(
                f"[{extract_id}] {len(uids_list)} UIDs - OK",
                level="info",
                logger_name="utils"
            )
    
    # Log warnings (non-critical)
    for warning in warnings:
        log_msg(warning, level="warning", logger_name="utils")
    
    # Raise errors (critical - stop pipeline)
    if errors:
        error_msg = "Drug mapping validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        log_msg(error_msg, level="error", logger_name="utils")
        raise ValueError(error_msg)


def load_configuration(config_path: Path) -> dict:
    """Reads a JSON file configuration and returns its contents as a dictionary.

    Args:
        config_path: Path to the JSON configuration file.

    Returns:
        dict: Dictionary containing the JSON data.
    """
    try:
        with Path.open(config_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError as e:
        raise Exception(f"The file '{config_path}' was not found {e}") from e
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON: {e}") from e
    except Exception as e:
        raise Exception(f"Unexpected error while loading configuration '{config_path}' {e}") from e


def load_pipeline_config(
    config_dir: Path,
    extract_config_file: str = "extract_config.json",
    push_settings_file: str = "push_settings.json",
) -> dict:
    """Load the main pipeline configuration.
    
    Loads extract_config.json and push_settings.json and merges them.
    Uses load_configuration() internally to avoid hardcoding file loading logic.
    File names can be customized via parameters.

    Args:
        config_dir: Path to the configuration directory.
        extract_config_file: Name of the extract configuration file (default: "extract_config.json").
        push_settings_file: Name of the push settings file (default: "push_settings.json").

    Returns:
        dict: Merged configuration with CONNECTIONS, EXTRACTION, EXTRACTS, and PUSH_SETTINGS.
    """
    extract_config_path = config_dir / extract_config_file
    push_settings_path = config_dir / push_settings_file
    
    # Load extract config using load_configuration (reusable function, no hardcoded json.load)
    config = load_configuration(extract_config_path)
    
    # Load push settings if it exists
    if push_settings_path.exists():
        config["PUSH_SETTINGS"] = load_configuration(push_settings_path)
    else:
        config["PUSH_SETTINGS"] = {}
    
    return config


def get_extract_config(config: dict, extract_id: str) -> dict | None:
    """Get configuration for a specific extract.

    Args:
        config: Pipeline configuration dict.
        extract_id: The extract ID to find (EXTRACT_UID or EXTRACT_ID).

    Returns:
        Extract configuration dict or None if not found.
    """
    extracts_list = config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    if not extracts_list:
        extracts_list = config.get("EXTRACTS", [])

    for extract in extracts_list:
        # Support both EXTRACT_UID (new) and EXTRACT_ID (legacy) for backward compatibility
        if extract.get("EXTRACT_UID") == extract_id or extract.get("EXTRACT_ID") == extract_id:
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


# Default DHIS2 Category Option Combo and Attribute Option Combo
# This is the standard "default" value used when no specific COC/AOC is needed
DEFAULT_COC_AOC = "HllvX50cXC0"


def get_mapping_file_for_extract(extract_id: str) -> str:
    """Get the drug mapping filename for an extract based on its ID.
    
    The mapping file is determined by the prefix of the extract_id.
    For example: "Fosa_exhaustivity_data_elements" -> "drug_mapping_fosa.json"
    
    Parameters
    ----------
    extract_id : str
        The extract identifier (e.g., "Fosa_exhaustivity_data_elements").
    
    Returns
    -------
    str
        The mapping filename (e.g., "drug_mapping_fosa.json").
    """
    mapping_prefix = extract_id.split("_")[0].lower()
    return f"drug_mapping_{mapping_prefix}.json"


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
    
    for _drug_name, indicators in drug_mapping.items():
        for _indicator_name, indicator_data in indicators.items():
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
    
    coc_count = sum(len(c) for c in uid_to_cocs.values())
    logging.info(f"Loaded drug mapping from {mapping_file}: {len(uids)} UIDs, {coc_count} COC mappings")
    
    return mappings, uids


def dhis2_request(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict:
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


def push_dataset_org_units(
    dhis2_client: DHIS2,
    source_dataset_id: str,
    target_dataset_id: str,
    dry_run: bool = True,
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
    to_add = set(source_ous) - set(target_ous)  # missing in target
    to_remove = set(target_ous) - set(source_ous)  # extra in target
    diff_org_units = to_add | to_remove
    if len(diff_org_units) == 0:
        if _has_current_run and _current_run:
            _current_run.log_info("Source and target dataset organisation units are in sync, no update needed.")
        return {"status": "skipped", "message": "No update needed, org units are identical."}

    if _has_current_run and _current_run:
        _current_run.log_info(
            f"Found {len(to_add)} org units to add and {len(to_remove)} org units to remove "
            f"from target dataset '{target_dataset['name'].item()}' ({target_dataset_id})."
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
        if _has_current_run and _current_run:
            _current_run.log_info(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
        logging.error(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
    else:
        msg = f"Dataset {target_dataset['name'].item()} ({target_dataset_id}) org units updated: {len(source_ous)}"
        if _has_current_run and _current_run:
            _current_run.log_info(msg)
        logging.info(msg)

    return update_response

