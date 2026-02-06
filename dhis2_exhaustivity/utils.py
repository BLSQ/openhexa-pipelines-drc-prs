import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2

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
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        # Connection is implicit - no need to log every time
        return dhis2_client
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
        drug_mapping_file = extract.get("DRUG_MAPPING_FILE")
        
        if not drug_mapping_file:
            # If UIDS are provided directly, drug_mapping_file is optional
            if not extract.get("UIDS"):
                errors.append(
                    f"[{extract_id}] Missing DRUG_MAPPING_FILE and no UIDS provided. "
                    "Either DRUG_MAPPING_FILE or UIDS must be configured."
                )
            else:
                log_msg(
                    f"[{extract_id}] Using UIDS from config ({len(extract.get('UIDS', []))} UIDs) - OK",
                    level="info",
                    logger_name="utils"
                )
            continue
        
        mapping_path = config_dir / drug_mapping_file
        
        if not mapping_path.exists():
            errors.append(
                f"[{extract_id}] Drug mapping file not found: {drug_mapping_file} "
                f"(expected at {mapping_path})"
            )
            continue
        
        # Load and validate UIDs from mapping file
        try:
            with mapping_path.open("r", encoding="utf-8") as f:
                drug_mapping = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(
                f"[{extract_id}] Invalid JSON in {drug_mapping_file}: {e}"
            )
            continue
        except Exception as e:
            errors.append(
                f"[{extract_id}] Error reading {drug_mapping_file}: {e}"
            )
            continue
        
        # Extract UIDs from mapping
        uids = set()
        for drug_name, indicators in drug_mapping.items():
            if not isinstance(indicators, dict):
                warnings.append(
                    f"[{extract_id}] Invalid structure in {drug_mapping_file}: "
                    f"'{drug_name}' is not a dictionary"
                )
                continue
            for indicator_name, indicator_data in indicators.items():
                if not isinstance(indicator_data, dict):
                    continue
                uid = indicator_data.get("UID")
                if uid:
                    uids.add(uid)
        
        uids_list = sorted(list(uids))
        
        if not uids_list:
            errors.append(
                f"[{extract_id}] No UIDs found in {drug_mapping_file}. "
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
            data = json.load(file)
        return data
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


