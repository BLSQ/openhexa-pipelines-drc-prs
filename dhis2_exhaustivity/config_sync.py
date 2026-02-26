"""
Configuration synchronization module (DEPRECATED).

NOTE: The main validation function has been moved to utils.py as `validate_drug_mapping_files()`.
This module is kept for backwards compatibility only.

The new function in utils.py:
- Uses current_run.log_* instead of print() (visible in OpenHexa UI)
- Raises errors instead of just logging warnings (stops pipeline on invalid config)
- Better error handling and validation
"""

import json
from pathlib import Path

try:
    from openhexa.sdk.workspaces import workspace
except ImportError:
    workspace = None

# Use the unified logging function from utils
try:
    from utils import log_msg
    def log_info(msg: str) -> None:
        log_msg(msg, level="info", logger_name="config_sync")
    def log_warning(msg: str) -> None:
        log_msg(msg, level="warning", logger_name="config_sync")
    def log_error(msg: str) -> None:
        log_msg(msg, level="error", logger_name="config_sync")
except ImportError:
    # Fallback if utils is not available (shouldn't happen in normal usage)
    import logging
    _logger = logging.getLogger("config_sync")
    def log_info(msg: str) -> None:
        _logger.info(msg)
    def log_warning(msg: str) -> None:
        _logger.warning(msg)
    def log_error(msg: str) -> None:
        _logger.error(msg)

# Import load_configuration from utils to avoid hardcoding file loading
try:
    from utils import load_configuration
except ImportError:
    # Fallback if utils is not available
    def load_configuration(config_path: Path) -> dict:
        """Fallback load_configuration if utils is not available."""
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)


def load_uids_from_drug_mapping(mapping_path: Path) -> list[str]:
    """Load UIDs from a drug_mapping file."""
    if not mapping_path.exists():
        return []
    
    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            drug_mapping = json.load(f)
        
        uids = set()
        for drug_name, indicators in drug_mapping.items():
            for indicator_name, indicator_data in indicators.items():
                uid = indicator_data.get("UID")
                if uid:
                    uids.add(uid)
        
        return sorted(list(uids))
    except Exception as e:
        log_error(f"Error reading {mapping_path}: {e}")
        return []


def sync_configs_from_drug_mapping(pipeline_path: Path = None) -> dict:
    """
    Validate drug_mapping files and log their status.
    
    With the new pipeline_config.json structure, UIDs are loaded dynamically
    from drug_mapping files at runtime. This function just validates the files exist.
    
    Args:
        pipeline_path: Path to the pipeline directory.
    
    Returns:
        dict: Summary of validation results.
    """
    if pipeline_path is None:
        if workspace:
            pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"
        else:
            pipeline_path = Path(__file__).parent
    
    config_dir = pipeline_path / "configuration"
    extract_config_path = config_dir / "extract_config.json"
    
    if not extract_config_path.exists():
        log_warning(f"extract_config.json not found: {extract_config_path}")
        return {}
    
    # Use load_configuration instead of hardcoding json.load
    config = load_configuration(extract_config_path)
    
    log_info("Validating drug_mapping files...")
    
    result = {}
    for extract in config.get("EXTRACTS", []):
        extract_id = extract.get("EXTRACT_ID")
        drug_mapping_file = extract.get("DRUG_MAPPING_FILE")
        
        if not drug_mapping_file:
            log_warning(f"[{extract_id}] No DRUG_MAPPING_FILE configured")
            continue
        
        mapping_path = config_dir / drug_mapping_file
        uids = load_uids_from_drug_mapping(mapping_path)
        
        if not uids:
            log_warning(f"[{extract_id}] No UIDs found in {drug_mapping_file}")
        else:
            log_info(f"[{extract_id}] {len(uids)} UIDs - OK")
        
        result[extract_id] = {
            "drug_mapping_file": drug_mapping_file,
            "uids_count": len(uids),
            "valid": len(uids) > 0
        }
    
    return result


# Alias for backwards compatibility
sync_configs_from_csv = sync_configs_from_drug_mapping


if __name__ == "__main__":
    # Use unified logging function
    try:
        from utils import log_msg
    except ImportError:
        import logging
        _logger = logging.getLogger("config_sync")
        def log_msg(msg: str, level: str = "info", **kwargs) -> None:
            getattr(_logger, level.lower())(msg)
    
    result = sync_configs_from_drug_mapping(Path(__file__).parent)
    log_msg("\nResult:", level="info", logger_name="config_sync")
    for extract_id, data in result.items():
        status = "OK" if data["valid"] else "INVALID"
        log_msg(
            f"  {extract_id}: {data['uids_count']} UIDs ({status})",
            level="info",
            logger_name="config_sync"
        )
