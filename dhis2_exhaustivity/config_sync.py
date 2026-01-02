"""
Configuration synchronization module.

With the new pipeline_config.json structure, UIDs are loaded dynamically from
drug_mapping files at runtime. This module now only validates that the drug_mapping
files exist and are valid.
"""

import json
from pathlib import Path

try:
    from openhexa.sdk.workspaces import workspace
    from openhexa.sdk import current_run
    def log_info(msg): current_run.log_info(msg)
    def log_warning(msg): current_run.log_warning(msg)
    def log_error(msg): current_run.log_error(msg)
except ImportError:
    workspace = None
    def log_info(msg): print(f"[INFO] {msg}")
    def log_warning(msg): print(f"[WARNING] {msg}")
    def log_error(msg): print(f"[ERROR] {msg}")


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
    
    with open(extract_config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
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
    result = sync_configs_from_drug_mapping(Path(__file__).parent)
    print("\nResult:")
    for extract_id, data in result.items():
        status = "OK" if data["valid"] else "INVALID"
        print(f"  {extract_id}: {data['uids_count']} UIDs ({status})")
