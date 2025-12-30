"""
Synchronisation des configurations avec les fichiers CSV de médicaments.

Lit les fichiers medicaments_fosa.csv et medicaments_bcz.csv
et met à jour extract_config.json et push_config.json en conséquence.

Pour les nouveaux UIDs sans mapping, interroge DHIS2 pour détecter
automatiquement le type (Type 1 = 5 COCs, Type 2 = 1 COC).
"""

import csv
import json
from pathlib import Path
from typing import Optional

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


# Configuration des niveaux avec les types de COCs connus
LEVEL_CONFIG = {
    "FOSA": {
        "csv_file": "medicaments_fosa.csv",
        "extract_uid": "Fosa_exhaustivity_data_elements",
        "org_unit_level": 5,
        "coc_types": {
            # Type 1: 5 COCs (désagrégé)
            "type_1": ["AeX0BJgW0s5", "WZwmzIuRvwV", "XEN3ucCGa07", "kOWsLrtvrhn", "pdKyvaYRqCj"],
            # Type 2: 1 COC (agrégé)
            "type_2": ["pGSfKOKL9s0"]
        }
    },
    "BCZ": {
        "csv_file": "medicaments_bcz.csv",
        "extract_uid": "BCZ_exhaustivity_data_elements",
        "org_unit_level": 3,
        "coc_types": {
            # Type 1: 5 COCs (désagrégé)
            "type_1": ["AeX0BJgW0s5", "cjeG5HSWRIU", "mcCtxO7bWz6", "q8NaeXeGd3g", "zRUwlSQL9de"],
            # Type 2: 1 COC (agrégé)
            "type_2": ["lZGBsOl4hOS"]
        }
    }
}


def load_medicaments_from_csv(csv_path: Path) -> list[str]:
    """Charge la liste des UIDs de médicaments depuis un fichier CSV."""
    uids = []
    if not csv_path.exists():
        log_warning(f"Fichier CSV non trouve: {csv_path}")
        return uids
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = row.get("MEDICAMENT_UID", "").strip()
            if uid:
                uids.append(uid)
    
    return uids


def detect_coc_type_from_extracts(uid: str, level_config: dict, extracts_dir: Path) -> Optional[str]:
    """
    Détecte le type de COC en regardant les données extraites (fichiers parquet).
    
    Logique:
    - Lire les fichiers parquet de l'extract
    - Trouver les COCs associés à cet UID dans les données
    - Si un COC trouvé appartient au Type 1 → retourner "type_1"
    - Si un COC trouvé appartient au Type 2 → retourner "type_2"
    
    Même si l'UID n'apparaît qu'avec 1-2 COCs dans les données,
    on le mappe avec TOUS les COCs de son type.
    
    Returns:
        "type_1" ou "type_2" selon les COCs trouvés, None si non détectable
    """
    try:
        import polars as pl
        
        # Trouver les fichiers parquet de l'extract
        parquet_files = list(extracts_dir.glob("*.parquet"))
        
        if not parquet_files:
            log_warning(f"Aucun fichier parquet trouve dans {extracts_dir}")
            return None
        
        # Lire et filtrer pour cet UID
        all_cocs = set()
        
        for pq_file in parquet_files:
            try:
                df = pl.read_parquet(pq_file)
                
                # Chercher la colonne qui contient les data elements (dx ou DX_UID)
                dx_col = None
                for col in ["dx", "DX_UID", "dataElement"]:
                    if col in df.columns:
                        dx_col = col
                        break
                
                if dx_col is None:
                    continue
                
                # Chercher la colonne COC
                coc_col = None
                for col in ["co", "COC", "categoryOptionCombo"]:
                    if col in df.columns:
                        coc_col = col
                        break
                
                if coc_col is None:
                    continue
                
                # Filtrer pour cet UID et récupérer les COCs uniques
                uid_data = df.filter(pl.col(dx_col) == uid)
                if len(uid_data) > 0:
                    cocs = uid_data.select(coc_col).unique().to_series().to_list()
                    all_cocs.update(cocs)
                    
            except Exception as e:
                log_warning(f"Erreur lecture {pq_file}: {e}")
                continue
        
        if not all_cocs:
            log_warning(f"UID {uid} non trouve dans les extracts")
            return None
        
        # Matcher avec les types connus
        type_1_cocs = set(level_config["coc_types"]["type_1"])
        type_2_cocs = set(level_config["coc_types"]["type_2"])
        
        # Si AU MOINS UN COC trouvé appartient au Type 1 → c'est un Type 1
        if all_cocs & type_1_cocs:
            log_info(f"  {uid} -> Type 1 (COCs trouves: {all_cocs & type_1_cocs})")
            return "type_1"
        # Si AU MOINS UN COC trouvé appartient au Type 2 → c'est un Type 2
        elif all_cocs & type_2_cocs:
            log_info(f"  {uid} -> Type 2 (COCs trouves: {all_cocs & type_2_cocs})")
            return "type_2"
        else:
            log_warning(f"COCs inconnus pour {uid}: {all_cocs}")
            return None
            
    except ImportError:
        log_warning("Polars non disponible pour lire les extracts")
        return None
    except Exception as e:
        log_warning(f"Erreur detection COC pour {uid}: {e}")
        return None


def create_mapping_for_uid(uid: str, coc_type: str, level_config: dict) -> dict:
    """Crée un mapping pour un UID basé sur son type détecté."""
    cocs = level_config["coc_types"][coc_type]
    
    return {
        "UID": uid,
        "CATEGORY_OPTION_COMBO": {coc: coc for coc in cocs},
        "ATTRIBUTE_OPTION_COMBO": {}
    }


def sync_extract_config(config_path: Path, extract_uid: str, uids: list[str]) -> bool:
    """Met à jour la liste des UIDs dans extract_config.json."""
    if not config_path.exists():
        log_warning(f"extract_config.json non trouve: {config_path}")
        return False
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    modified = False
    for extract in config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
        if extract.get("EXTRACT_UID") == extract_uid:
            current_uids = set(extract.get("UIDS", []))
            new_uids = set(uids)
            
            if current_uids != new_uids:
                added = new_uids - current_uids
                removed = current_uids - new_uids
                
                if added:
                    log_info(f"[{extract_uid}] +{len(added)} medicaments ajoutes: {list(added)}")
                if removed:
                    log_info(f"[{extract_uid}] -{len(removed)} medicaments retires: {list(removed)}")
                
                extract["UIDS"] = sorted(list(new_uids))
                modified = True
            break
    
    if modified:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    
    return modified


def sync_push_config(
    config_path: Path, 
    extract_uid: str, 
    uids: list[str], 
    level_config: dict,
    extracts_dir: Optional[Path] = None
) -> tuple[bool, list[str]]:
    """
    Met à jour les MAPPINGS dans push_config.json.
    
    - Retire les mappings pour les UIDs qui ne sont plus dans le CSV
    - Détecte et crée les mappings pour les nouveaux UIDs en regardant les extracts
    
    Args:
        config_path: Chemin vers push_config.json
        extract_uid: ID de l'extract (ex: "Fosa_exhaustivity_data_elements")
        uids: Liste des UIDs depuis le CSV
        level_config: Configuration du niveau (contient les types COC)
        extracts_dir: Dossier contenant les fichiers parquet extraits
    
    Returns:
        tuple: (modified: bool, missing_uids: list[str] sans mapping créé)
    """
    if not config_path.exists():
        log_warning(f"push_config.json non trouve: {config_path}")
        return False, list(uids)
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    modified = False
    missing_uids = []
    uids_set = set(uids)
    
    for extract in config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
        if extract.get("EXTRACT_UID") == extract_uid:
            mappings = extract.get("MAPPINGS", {})
            current_mapping_uids = set(mappings.keys())
            
            # Retirer les mappings pour les UIDs qui ne sont plus dans le CSV
            removed_uids = current_mapping_uids - uids_set
            if removed_uids:
                for uid in removed_uids:
                    del mappings[uid]
                log_info(f"[{extract_uid}] Mappings retires: {list(removed_uids)}")
                modified = True
            
            # Trouver les UIDs sans mapping
            new_uids_without_mapping = uids_set - current_mapping_uids
            
            if new_uids_without_mapping:
                if extracts_dir and extracts_dir.exists():
                    log_info(f"[{extract_uid}] Detection du type depuis les extracts pour {len(new_uids_without_mapping)} nouveaux UIDs...")
                    
                    for uid in new_uids_without_mapping:
                        coc_type = detect_coc_type_from_extracts(uid, level_config, extracts_dir)
                        
                        if coc_type:
                            # Créer le mapping automatiquement
                            mappings[uid] = create_mapping_for_uid(uid, coc_type, level_config)
                            log_info(f"  [OK] Mapping cree pour {uid} ({coc_type})")
                            modified = True
                        else:
                            missing_uids.append(uid)
                else:
                    # Pas d'extracts disponibles
                    missing_uids = list(new_uids_without_mapping)
                    if new_uids_without_mapping:
                        log_warning(f"[{extract_uid}] Pas d'extracts pour detecter le type des nouveaux UIDs")
            
            break
    
    if modified:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    
    if missing_uids:
        log_warning(f"[{extract_uid}] {len(missing_uids)} UIDs sans mapping (non trouves dans extracts): {missing_uids}")
    
    return modified, missing_uids


def get_extracts_dir_for_level(pipeline_path: Path, extract_config_path: Path, extract_uid: str) -> Optional[Path]:
    """Trouve le dossier d'extracts pour un niveau donné."""
    try:
        from utils import load_configuration
        extract_config = load_configuration(extract_config_path)
        
        for extract in extract_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", []):
            if extract.get("EXTRACT_UID") == extract_uid:
                # Le dossier d'extracts est basé sur le DATASET_UID ou un nom custom
                # Convention: data/extracts/Extract lvl {level}
                level = extract.get("ORG_UNITS_LEVEL", "")
                extracts_dir = pipeline_path / "data" / "extracts" / f"Extract lvl {level}"
                if extracts_dir.exists():
                    return extracts_dir
                # Alternative: chercher un dossier qui contient des parquet
                data_dir = pipeline_path / "data" / "extracts"
                if data_dir.exists():
                    for subdir in data_dir.iterdir():
                        if subdir.is_dir() and list(subdir.glob("*.parquet")):
                            # Vérifier si c'est le bon niveau en regardant le nom
                            if f"lvl {level}" in subdir.name.lower() or f"level {level}" in subdir.name.lower():
                                return subdir
        return None
    except Exception as e:
        log_warning(f"Erreur recherche dossier extracts: {e}")
        return None


def sync_configs_from_csv(pipeline_path: Optional[Path] = None) -> dict:
    """
    Synchronise les configurations avec les fichiers CSV de médicaments.
    
    - Lit les CSVs (medicaments_fosa.csv, medicaments_bcz.csv)
    - Met à jour extract_config.json avec les UIDs
    - Pour les nouveaux UIDs, détecte leur type depuis les extracts parquet
    - Crée automatiquement les mappings COC dans push_config.json
    
    Args:
        pipeline_path: Chemin vers le dossier du pipeline
    
    Returns:
        dict: Résumé de la synchronisation
    """
    if pipeline_path is None:
        if workspace:
            pipeline_path = Path(workspace.files_path) / "pipelines" / "dhis2_exhaustivity"
        else:
            pipeline_path = Path(__file__).parent
    
    config_dir = pipeline_path / "configuration"
    extract_config_path = config_dir / "extract_config.json"
    push_config_path = config_dir / "push_config.json"
    
    log_info("Synchronisation des configs avec les fichiers CSV...")
    
    result = {}
    
    for level_name, level_config in LEVEL_CONFIG.items():
        csv_path = config_dir / level_config["csv_file"]
        extract_uid = level_config["extract_uid"]
        
        # Charger les UIDs depuis le CSV
        uids = load_medicaments_from_csv(csv_path)
        
        if not uids:
            log_warning(f"[{level_name}] Aucun medicament trouve dans {level_config['csv_file']}")
            continue
        
        # Synchroniser extract_config
        extract_modified = sync_extract_config(extract_config_path, extract_uid, uids)
        
        # Trouver le dossier d'extracts pour ce niveau
        extracts_dir = get_extracts_dir_for_level(pipeline_path, extract_config_path, extract_uid)
        if extracts_dir:
            log_info(f"[{level_name}] Dossier extracts: {extracts_dir}")
        
        # Synchroniser push_config (avec auto-détection depuis les extracts)
        push_modified, missing = sync_push_config(
            push_config_path, 
            extract_uid, 
            uids, 
            level_config,
            extracts_dir
        )
        
        result[level_name] = {
            "uids_count": len(uids),
            "extract_modified": extract_modified,
            "push_modified": push_modified,
            "missing_mappings": missing
        }
        
        status = "OK" if not missing else f"WARN ({len(missing)} sans mapping)"
        log_info(f"[{level_name}] {len(uids)} medicaments - {status}")
    
    return result


if __name__ == "__main__":
    # Test local - utilise les extracts existants pour détecter les types
    result = sync_configs_from_csv(Path(__file__).parent)
    print("\nResultat:")
    for level, data in result.items():
        print(f"  {level}: {data['uids_count']} medicaments")
        if data.get("missing_mappings"):
            print(f"    -> {len(data['missing_mappings'])} sans mapping (pas dans les extracts)")
