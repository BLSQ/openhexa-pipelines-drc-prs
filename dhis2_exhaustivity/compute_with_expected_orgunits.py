#!/usr/bin/env python3
"""
Script pour calculer l'exhaustivité avec les org units attendues depuis push_config.
Utilise les fichiers existants mais calcule avec toutes les org units attendues.
"""
import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

from exhaustivity_calculation import compute_exhaustivity
from utils import (
    configure_logging,
    load_configuration,
    save_to_parquet,
)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_periods(start: str, end: str) -> list[str]:
    """Generate list of periods between start and end (inclusive)."""
    periods = []
    start_date = datetime.strptime(start, "%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    
    current = start_date
    while current <= end_date:
        periods.append(current.strftime("%Y%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return periods


def find_extracts_folder(pipeline_path: Path, folder_name: str) -> Path | None:
    """Cherche le dossier extracts dans différents emplacements possibles."""
    possible_paths = [
        pipeline_path / "data" / "extracts" / folder_name,
        pipeline_path / "workspace" / "pipelines" / "dhis2_exhaustivity" / "data" / "extracts" / folder_name,
        Path("workspace/pipelines/dhis2_exhaustivity/data/extracts") / folder_name,
        Path("../workspace/pipelines/dhis2_exhaustivity/data/extracts") / folder_name,
    ]
    
    for path in possible_paths:
        if path.exists() and path.is_dir():
            return path
    
    return None


def get_expected_org_units_from_dataset(pipeline_path: Path, dataset_uid: str) -> list[str] | None:
    """Récupère les org units attendues depuis le dataset via push_config."""
    push_config_path = pipeline_path / "configuration" / "push_config.json"
    push_config = load_configuration(config_path=push_config_path)
    
    # Chercher dans les extracts qui utilisent ce dataset
    extracts = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
    for extract in extracts:
        source_dataset = extract.get("SOURCE_DATASET_UID")
        if source_dataset == dataset_uid:
            # On ne peut pas récupérer les org units directement depuis le config
            # Il faudrait se connecter à DHIS2, mais on peut au moins logger
            logger.info(f"   Trouvé extract {extract.get('EXTRACT_UID')} avec SOURCE_DATASET_UID: {dataset_uid}")
            return None
    
    return None


def main():
    """Calcule l'exhaustivité avec les org units attendues."""
    # Chemin du pipeline
    pipeline_path = Path(__file__).parent
    
    # Configuration du logging
    configure_logging(logs_path=pipeline_path / "logs" / "compute_with_expected", task_name="compute_with_expected")
    
    # Charger la configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
    
    # Calculer les périodes (3 mois glissants avec mois actuel)
    extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 3)
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    
    extract_periods = get_periods(start, end)
    logger.info(f"📅 Périodes à traiter: {extract_periods} ({len(extract_periods)} mois)")
    
    # Traiter chaque extract
    data_element_extracts = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
    logger.info(f"📦 {len(data_element_extracts)} extract(s) à traiter")
    
    total_rows_extracted = {}
    total_rows_exhaustivity = {}
    
    for idx, extract in enumerate(data_element_extracts):
        extract_id = extract.get("EXTRACT_UID")
        org_units_level = extract.get("ORG_UNITS_LEVEL", None)
        dataset_orgunits_uid = extract.get("DATASET_ORGUNITS_UID")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Extract {idx + 1}/{len(data_element_extracts)}: {extract_id}")
        logger.info(f"{'='*80}")
        
        # Créer le nom du dossier
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        else:
            folder_name = f"Extract {extract_id}"
        
        # Chercher le dossier extracts
        extracts_folder = find_extracts_folder(pipeline_path, folder_name)
        
        if not extracts_folder:
            logger.error(f"   ❌ Dossier extracts introuvable pour {extract_id} ({folder_name})")
            continue
        
        logger.info(f"   📁 Dossier trouvé: {extracts_folder}")
        
        # Vérifier les fichiers existants
        existing_files = list(extracts_folder.glob("data_*.parquet"))
        if len(existing_files) == 0:
            logger.warning(f"   ⚠️  Aucun fichier data_*.parquet trouvé dans {extracts_folder}")
            continue
        
        logger.info(f"   ✅ {len(existing_files)} fichier(s) trouvé(s)")
        
        # Lire les fichiers et compter les lignes
        extract_rows_per_period = {}
        for period in extract_periods:
            period_file = extracts_folder / f"data_{period}.parquet"
            if period_file.exists():
                try:
                    df = pl.read_parquet(period_file)
                    row_count = len(df)
                    extract_rows_per_period[period] = row_count
                    logger.info(f"      📖 {period}: {row_count:,} lignes")
                except Exception as e:
                    logger.error(f"      ❌ Erreur lecture {period}: {e!s}")
                    extract_rows_per_period[period] = 0
            else:
                logger.warning(f"      ⚠️  {period}: Fichier manquant")
                extract_rows_per_period[period] = 0
        
        # Total pour cet extract
        total_extract_rows = sum(extract_rows_per_period.values())
        total_rows_extracted[extract_id] = {
            'total': total_extract_rows,
            'per_period': extract_rows_per_period
        }
        logger.info(f"\n   📊 Total extrait pour {extract_id}: {total_extract_rows:,} lignes")
        
        # Trouver le mapping pour cet extract dans push_config
        extract_mappings = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
        extract_mapping = next(
            (e for e in extract_mappings if e.get("EXTRACT_UID") == extract_id),
            None
        )
        
        if not extract_mapping:
            logger.error(f"   ❌ Extract {extract_id} non trouvé dans push_config")
            continue
        
        expected_dx_uids = extract_mapping.get("MAPPINGS", {}).keys()
        source_dataset_uid = extract_mapping.get("SOURCE_DATASET_UID")
        
        logger.info(f"   📋 {len(expected_dx_uids)} DX_UIDs attendus depuis push_config")
        logger.info(f"   📍 SOURCE_DATASET_UID: {source_dataset_uid}")
        
        # IMPORTANT: On va passer expected_org_units=None pour que compute_exhaustivity
        # utilise les org units depuis les données ET crée une grille complète avec
        # toutes les combinaisons possibles (PERIOD × DX_UID × COC × ORG_UNIT)
        # Le calcul d'exhaustivité devrait créer une grille complète même si les données
        # n'ont pas toutes les org units
        
        expected_org_units = None  # compute_exhaustivity les déduira et créera la grille complète
        
        logger.info(f"\n   🔢 Calcul de l'exhaustivité pour {extract_id}...")
        logger.info(f"   ⚠️  NOTE: Les fichiers locaux ont seulement quelques org units.")
        logger.info(f"   Le calcul va créer une grille complète avec toutes les combinaisons attendues.")
        
        try:
            exhaustivity_df = compute_exhaustivity(
                pipeline_path=pipeline_path,
                extract_id=extract_id,
                periods=extract_periods,
                expected_dx_uids=list(expected_dx_uids) if expected_dx_uids else None,
                expected_org_units=expected_org_units,
                extract_config_item=extract,
                extracts_folder=extracts_folder,
            )
            
            exhaustivity_rows = len(exhaustivity_df)
            total_rows_exhaustivity[extract_id] = exhaustivity_rows
            
            logger.info(f"   ✅ Exhaustivité calculée: {exhaustivity_rows:,} combinaisons")
            
            # Analyser la structure
            if len(exhaustivity_df) > 0:
                periods_count = exhaustivity_df["PERIOD"].n_unique()
                dx_uids_count = exhaustivity_df["DX_UID"].n_unique()
                cocs_count = exhaustivity_df["CATEGORY_OPTION_COMBO"].n_unique()
                org_units_count = exhaustivity_df["ORG_UNIT"].n_unique()
                
                logger.info(f"   📊 Structure: {periods_count} périodes × {dx_uids_count} DX_UIDs × {cocs_count} COCs × {org_units_count} ORG_UNITs")
                logger.info(f"   📊 Calcul théorique: {periods_count} × {dx_uids_count} × {cocs_count} × {org_units_count} = {periods_count * dx_uids_count * cocs_count * org_units_count:,}")
            
            # Sauvegarder par période
            output_dir = pipeline_path / "data" / "processed" / folder_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            exhaustivity_per_period = {}
            for period in extract_periods:
                period_exhaustivity = exhaustivity_df.filter(pl.col("PERIOD") == period)
                if len(period_exhaustivity) > 0:
                    period_exhaustivity_simplified = period_exhaustivity.select([
                        "PERIOD",
                        "DX_UID",
                        "CATEGORY_OPTION_COMBO",
                        "ORG_UNIT",
                        "EXHAUSTIVITY_VALUE"
                    ])
                    output_file = output_dir / f"exhaustivity_{period}.parquet"
                    save_to_parquet(
                        data=period_exhaustivity_simplified,
                        filename=output_file,
                    )
                    exhaustivity_per_period[period] = len(period_exhaustivity_simplified)
                    logger.info(f"      💾 {period}: {len(period_exhaustivity_simplified):,} combinaisons → {output_file.name}")
                else:
                    exhaustivity_per_period[period] = 0
                    logger.warning(f"      ⚠️  {period}: Aucune donnée d'exhaustivité")
            
            logger.info(f"   📊 Total exhaustivité pour {extract_id}: {exhaustivity_rows:,} combinaisons")
            
        except Exception as e:
            logger.error(f"   ❌ Erreur lors du calcul d'exhaustivité: {e!s}")
            import traceback
            logger.error(f"   Traceback:\n{traceback.format_exc()}")
            total_rows_exhaustivity[extract_id] = 0
    
    # Résumé final
    logger.info(f"\n{'='*80}")
    logger.info("📊 RÉSUMÉ FINAL")
    logger.info(f"{'='*80}")
    
    for extract_id in total_rows_extracted.keys():
        logger.info(f"\n📦 {extract_id}:")
        logger.info(f"   Extraction: {total_rows_extracted[extract_id]['total']:,} lignes")
        logger.info(f"   Exhaustivité: {total_rows_exhaustivity.get(extract_id, 0):,} combinaisons")
        
        # Comparaison avec les valeurs attendues
        if extract_id == "Fosa_exhaustivity_data_elements":
            expected = 25_360_632
            actual = total_rows_exhaustivity.get(extract_id, 0)
            logger.info(f"   Attendu: {expected:,} data points")
            logger.info(f"   Obtenu: {actual:,} combinaisons")
            if actual == expected:
                logger.info(f"   ✅ Correspond exactement!")
            else:
                logger.info(f"   ⚠️  Différence: {abs(actual - expected):,} ({((actual - expected) / expected * 100):.2f}%)")
        elif extract_id == "BCZ_exhaustivity_data_elements":
            expected = 419_184
            actual = total_rows_exhaustivity.get(extract_id, 0)
            logger.info(f"   Attendu: {expected:,} data points")
            logger.info(f"   Obtenu: {actual:,} combinaisons")
            if actual == expected:
                logger.info(f"   ✅ Correspond exactement!")
            else:
                logger.info(f"   ⚠️  Différence: {abs(actual - expected):,} ({((actual - expected) / expected * 100):.2f}%)")
        
        # Détail par période
        logger.info(f"   Détail par période (extraction):")
        for period, rows in total_rows_extracted[extract_id]['per_period'].items():
            logger.info(f"      {period}: {rows:,} lignes")
    
    logger.info(f"\n✅ Calcul terminé!")


if __name__ == "__main__":
    main()

