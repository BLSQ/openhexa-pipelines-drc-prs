#!/usr/bin/env python3
"""
Script pour extraire TOUTES les données (sans limite) et calculer l'exhaustivité.
À exécuter dans OpenHexa où la connexion DHIS2 est configurée.
"""
import logging
import os
from datetime import datetime
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta
from openhexa.toolbox.dhis2 import DHIS2

from d2d_library.dhis2_extract_handlers import DHIS2Extractor
from exhaustivity_calculation import compute_exhaustivity
from utils import (
    configure_logging,
    connect_to_dhis2,
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


def main():
    """Extrait toutes les données (sans limite) et calcule l'exhaustivité."""
    # Chemin du pipeline
    pipeline_path = Path(__file__).parent
    
    # Configuration du logging
    configure_logging(logs_path=pipeline_path / "logs" / "extract_all", task_name="extract_all_and_compute")
    
    # Charger la configuration
    extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    
    # Vérifier si on peut utiliser les fichiers existants ou si on doit télécharger
    logger.info("🔍 Vérification des fichiers existants...")
    use_existing_files = True
    dhis2_client = None
    
    # Vérifier si tous les fichiers existent
    data_element_extracts = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
    for extract in data_element_extracts:
        org_units_level = extract.get("ORG_UNITS_LEVEL", None)
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        else:
            folder_name = f"Extract {extract.get('EXTRACT_UID')}"
        
        extracts_folder = pipeline_path / "data" / "extracts" / folder_name
        if not extracts_folder.exists():
            extracts_folder = pipeline_path / "workspace" / "pipelines" / "dhis2_exhaustivity" / "data" / "extracts" / folder_name
        
        if not extracts_folder.exists() or len(list(extracts_folder.glob("data_*.parquet"))) == 0:
            use_existing_files = False
            break
    
    # Si on doit télécharger, se connecter à DHIS2
    if not use_existing_files:
        logger.info("📥 Fichiers manquants, connexion à DHIS2 nécessaire pour télécharger...")
        connection_str = extract_config["SETTINGS"]["SOURCE_DHIS2_CONNECTION"]
        
        try:
            dhis2_client = connect_to_dhis2(
                connection_str=connection_str, 
                cache_dir=None
            )
            logger.info("✅ Connecté à DHIS2 via workspace")
        except Exception as e:
            # Si ça échoue, essayer avec variables d'environnement
            logger.warning(f"Connexion workspace échouée ({e!s}), tentative avec variables d'environnement...")
            
            dhis2_url = os.getenv("DHIS2_URL") or os.getenv("DHIS2_BASE_URL")
            dhis2_username = os.getenv("DHIS2_USERNAME") or os.getenv("DHIS2_USER")
            dhis2_password = os.getenv("DHIS2_PASSWORD")
            
            if dhis2_url and dhis2_username and dhis2_password:
                dhis2_client = DHIS2(
                    url=dhis2_url,
                    username=dhis2_username,
                    password=dhis2_password,
                    cache_dir=None
                )
                logger.info(f"✅ Connecté à DHIS2 via variables d'environnement: {dhis2_url}")
            else:
                raise Exception(
                    f"Impossible de se connecter à DHIS2. "
                    f"Connexion workspace '{connection_str}' introuvable et variables d'environnement manquantes. "
                    f"Définissez DHIS2_URL, DHIS2_USERNAME et DHIS2_PASSWORD, ou utilisez les fichiers existants."
                )
    else:
        logger.info("✅ Tous les fichiers existent, utilisation des fichiers locaux (pas besoin de connexion DHIS2)")
    
    # IMPORTANT: SUPPRIMER TOUTES LES LIMITES pour extraire TOUT (seulement si on télécharge)
    if dhis2_client:
        logger.info("🔓 Suppression de toutes les limites (MAX_DATA_ELEMENTS, MAX_ORG_UNITS)")
        
        # Supprimer les limites pour data_value_sets
        if hasattr(dhis2_client, 'data_value_sets'):
            if hasattr(dhis2_client.data_value_sets, 'MAX_DATA_ELEMENTS'):
                original_max_de = dhis2_client.data_value_sets.MAX_DATA_ELEMENTS
                dhis2_client.data_value_sets.MAX_DATA_ELEMENTS = 999999
                logger.info(f"   data_value_sets.MAX_DATA_ELEMENTS: {original_max_de} → 999999 (illimité)")
            if hasattr(dhis2_client.data_value_sets, 'MAX_ORG_UNITS'):
                original_max_ou = dhis2_client.data_value_sets.MAX_ORG_UNITS
                dhis2_client.data_value_sets.MAX_ORG_UNITS = 999999
                logger.info(f"   data_value_sets.MAX_ORG_UNITS: {original_max_ou} → 999999 (illimité)")
        
        # Supprimer les limites pour analytics (utilisé par analytics_data_elements)
        if hasattr(dhis2_client, 'analytics'):
            if hasattr(dhis2_client.analytics, 'MAX_DATA_ELEMENTS'):
                original_max_de = dhis2_client.analytics.MAX_DATA_ELEMENTS
                dhis2_client.analytics.MAX_DATA_ELEMENTS = 999999
                logger.info(f"   analytics.MAX_DATA_ELEMENTS: {original_max_de} → 999999 (illimité)")
            if hasattr(dhis2_client.analytics, 'MAX_ORG_UNITS'):
                original_max_ou = dhis2_client.analytics.MAX_ORG_UNITS
                dhis2_client.analytics.MAX_ORG_UNITS = 999999
                logger.info(f"   analytics.MAX_ORG_UNITS: {original_max_ou} → 999999 (illimité)")
    
    # Calculer les périodes (même logique que dans extract_data)
    extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
    
    extract_periods = get_periods(start, end)
    logger.info(f"📅 Périodes à traiter: {extract_periods} ({len(extract_periods)} mois)")
    
    # Setup extractor (seulement si on télécharge)
    dhis2_extractor = None
    source_datasets = None
    if dhis2_client:
        dhis2_extractor = DHIS2Extractor(
            dhis2_client=dhis2_client, 
            download_mode="DOWNLOAD_REPLACE", 
            return_existing_file=False
        )
        
        # Récupérer les datasets
        from openhexa.toolbox.dhis2.dataframe import get_datasets
        source_datasets = get_datasets(dhis2_client)
    
    # Traiter chaque extract
    data_element_extracts = extract_config["DATA_ELEMENTS"].get("EXTRACTS", [])
    logger.info(f"📦 {len(data_element_extracts)} extract(s) à traiter")
    
    total_rows_extracted = {}
    total_rows_exhaustivity = {}
    
    for idx, extract in enumerate(data_element_extracts):
        extract_id = extract.get("EXTRACT_UID")
        org_units_level = extract.get("ORG_UNITS_LEVEL", None)
        data_element_uids = extract.get("UIDS", [])
        dataset_orgunits_uid = extract.get("DATASET_ORGUNITS_UID")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 Extract {idx + 1}/{len(data_element_extracts)}: {extract_id}")
        logger.info(f"{'='*80}")
        
        # Récupérer TOUTES les org units depuis le dataset (SANS LIMITE)
        # Seulement si on a une connexion DHIS2, sinon on utilisera les org units des fichiers
        org_units = []
        dataset_name = "Unknown"
        if dhis2_client and source_datasets is not None:
            if dataset_orgunits_uid is not None:
                source_dataset = source_datasets.filter(pl.col("id").is_in([dataset_orgunits_uid]))
                org_units = source_dataset["organisation_units"].explode().to_list()
                dataset_name = source_dataset["name"][0] if len(source_dataset) > 0 else "Unknown"
                logger.info(f"   📍 Dataset: {dataset_name} (DATASET_ORGUNITS_UID: {dataset_orgunits_uid})")
            else:
                from utils import retrieve_ou_list
                org_units = retrieve_ou_list(dhis2_client=dhis2_client, ou_level=org_units_level)
                dataset_name = f"Level {org_units_level}"
                logger.info(f"   📍 Level: {dataset_name}")
            
            # IMPORTANT: Ne PAS limiter les org units - utiliser TOUTES celles du dataset
            logger.info(f"   🏢 Org Units: {len(org_units):,} (TOUTES, sans limite)")
        else:
            logger.info(f"   📍 Utilisation des fichiers existants")
            logger.info(f"   ⚠️  NOTE: Les org units seront déduites des données existantes")
            logger.info(f"   ⚠️  Pour obtenir TOUTES les org units du dataset, il faut télécharger depuis DHIS2")
        
        logger.info(f"   📋 Data Elements: {len(data_element_uids)}")
        
        # Créer le nom du dossier
        if org_units_level is not None:
            folder_name = f"Extract lvl {org_units_level}"
        else:
            folder_name = f"Extract {extract_id}"
        
        # Chercher le dossier extracts (local ou workspace)
        extracts_folder = pipeline_path / "data" / "extracts" / folder_name
        if not extracts_folder.exists():
            extracts_folder = pipeline_path / "workspace" / "pipelines" / "dhis2_exhaustivity" / "data" / "extracts" / folder_name
        
        extracts_folder.mkdir(parents=True, exist_ok=True)
        
        # Télécharger ou lire les données pour chaque période
        extract_rows_per_period = {}
        for period in extract_periods:
            period_file = extracts_folder / f"data_{period}.parquet"
            
            if period_file.exists() and not dhis2_client:
                # Fichier existe et on n'a pas de connexion, le lire
                logger.info(f"\n   📖 Lecture fichier existant {period}...")
                try:
                    df = pl.read_parquet(period_file)
                    row_count = len(df)
                    extract_rows_per_period[period] = row_count
                    logger.info(f"      ✅ {period}: {row_count:,} lignes → {period_file.name}")
                except Exception as e:
                    logger.error(f"      ❌ Erreur lecture {period}: {e!s}")
                    extract_rows_per_period[period] = 0
            elif dhis2_client and dhis2_extractor:
                # Télécharger
                logger.info(f"\n   📥 Téléchargement période {period}...")
                try:
                    output_file = dhis2_extractor.analytics_data_elements.download_period(
                        data_elements=data_element_uids,
                        org_units=org_units,
                        period=period,
                        output_dir=extracts_folder,
                        filename=f"data_{period}.parquet",
                    )
                    
                    if output_file and output_file.exists():
                        df = pl.read_parquet(output_file)
                        row_count = len(df)
                        extract_rows_per_period[period] = row_count
                        logger.info(f"      ✅ {period}: {row_count:,} lignes → {output_file.name}")
                    else:
                        logger.warning(f"      ⚠️  {period}: Aucun fichier créé")
                        extract_rows_per_period[period] = 0
                        
                except Exception as e:
                    logger.error(f"      ❌ Erreur pour {period}: {e!s}")
                    import traceback
                    logger.error(f"      Traceback:\n{traceback.format_exc()}")
                    extract_rows_per_period[period] = 0
            else:
                logger.warning(f"      ⚠️  {period}: Fichier manquant et pas de connexion DHIS2")
                extract_rows_per_period[period] = 0
        
        # Total pour cet extract
        total_extract_rows = sum(extract_rows_per_period.values())
        total_rows_extracted[extract_id] = {
            'total': total_extract_rows,
            'per_period': extract_rows_per_period
        }
        logger.info(f"\n   📊 Total extrait pour {extract_id}: {total_extract_rows:,} lignes")
        
        # Calculer l'exhaustivité
        logger.info(f"\n   🔢 Calcul de l'exhaustivité pour {extract_id}...")
        
        # Récupérer les mappings pour les expected_dx_uids
        push_config_path = pipeline_path / "configuration" / "push_config.json"
        push_config = load_configuration(config_path=push_config_path)
        
        # Trouver le mapping pour cet extract
        extract_mappings = push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
        extract_mapping = next(
            (e for e in extract_mappings if e.get("EXTRACT_UID") == extract_id),
            None
        )
        
        expected_dx_uids = None
        if extract_mapping:
            expected_dx_uids = extract_mapping.get("MAPPINGS", {}).keys()
            logger.info(f"   📋 {len(expected_dx_uids)} DX_UIDs attendus depuis push_config")
        
        # Récupérer les org units attendues (depuis le dataset)
        expected_org_units = org_units if org_units else None
        
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
        
        # Détail par période
        logger.info(f"   Détail par période (extraction):")
        for period, rows in total_rows_extracted[extract_id]['per_period'].items():
            logger.info(f"      {period}: {rows:,} lignes")
    
    logger.info(f"\n✅ Extraction et calcul terminés!")


if __name__ == "__main__":
    main()

