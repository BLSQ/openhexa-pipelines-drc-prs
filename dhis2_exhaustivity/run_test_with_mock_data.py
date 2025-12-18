#!/usr/bin/env python3
"""Script pour tester le calcul d'exhaustivité avec des données mockées (sans connexion DHIS2)."""

import sys
import json
import shutil
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Ajouter le répertoire au path
sys.path.insert(0, str(Path(__file__).parent))

# Mock current_run pour les logs
class MockCurrentRun:
    def log_info(self, msg):
        print(f"ℹ️  {msg}")
    
    def log_warning(self, msg):
        print(f"⚠️  {msg}")
    
    def log_error(self, msg):
        print(f"❌ {msg}")

# Mock workspace pour éviter les connexions DHIS2
class MockWorkspace:
    def dhis2_connection(self, connection_str):
        raise Exception("Mock workspace - no real DHIS2 connection available")

# Patcher avant les imports
import openhexa.sdk
openhexa.sdk.current_run = MockCurrentRun()
openhexa.sdk.workspace = MockWorkspace()

# Importer les modules après le patch
import polars as pl
from utils import save_to_parquet, load_configuration

def create_mock_extract_data(pipeline_path: Path, extract_id: str, period: str):
    """Crée des données mockées pour tester le calcul d'exhaustivité."""
    # Utiliser le même nom de dossier que celui attendu par compute_exhaustivity_data
    # Pour "Fosa" extracts, le dossier est "Extract lvl 5"
    if "Fosa" in extract_id:
        folder_name = "Extract lvl 5"
    elif "BCZ" in extract_id:
        folder_name = "Extract lvl 3"
    else:
        folder_name = f"Extract {extract_id}"
    
    extracts_dir = pipeline_path / "data" / "extracts" / folder_name
    extracts_dir.mkdir(parents=True, exist_ok=True)
    
    # Charger les mappings réels depuis push_config pour utiliser les vrais COCs
    from utils import load_configuration
    push_config = load_configuration(config_path=pipeline_path / "configuration" / "push_config.json")
    
    # Trouver l'extract correspondant (gérer le cas avec _test)
    base_extract_id = extract_id.replace("_test", "")
    extract_config = next(
        (e for e in push_config.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
         if e.get("EXTRACT_UID") == extract_id or e.get("EXTRACT_UID") == base_extract_id),
        None
    )
    
    if not extract_config:
        print(f"⚠️  Extract {extract_id} non trouvé dans push_config, utilisation des valeurs par défaut")
    
    # Charger extract_config pour obtenir les UIDs si les mappings sont vides
    extract_config_full = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    extract_config_item = next(
        (e for e in extract_config_full.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
         if e.get("EXTRACT_UID") == extract_id),
        None
    )
    
    # Extraire les COCs source réels des mappings
    # IMPORTANT: Utiliser les DX_UIDs SOURCE (clés du mapping) et les COCs SOURCE (clés du CATEGORY_OPTION_COMBO)
    real_cocs = []
    real_dx_uids = []
    if extract_config:
        mappings = extract_config.get("MAPPINGS", {})
        if mappings:
            # Prendre TOUS les DX_UIDs source pour avoir des données complètes
            # Les clés du mapping sont les DX_UIDs SOURCE (ceux qui sont dans les données extraites)
            for dx_uid_source, mapping in mappings.items():
                real_dx_uids.append(dx_uid_source)  # Utiliser le DX_UID source (clé du mapping)
                # Prendre TOUS les COCs SOURCE du mapping (clés du CATEGORY_OPTION_COMBO)
                coc_mapping = mapping.get("CATEGORY_OPTION_COMBO", {})
                if coc_mapping:
                    # Les clés sont les COCs SOURCE (ceux qui sont dans les données extraites)
                    real_cocs.extend(list(coc_mapping.keys()))  # Prendre tous les COCs source
    
    # Si pas de mappings trouvés, utiliser extract_config.UIDS et dériver les COCs depuis les données
    if not real_dx_uids and extract_config_item:
        # Utiliser les UIDs depuis extract_config.UIDS
        real_dx_uids = extract_config_item.get("UIDS", [])
        print(f"   ⚠️  Pas de mappings dans push_config, utilisation de {len(real_dx_uids)} UIDs depuis extract_config.UIDS")
    
    # Si toujours pas de DX_UIDs, utiliser des valeurs par défaut
    if not real_dx_uids:
        real_dx_uids = ["eKHjiGzfBep", "FVJ2v5RgBgL", "PlYGakAhqbk", "ccpDQ5umc0a"]  # DX_UIDs réels par défaut
    
    # Si pas de COCs trouvés, utiliser des valeurs par défaut
    if not real_cocs:
        real_cocs = ["ddJmZUacsvQ", "WZwmzIuRvwV", "t5L9ODSuYOG"]  # COCs réels par défaut
    
    # Utiliser TOUS les COCs et DX_UIDs uniques pour un test complet
    unique_cocs = sorted(list(set(real_cocs)))  # Utiliser TOUS les COCs source
    unique_dx_uids = sorted(list(set(real_dx_uids)))  # Utiliser TOUS les DX_UIDs source
    
    print(f"📊 Données mockées (TEST COMPLET):")
    print(f"   - {len(unique_dx_uids)} DX_UIDs source")
    print(f"   - {len(unique_cocs)} COCs source")
    
    # Utiliser de vrais ORG_UNIT UIDs depuis extract_config ou sync_config
    # En production, ce sont des UIDs DHIS2 comme "rWrCdr321Qu", "XjeRGfqHMrl", etc.
    # extract_config_item a déjà été chargé plus haut
    real_org_units = []
    if extract_config_item:
        # Essayer d'obtenir les ORG_UNITS depuis extract_config
        org_units_from_config = extract_config_item.get("ORG_UNITS", [])
        if org_units_from_config:
            real_org_units = org_units_from_config[:20] if len(org_units_from_config) >= 20 else org_units_from_config  # Prendre les 20 premiers pour un test complet
    
    # Si pas d'ORG_UNITS dans extract_config, utiliser des UIDs réalistes de test
    if not real_org_units:
        # Utiliser des UIDs réalistes (format DHIS2: 11 caractères alphanumériques)
        # Créer 20 ORG_UNITs pour un test complet
        real_org_units = [
            "rWrCdr321Qu", "XjeRGfqHMrl", "F9w3VW1cQmb", "wy1lwIP18SL",
            "aBcDeFgHiJk", "lMnOpQrStUv", "wXyZaBcDeFg", "hIjKlMnOpQr",
            "sTuVwXyZaBc", "dEfGhIjKlMn", "oPqRsTuVwXy", "zAbCdEfGhIj",
            "kLmNoPqRsTu", "vWxYzAbCdEf", "gHiJkLmNoPq", "rStUvWxYzAb",
            "cDeFgHiJkLm", "nOpQrStUvWx", "yZaBcDeFgHi", "jKlMnOpQrSt"
        ]
    
    # Créer des données de test COMPLÈTES avec TOUS les COCs et DX_UIDs
    # Simuler des données où certains DX_UIDs sont manquants pour certaines combinaisons
    # Structure: (DX_UID, ORG_UNIT, COC, VALUE)
    # Utiliser TOUS les ORG_UNITs disponibles (pas seulement les 4 premiers)
    all_org_units = real_org_units
    
    # Créer des données complètes pour tester l'exhaustivity
    # Pour chaque COC, on doit avoir TOUS les DX_UIDs attendus pour que exhaustivity = 1
    # Utiliser des valeurs réalistes (quantités) au lieu de valeurs séquentielles
    import random
    rows = []
    
    # Pour chaque COC, créer des données pour chaque ORG_UNIT
    for coc_idx, coc in enumerate(unique_cocs):
        for ou_idx, org_unit in enumerate(all_org_units):
            # Varier la complétude selon le COC et l'ORG_UNIT pour avoir des données réalistes
            # Les premiers COCs et ORG_UNITs sont plus complets
            if coc_idx < len(unique_cocs) // 2 and ou_idx < len(all_org_units) // 2:
                # Complet: TOUS les DX_UIDs
                for dx_uid in unique_dx_uids:
                    # Valeur réaliste: nombre aléatoire entre 1 et 1000 (quantités de médicaments, etc.)
                    value = random.randint(1, 1000)
                    rows.append((dx_uid, org_unit, coc, value))
            elif coc_idx < len(unique_cocs) // 3 or ou_idx < len(all_org_units) // 3:
                # Presque complet: manque les 5 derniers DX_UIDs
                for dx_uid in unique_dx_uids[:-5]:
                    value = random.randint(1, 1000)
                    rows.append((dx_uid, org_unit, coc, value))
            else:
                # Incomplet: manque les 10 derniers DX_UIDs
                for dx_uid in unique_dx_uids[:-10]:
                    value = random.randint(1, 1000)
                    rows.append((dx_uid, org_unit, coc, value))
    
    print(f"   - {len(rows)} lignes de données créées")
    print(f"   - {len(unique_cocs)} COCs × {len(real_org_units)} ORG_UNITs = {len(unique_cocs) * len(real_org_units)} combinaisons")
    
    data = {
        "DATA_TYPE": ["DATA_ELEMENT"] * len(rows),
        "DX_UID": [r[0] for r in rows],
        "PERIOD": [period] * len(rows),
        "ORG_UNIT": [r[1] for r in rows],
        "CATEGORY_OPTION_COMBO": [r[2] for r in rows],
        "VALUE": [r[3] for r in rows],
        "ATTRIBUTE_OPTION_COMBO": [None] * len(rows),
        "RATE_TYPE": [None] * len(rows),
        "DOMAIN_TYPE": ["AGGREGATED"] * len(rows),
    }
    
    df = pl.DataFrame(data)
    filename = extracts_dir / f"data_{period}.parquet"
    save_to_parquet(df, filename)
    print(f"✅ Données mockées créées: {filename}")
    return filename

def main():
    print("=" * 80)
    print("🧪 TEST DU CALCUL D'EXHAUSTIVITÉ AVEC VRAIES DONNÉES DHIS2")
    print("=" * 80)
    print("\nCe script extrait les vraies données depuis DHIS2 et calcule l'exhaustivité.\n")
    
    # Définir le chemin du workspace
    workspace_path = Path(__file__).parent / "workspace"
    pipeline_path = workspace_path / "pipelines" / "dhis2_exhaustivity"
    pipeline_path.mkdir(parents=True, exist_ok=True)
    
    # Copier les configs si nécessaire
    config_dir = Path(__file__).parent / "configuration"
    workspace_config_dir = pipeline_path / "configuration"
    workspace_config_dir.mkdir(parents=True, exist_ok=True)
    
    # Copier les vraies configs
    for cfg_file in ["extract_config.json", "push_config.json"]:
        src = config_dir / cfg_file
        if src.exists():
            dest = workspace_config_dir / cfg_file
            shutil.copy2(src, dest)
            print(f"✅ {cfg_file} copié vers configuration/")
    
    # Extraire les vraies données depuis DHIS2
    print("\n" + "=" * 80)
    print("📥 EXTRACTION DES VRAIES DONNÉES DEPUIS DHIS2")
    print("=" * 80 + "\n")
    
    try:
        # Importer et appeler la vraie fonction d'extraction
        from pipeline import extract_data
        
        print("🔄 Démarrage de l'extraction des données...")
        extract_data(
            pipeline_path=pipeline_path,
            run_task=True
        )
        print("✅ Extraction terminée avec succès")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 80)
    print("📊 CALCUL D'EXHAUSTIVITÉ")
    print("=" * 80 + "\n")
    
    try:
        # Mock configure_logging pour éviter les erreurs dans l'environnement de test
        import pipeline
        original_configure = pipeline.configure_logging
        def mock_configure_logging(*args, **kwargs):
            pass
        pipeline.configure_logging = mock_configure_logging
        
        # Appeler directement compute_exhaustivity_and_queue au lieu de compute_exhaustivity_data
        # car compute_exhaustivity_data est un PipelineWithTask qui ne s'exécute pas directement
        from pipeline import compute_exhaustivity_and_queue
        from d2d_library.db_queue import Queue
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        from pipeline import get_periods
        
        # Calculer les périodes
        extract_config = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
        extraction_window = extract_config["SETTINGS"].get("EXTRACTION_MONTHS_WINDOW", 6)
        end = datetime.now().strftime("%Y%m")
        end_date = datetime.strptime(end, "%Y%m")
        start = (end_date - relativedelta(months=extraction_window - 1)).strftime("%Y%m")
        exhaustivity_periods = get_periods(start, end)
        
        # Initialiser la queue
        db_path = pipeline_path / "configuration" / ".queue.db"
        push_queue = Queue(db_path)
        
        # Calculer l'exhaustivité pour tous les extracts d'exhaustivité
        extract_ids = ["Fosa_exhaustivity_data_elements", "BCZ_exhaustivity_data_elements"]
        for target_extract in extract_config["DATA_ELEMENTS"].get("EXTRACTS", []):
            extract_id_to_compute = target_extract.get("EXTRACT_UID")
            # Ne traiter que les extracts d'exhaustivité
            if extract_id_to_compute in extract_ids:
                print(f"\n🔄 Calcul exhaustivity pour: {extract_id_to_compute}")
                compute_exhaustivity_and_queue(
                    pipeline_path=pipeline_path,
                    extract_id=extract_id_to_compute,
                    exhaustivity_periods=exhaustivity_periods,
                    push_queue=push_queue,
                )
        
        print("\n✅ Calcul d'exhaustivité terminé avec succès!")
        
        # Restore original function
        pipeline.configure_logging = original_configure
        
        # Afficher les résultats pour chaque extract
        for extract_id_to_show in extract_ids:
            # Afficher les résultats (utiliser le même nom de dossier que extracts)
            if "Fosa" in extract_id_to_show:
                folder_name = "Extract lvl 5"
            elif "BCZ" in extract_id_to_show:
                folder_name = "Extract lvl 3"
            else:
                folder_name = f"Extract {extract_id_to_show}"
            
            processed_dir = pipeline_path / "data" / "processed" / folder_name
            if processed_dir.exists():
                exhaustivity_files = list(processed_dir.glob("exhaustivity_*.parquet"))
                print(f"\n📁 Fichiers d'exhaustivité créés pour {extract_id_to_show}: {len(exhaustivity_files)}")
                
                if exhaustivity_files:
                    print(f"\n📊 RÉSULTATS POUR {extract_id_to_show}")
                    print("=" * 80)
                    
                    for f in exhaustivity_files:
                        df = pl.read_parquet(f)
                        print(f"\n📄 {f.name}:")
                        print(f"   Total combinaisons: {len(df)}")
                        
                        if len(df) > 0:
                            periods = df["PERIOD"].unique().to_list()
                            print(f"   Périodes: {periods}")
                            
                            if "EXHAUSTIVITY_VALUE" in df.columns:
                                exhaustivity_stats = df.group_by("EXHAUSTIVITY_VALUE").agg(
                                    pl.len().alias("count")
                                )
                                print(f"   Exhaustivity:")
                                for row in exhaustivity_stats.iter_rows(named=True):
                                    value = row["EXHAUSTIVITY_VALUE"]
                                    count = row["count"]
                                    pct = (count / len(df)) * 100
                                    status = "✅ COMPLET" if value == 1 else "❌ INCOMPLET"
                                    print(f"     {status}: {count} combinaisons ({pct:.1f}%)")
                                
                                # Afficher quelques exemples
                                print(f"\n   📋 Exemples:")
                                print(df.head(10))
                else:
                    print(f"⚠️  Aucun fichier trouvé dans {processed_dir}")
        
        print("\n" + "=" * 80)
        print("✅ TEST TERMINÉ AVEC SUCCÈS")
        print("=" * 80)
        print(f"\n📝 Résultats disponibles dans:")
        print(f"   - Processed: {pipeline_path / 'data' / 'processed'}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

