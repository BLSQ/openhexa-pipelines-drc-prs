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
from pipeline import compute_exhaustivity_data
from utils import save_to_parquet

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
    
    # Extraire les COCs source réels des mappings
    # IMPORTANT: Utiliser les DX_UIDs SOURCE (clés du mapping) et les COCs SOURCE (clés du CATEGORY_OPTION_COMBO)
    real_cocs = []
    real_dx_uids = []
    if extract_config:
        mappings = extract_config.get("MAPPINGS", {})
        # Prendre TOUS les DX_UIDs source pour avoir des données complètes
        # Les clés du mapping sont les DX_UIDs SOURCE (ceux qui sont dans les données extraites)
        for dx_uid_source, mapping in mappings.items():
            real_dx_uids.append(dx_uid_source)  # Utiliser le DX_UID source (clé du mapping)
            # Prendre TOUS les COCs SOURCE du mapping (clés du CATEGORY_OPTION_COMBO)
            coc_mapping = mapping.get("CATEGORY_OPTION_COMBO", {})
            if coc_mapping:
                # Les clés sont les COCs SOURCE (ceux qui sont dans les données extraites)
                real_cocs.extend(list(coc_mapping.keys()))  # Prendre tous les COCs source
    
    # Si pas de mappings trouvés, utiliser des valeurs par défaut
    if not real_cocs:
        real_cocs = ["ddJmZUacsvQ", "WZwmzIuRvwV", "t5L9ODSuYOG"]  # COCs réels par défaut
    if not real_dx_uids:
        real_dx_uids = ["eKHjiGzfBep", "FVJ2v5RgBgL", "PlYGakAhqbk", "ccpDQ5umc0a"]  # DX_UIDs réels par défaut
    
    # Utiliser TOUS les COCs et DX_UIDs uniques pour un test complet
    unique_cocs = sorted(list(set(real_cocs)))  # Utiliser TOUS les COCs source
    unique_dx_uids = sorted(list(set(real_dx_uids)))  # Utiliser TOUS les DX_UIDs source
    
    print(f"📊 Données mockées (TEST COMPLET):")
    print(f"   - {len(unique_dx_uids)} DX_UIDs source")
    print(f"   - {len(unique_cocs)} COCs source")
    
    # Utiliser de vrais ORG_UNIT UIDs depuis extract_config ou sync_config
    # En production, ce sont des UIDs DHIS2 comme "rWrCdr321Qu", "XjeRGfqHMrl", etc.
    real_org_units = []
    extract_config_full = load_configuration(config_path=pipeline_path / "configuration" / "extract_config.json")
    # Trouver l'extract config correspondant
    extract_config_item = next(
        (e for e in extract_config_full.get("DATA_ELEMENTS", {}).get("EXTRACTS", [])
         if e.get("EXTRACT_UID") == extract_id),
        None
    )
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
    rows = []
    value_counter = 1
    
    # Pour chaque COC, créer des données pour chaque ORG_UNIT
    for coc_idx, coc in enumerate(unique_cocs):
        for ou_idx, org_unit in enumerate(all_org_units):
            # Varier la complétude selon le COC et l'ORG_UNIT pour avoir des données réalistes
            # Les premiers COCs et ORG_UNITs sont plus complets
            if coc_idx < len(unique_cocs) // 2 and ou_idx < len(all_org_units) // 2:
                # Complet: TOUS les DX_UIDs
                for dx_uid in unique_dx_uids:
                    rows.append((dx_uid, org_unit, coc, value_counter))
                    value_counter += 1
            elif coc_idx < len(unique_cocs) // 3 or ou_idx < len(all_org_units) // 3:
                # Presque complet: manque les 5 derniers DX_UIDs
                for dx_uid in unique_dx_uids[:-5]:
                    rows.append((dx_uid, org_unit, coc, value_counter))
                    value_counter += 1
            else:
                # Incomplet: manque les 10 derniers DX_UIDs
                for dx_uid in unique_dx_uids[:-10]:
                    rows.append((dx_uid, org_unit, coc, value_counter))
                    value_counter += 1
    
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
    print("🧪 TEST DU CALCUL D'EXHAUSTIVITÉ AVEC DONNÉES MOCKÉES")
    print("=" * 80)
    print("\nCe script crée des données de test et calcule l'exhaustivité")
    print("sans nécessiter de connexion DHIS2 réelle.\n")
    
    # Définir le chemin du workspace
    workspace_path = Path(__file__).parent / "workspace"
    pipeline_path = workspace_path / "pipelines" / "dhis2_exhaustivity"
    pipeline_path.mkdir(parents=True, exist_ok=True)
    
    # Copier les configs si nécessaire
    config_dir = Path(__file__).parent / "configuration"
    workspace_config_dir = pipeline_path / "configuration"
    workspace_config_dir.mkdir(parents=True, exist_ok=True)
    
    # Copier les vraies configs (pas les configs de test) pour avoir accès aux mappings
    for cfg_file in ["extract_config.json", "push_config.json"]:
        src = config_dir / cfg_file
        if src.exists():
            dest = workspace_config_dir / cfg_file
            shutil.copy2(src, dest)
            print(f"✅ {cfg_file} copié vers configuration/")
    
    # Créer des données mockées
    print("\n" + "=" * 80)
    print("📝 CRÉATION DE DONNÉES MOCKÉES")
    print("=" * 80 + "\n")
    
    # Utiliser le vrai extract_id (sans _test) pour que compute_exhaustivity_data le trouve
    extract_id = "Fosa_exhaustivity_data_elements"
    
    # Créer des données pour plusieurs périodes (6 mois comme en production)
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    end = datetime.now().strftime("%Y%m")
    end_date = datetime.strptime(end, "%Y%m")
    start = (end_date - relativedelta(months=5)).strftime("%Y%m")  # 6 mois de données
    
    # Générer toutes les périodes
    periods = []
    current = datetime.strptime(start, "%Y%m")
    end_dt = datetime.strptime(end, "%Y%m")
    while current <= end_dt:
        periods.append(current.strftime("%Y%m"))
        current = current + relativedelta(months=1)
    
    print(f"📅 Périodes à créer: {len(periods)} ({start} à {end})")
    
    # Créer des données pour chaque période
    for period in periods:
        create_mock_extract_data(pipeline_path, extract_id, period)
    
    print("\n" + "=" * 80)
    print("📊 CALCUL D'EXHAUSTIVITÉ")
    print("=" * 80 + "\n")
    
    try:
        # Calcul exhaustivity
        compute_exhaustivity_data(pipeline_path=pipeline_path, run_task=True)
        print("\n✅ Calcul d'exhaustivité terminé avec succès!")
        
        # Afficher les résultats (utiliser le même nom de dossier que extracts)
        if "Fosa" in extract_id:
            folder_name = "Extract lvl 5"
        elif "BCZ" in extract_id:
            folder_name = "Extract lvl 3"
        else:
            folder_name = f"Extract {extract_id}"
        
        processed_dir = pipeline_path / "data" / "processed" / folder_name
        if processed_dir.exists():
            exhaustivity_files = list(processed_dir.glob("exhaustivity_*.parquet"))
            print(f"\n📁 Fichiers d'exhaustivité créés: {len(exhaustivity_files)}")
            
            if exhaustivity_files:
                print("\n" + "=" * 80)
                print("📊 RÉSULTATS DÉTAILLÉS")
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
        print(f"   - Processed: {processed_dir}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

