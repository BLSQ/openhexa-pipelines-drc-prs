#!/usr/bin/env python3
"""Script pour tester le calcul d'exhaustivité avec des données mockées (sans connexion DHIS2)."""

import sys
import json
import shutil
from pathlib import Path

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
import pandas as pd
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
    
    # Créer des données de test avec quelques DX_UIDs et COCs
    # Simuler des données où certains DX_UIDs sont manquants pour certaines combinaisons
    # Structure: (DX_UID, ORG_UNIT, COC, VALUE)
    rows = [
        # COC1, OU1 - complet (4 DX_UIDs)
        ("imqSmhfQTPN", "OU1", "COC1", 10),
        ("hgjnpRWXunB", "OU1", "COC1", 20),
        ("fHyOLs6mPEr", "OU1", "COC1", 30),
        ("PMuB7JCsLsg", "OU1", "COC1", 40),
        # COC2, OU1 - incomplet (manque PMuB7JCsLsg)
        ("imqSmhfQTPN", "OU1", "COC2", 15),
        ("hgjnpRWXunB", "OU1", "COC2", 25),
        ("fHyOLs6mPEr", "OU1", "COC2", 35),
        # COC1, OU2 - complet
        ("imqSmhfQTPN", "OU2", "COC1", 12),
        ("hgjnpRWXunB", "OU2", "COC1", 22),
        ("fHyOLs6mPEr", "OU2", "COC1", 32),
        ("PMuB7JCsLsg", "OU2", "COC1", 42),
        # COC3, OU2 - incomplet (manque fHyOLs6mPEr et PMuB7JCsLsg)
        ("imqSmhfQTPN", "OU2", "COC3", 18),
        ("hgjnpRWXunB", "OU2", "COC3", 28),
        # COC1, OU3 - complet
        ("imqSmhfQTPN", "OU3", "COC1", 11),
        ("hgjnpRWXunB", "OU3", "COC1", 21),
        ("fHyOLs6mPEr", "OU3", "COC1", 31),
        ("PMuB7JCsLsg", "OU3", "COC1", 41),
        # COC1, OU4 - complet
        ("imqSmhfQTPN", "OU4", "COC1", 13),
        ("hgjnpRWXunB", "OU4", "COC1", 23),
        ("fHyOLs6mPEr", "OU4", "COC1", 33),
        ("PMuB7JCsLsg", "OU4", "COC1", 43),
    ]
    
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
    
    df = pd.DataFrame(data)
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
    config_files_dir = Path(__file__).parent / "config_files"
    workspace_config_dir = pipeline_path / "config_files"
    workspace_config_dir.mkdir(parents=True, exist_ok=True)
    
    # Copier les configs nécessaires
    for cfg_file in ["extract_config_test.json", "push_config_test.json"]:
        src = config_files_dir / cfg_file
        if src.exists():
            if cfg_file == "extract_config_test.json":
                dest = workspace_config_dir / "extract_config.json"
            elif cfg_file == "push_config_test.json":
                dest = workspace_config_dir / "push_config.json"
            else:
                dest = workspace_config_dir / cfg_file
            shutil.copy2(src, dest)
            print(f"✅ {cfg_file} copié")
    
    # Créer des données mockées
    print("\n" + "=" * 80)
    print("📝 CRÉATION DE DONNÉES MOCKÉES")
    print("=" * 80 + "\n")
    
    extract_id = "Fosa_exhaustivity_data_elements_test"
    period = "202507"
    
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
                                pl.count().alias("count")
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

