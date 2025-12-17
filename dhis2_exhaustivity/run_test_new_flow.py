#!/usr/bin/env python3
"""Script pour tester le nouveau flow avec compute_exhaustivity_data comme task séparée."""

import sys
import os
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

# Patcher current_run avant les imports
import openhexa.sdk
openhexa.sdk.current_run = MockCurrentRun()

# Importer les modules après le patch
from pipeline import extract_data, compute_exhaustivity_data, update_dataset_org_units
from utils import load_configuration

def main():
    print("=" * 80)
    print("🧪 TEST DU NOUVEAU FLOW DU PIPELINE")
    print("=" * 80)
    print("\nNouveau flow:")
    print("  1. extract_data")
    print("  2. compute_exhaustivity_data (nouvelle task séparée)")
    print("  3. update_dataset_org_units")
    print("  4. push_data (skipped pour le test)")
    print("\n" + "=" * 80 + "\n")
    
    # Définir le chemin du workspace
    workspace_path = Path(__file__).parent / "workspace"
    pipeline_path = workspace_path / "pipelines" / "dhis2_exhaustivity"
    pipeline_path.mkdir(parents=True, exist_ok=True)
    
    # Copier les configs si nécessaire
    config_files_dir = Path(__file__).parent / "config_files"
    workspace_config_dir = pipeline_path / "config_files"
    workspace_config_dir.mkdir(parents=True, exist_ok=True)
    
    # Copier les configs nécessaires
    for cfg_file in ["extract_config_test.json", "push_config.json", "sync_config.json"]:
        src = config_files_dir / cfg_file
        if src.exists():
            # Pour extract_config, utiliser le test si disponible, sinon l'original
            if cfg_file == "extract_config_test.json":
                dest = workspace_config_dir / "extract_config.json"
            else:
                dest = workspace_config_dir / cfg_file
            shutil.copy2(src, dest)
            print(f"✅ {cfg_file} copié vers {dest.name}")
    
    # Vérifier que la config existe
    config_path = workspace_config_dir / "extract_config.json"
    if not config_path.exists():
        # Essayer avec la config originale
        original_config = config_files_dir / "extract_config.json"
        if original_config.exists():
            shutil.copy2(original_config, config_path)
            print(f"✅ extract_config.json copié depuis config_files")
        else:
            print(f"❌ Configuration non trouvée: {config_path}")
            sys.exit(1)
    
    # Afficher la config
    with open(config_path) as f:
        config = json.load(f)
    print(f"\n📋 Configuration:")
    if config.get("DATA_ELEMENTS", {}).get("EXTRACTS"):
        extract = config["DATA_ELEMENTS"]["EXTRACTS"][0]
        print(f"   - Extract ID: {extract.get('EXTRACT_UID')}")
        print(f"   - Data Elements: {len(extract.get('UIDS', []))}")
        print(f"   - Org Units Level: {extract.get('ORG_UNITS_LEVEL')}")
    
    print("\n" + "=" * 80)
    print("🔄 ÉTAPE 1: EXTRACTION DES DONNÉES")
    print("=" * 80 + "\n")
    
    try:
        # Étape 1: Extraction
        extract_data(pipeline_path=pipeline_path, run_task=True)
        print("\n✅ Extraction terminée avec succès!")
        
        # Vérifier que des fichiers ont été extraits
        extracts_dir = pipeline_path / "data" / "extracts"
        if extracts_dir.exists():
            extract_folders = [d for d in extracts_dir.iterdir() if d.is_dir()]
            print(f"\n📁 Dossiers d'extraction trouvés: {len(extract_folders)}")
            for folder in extract_folders:
                parquet_files = list(folder.glob("data_*.parquet"))
                print(f"   - {folder.name}: {len(parquet_files)} fichiers parquet")
        
        print("\n" + "=" * 80)
        print("📊 ÉTAPE 2: CALCUL D'EXHAUSTIVITÉ (nouvelle task séparée)")
        print("=" * 80 + "\n")
        
        # Étape 2: Calcul exhaustivity (nouvelle task séparée)
        compute_exhaustivity_data(pipeline_path=pipeline_path, run_task=True)
        print("\n✅ Calcul d'exhaustivité terminé avec succès!")
        
        # Vérifier que des fichiers d'exhaustivité ont été créés
        processed_dir = pipeline_path / "data" / "processed"
        if processed_dir.exists():
            processed_folders = [d for d in processed_dir.iterdir() if d.is_dir()]
            print(f"\n📁 Dossiers de résultats trouvés: {len(processed_folders)}")
            for folder in processed_folders:
                exhaustivity_files = list(folder.glob("exhaustivity_*.parquet"))
                print(f"   - {folder.name}: {len(exhaustivity_files)} fichiers exhaustivity")
                if exhaustivity_files:
                    # Afficher un aperçu du premier fichier
                    try:
                        import polars as pl
                        df = pl.read_parquet(exhaustivity_files[0])
                        print(f"     Aperçu du premier fichier ({exhaustivity_files[0].name}):")
                        print(f"       - Lignes: {len(df)}")
                        print(f"       - Colonnes: {df.columns}")
                        if len(df) > 0:
                            periods = df["PERIOD"].unique().to_list()
                            print(f"       - Périodes: {sorted(periods)}")
                            if "EXHAUSTIVITY_VALUE" in df.columns:
                                values = df["EXHAUSTIVITY_VALUE"].unique().to_list()
                                print(f"       - Valeurs exhaustivity: {sorted(values)}")
                    except Exception as e:
                        print(f"     ⚠️  Erreur lecture: {e}")
        
        # Vérifier la queue
        from d2d_library.db_queue import Queue
        db_path = pipeline_path / "config_files" / ".queue.db"
        push_queue = Queue(db_path)
        queue_count = push_queue.count()
        print(f"\n📋 Queue: {queue_count} éléments en attente de push")
        
        # Afficher un résumé détaillé des résultats
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ DÉTAILLÉ DES RÉSULTATS")
        print("=" * 80)
        if processed_dir.exists():
            for folder in processed_dir.iterdir():
                if folder.is_dir():
                    exhaustivity_files = list(folder.glob("exhaustivity_*.parquet"))
                    if exhaustivity_files:
                        print(f"\n📁 Extract: {folder.name}")
                        try:
                            import polars as pl
                            all_data = []
                            for f in exhaustivity_files:
                                df = pl.read_parquet(f)
                                all_data.append(df)
                            
                            if all_data:
                                combined_df = pl.concat(all_data)
                                print(f"   Total combinaisons: {len(combined_df)}")
                                
                                # Statistiques par période
                                periods = combined_df["PERIOD"].unique().to_list()
                                print(f"   Périodes: {sorted(periods)}")
                                
                                # Statistiques exhaustivity
                                if "EXHAUSTIVITY_VALUE" in combined_df.columns:
                                    exhaustivity_stats = combined_df.group_by("EXHAUSTIVITY_VALUE").agg(
                                        pl.count().alias("count")
                                    )
                                    print(f"   Exhaustivity:")
                                    for row in exhaustivity_stats.iter_rows(named=True):
                                        value = row["EXHAUSTIVITY_VALUE"]
                                        count = row["count"]
                                        pct = (count / len(combined_df)) * 100
                                        print(f"     - {value}: {count} combinaisons ({pct:.1f}%)")
                                
                                # Statistiques par COC
                                cocs = combined_df["CATEGORY_OPTION_COMBO"].unique().to_list()
                                print(f"   COCs: {len(cocs)}")
                                
                                # Statistiques par ORG_UNIT
                                org_units = combined_df["ORG_UNIT"].unique().to_list()
                                print(f"   ORG_UNITs: {len(org_units)}")
                                
                                # Afficher quelques exemples
                                print(f"\n   📋 Exemples (5 premières lignes):")
                                print(combined_df.head(5))
                        except Exception as e:
                            print(f"   ⚠️  Erreur lors de l'analyse: {e}")
        
        print("\n" + "=" * 80)
        print("🔄 ÉTAPE 3: SYNC DATASET ORG UNITS (skipped pour le test)")
        print("=" * 80 + "\n")
        
        # Étape 3: Sync org units (on skip pour le test car ça nécessite une connexion DHIS2)
        print("⚠️  Sync dataset org units skipped (nécessite connexion DHIS2)")
        
        print("\n" + "=" * 80)
        print("🔄 ÉTAPE 4: PUSH DATA (skipped pour le test)")
        print("=" * 80 + "\n")
        
        # Étape 4: Push (on skip pour le test car ça nécessite une connexion DHIS2)
        print("⚠️  Push data skipped (nécessite connexion DHIS2)")
        print("   Pour tester le push, utilisez: push_data(pipeline_path=pipeline_path, run_task=True)")
        
        print("\n" + "=" * 80)
        print("✅ TEST TERMINÉ AVEC SUCCÈS")
        print("=" * 80)
        print("\n📊 RÉSUMÉ:")
        print("  ✅ Extraction: OK")
        print("  ✅ Calcul exhaustivity (nouvelle task): OK")
        print("  ⏭️  Sync org units: Skipped (nécessite connexion DHIS2)")
        print("  ⏭️  Push: Skipped (nécessite connexion DHIS2)")
        print("\n📝 Les résultats sont disponibles dans:")
        print(f"   - Extracts: {extracts_dir}")
        print(f"   - Processed: {processed_dir}")
        print(f"   - Logs: {pipeline_path / 'logs'}")
        print("\n💡 Pour visualiser les résultats en détail:")
        print("   - Ouvrez les fichiers .parquet dans processed/")
        print("   - Utilisez pandas/polars pour analyser les données")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

