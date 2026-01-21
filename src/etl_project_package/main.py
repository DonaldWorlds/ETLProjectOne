import os
from pathlib import Path 
from .compare import get_data_path, compare_and_decide, log_decision
from .kaggle_connect import download_kaggle_dataset_to, validate_downloaded_files
import shutil
from datetime import datetime

from dotenv import load_dotenv

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # repo root, adjust if needed


def next_local_version_name(project_root: Path, base_name: str = "nbadataset_temp_data") -> str:
    temp_root = get_data_path(project_root)  # data/temp/
    temp_root.mkdir(parents=True, exist_ok=True)

    existing = [p.name for p in temp_root.iterdir() if p.is_dir()]
    max_n = 0
    for name in existing:
        # expect names like v1_nbadataset_temp_data
        if name.startswith("v") and "_" in name:
            try:
                num_str = name[1:name.index("_")]
                num = int(num_str)
                max_n = max(max_n, num)
            except ValueError:
                continue

    next_n = max_n + 1
    return f"v{next_n}_{base_name}"




def cleanup_temp_on_ingest(project_root: Path, current_version_path: Path) -> None:
    """
    When we decide to INGEST, delete the oldest COMPLETE version folder.
    Keeps your data/temp/ clean with only recent + working versions.
    """
    temp_root = project_root / "data" / "temp"
    
    # Find ALL complete version folders (have CSV files)
    complete_versions = []
    for version_folder in temp_root.iterdir():
        if (version_folder.is_dir() and 
            version_folder.name.startswith('v') and 
            any(version_folder.glob("*.csv"))):  # Has CSV files
            complete_versions.append(version_folder)
    
    if len(complete_versions) <= 1:
        print("ℹ️  Only 1 complete version, nothing to clean")
        return
    
    # Sort by name (v1 < v2 < v3...), delete OLDEST
    complete_versions.sort(key=lambda p: p.name)
    oldest_version = complete_versions[0]
    
    print(f"🗑️  INGEST detected → Deleting oldest version: {oldest_version.name}")
    shutil.rmtree(oldest_version)
    print(f"✅ Cleaned up {oldest_version.name}")

    

def cron_run(project_root: Path) -> None:
    """Production cron: fail fast, cleanup failures, never leave empty folders"""
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"🚀 Cron started: {start_time}")
    
    version = next_local_version_name(project_root)
    data_path = get_data_path(project_root, version)
    
    # CRITICAL: Don't create folder until we're SURE we can download
    try:
        # 1. DOWNLOAD FIRST (with validation)
        print(f"📥 Downloading to {data_path}...")
        download_kaggle_dataset_to(data_path)  # Creates folder + validates
        
        # 2. ONLY if download succeeded → compare
        print("🔍 Comparing...")
        decision, reason, comparison_details = compare_and_decide(
            project_root=project_root, version=version, data_path=data_path
        )
        
        # 3. Log result
        log_decision(decision, reason, comparison_details, project_root, version)
        print(f"✅ Decision: {decision} - {reason}")
        
        # 4. Cleanup only on ingest
        if decision == "ingest":
            cleanup_temp_on_ingest(project_root, data_path)
            
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        
        # IMMEDIATE CLEANUP - NO EMPTY FOLDERS
        if data_path.exists():
            shutil.rmtree(data_path)
            print(f"🗑️ Cleaned failed folder: {data_path.name}")
        
        # Log failure to ingestion_log
        log_decision(
            "alert", f"Cron failed: {str(e)}", 
            {"timestamp": start_time, "error": str(e)}, 
            project_root, version
        )
        raise
    
    print("🎉 Cron completed successfully")



if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[2]  # repo root, adjust if needed
    print(f"Project root: {PROJECT_ROOT}")
    cron_run(PROJECT_ROOT)

