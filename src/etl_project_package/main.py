import os
from pathlib import Path
from sys import meta_path 
from .compare import get_data_path, compare_and_decide, log_decision, save_current_state, get_metadata_paths
from .kaggle_connect import download_kaggle_dataset_to, validate_downloaded_files, get_kaggle_dataset_version,get_saved_kaggle_version,save_kaggle_version
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
        print("Only 1 complete version, nothing to clean")
        return
    
    # Sort by name (v1 < v2 < v3...), delete OLDEST
    complete_versions.sort(key=lambda p: p.name)
    oldest_version = complete_versions[0]
    
    print(f"INGEST detected → Deleting oldest version: {oldest_version.name}")
    shutil.rmtree(oldest_version)
    print(f"Cleaned up {oldest_version.name}")

    

def cron_run(project_root: Path) -> None:
    """Smart cron: Check Kaggle FIRST before downloading."""
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Cron started: {start_time}")
    
    # 1. CHECK KAGGLE VERSION FIRST (no download yet)
    print("Checking Kaggle dataset version...")
    kaggle_version = get_kaggle_dataset_version()  # NEW FUNCTION
    
    # 2. COMPARE vs saved baseline version
    saved_version = get_saved_kaggle_version(project_root)  # NEW FUNCTION
    if kaggle_version == saved_version and saved_version is not None:
        print(f"→ Kaggle unchanged (v{saved_version}) → SKIP download")
        log_decision(
            "skip", f"Kaggle dataset unchanged (version {saved_version})", 
            {"timestamp": start_time}, project_root
        )
        return  # NO DOWNLOAD!
    
    # 3. Kaggle CHANGED → Download + full pipeline
    print("→ Kaggle changed → Downloading...")
    version = next_local_version_name(project_root)
    data_path = get_data_path(project_root, version)
    
    try:
        download_kaggle_dataset_to(data_path)
        decision, reason, details = compare_and_decide(project_root, version, data_path)
        metadata_paths = get_metadata_paths(project_root)
        
        log_decision(decision, reason, details, project_root, version)
        print(f"Decision: {decision} - {reason}")
        
        if decision == "ingest":
            save_current_state(data_path, metadata_paths, details.get('current_state', {}))
            print("✅ New baseline established!")
            save_kaggle_version(project_root, kaggle_version)  # NEW
            cleanup_temp_on_ingest(project_root, data_path)
        else:
            print(f"{decision.upper()} → No persistence needed")
            
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        if data_path.exists():
            shutil.rmtree(data_path)
        log_decision("alert", f"Cron failed: {str(e)}", 
                    {"timestamp": start_time, "error": str(e)}, project_root, version)
        raise
    
    print("Cron completed successfully")




if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[2]  # repo root, adjust if needed
    print(f"Project root: {PROJECT_ROOT}")
    cron_run(PROJECT_ROOT)

    

