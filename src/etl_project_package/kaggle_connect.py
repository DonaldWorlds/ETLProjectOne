import os
from pathlib import Path

import time

from dotenv import load_dotenv

from typing import Optional

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')


import kaggle

DATASET_HANDLE = "eoinamoore/historical-nba-data-and-player-box-scores"




def download_kaggle_dataset_to(target_dir: Path, max_retries: int = 3) -> None:
    """
    Download Kaggle dataset with retries + validation. Raises on failure.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    
    #Verify dotenv vars are loaded
    if not os.getenv('KAGGLE_USERNAME') or not os.getenv('KAGGLE_KEY'):
        raise RuntimeError("KAGGLE_USERNAME or KAGGLE_KEY missing from .env")
    
    api = kaggle.api  # Fixed: no ()
    
    for attempt in range(max_retries):
        try:
            print(f"Kaggle download attempt {attempt + 1}/{max_retries}")
            
            # Download with unzip=True
            api.dataset_download_files(
                DATASET_HANDLE,
                path=str(target_dir),
                unzip=True,
                quiet=False
            )
            
            # IMMEDIATE VALIDATION
            time.sleep(2)  # Let filesystem settle
            csv_files = list(target_dir.glob("*.csv"))
            
            if len(csv_files) < 3:
                raise RuntimeError(f"Only {len(csv_files)} CSVs found (expected 3+)")
            
            print(f"Download success: {len(csv_files)} CSV files")
            return  # Success!
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise RuntimeError(f"All {max_retries} download attempts failed")
            time.sleep(30)  # Wait before retry


def validate_downloaded_files(data_path: Path) -> bool:
    """Check if Kaggle download actually produced CSV files"""
    csv_files = [p for p in data_path.iterdir() 
                if p.suffix == '.csv' and p.is_file()]
    
    expected_files = {'games.csv', 'players.csv', 'box_scores.csv'}
    found_files = {p.name for p in csv_files}
    
    # Must have CSV files AND expected NBA files
    has_csvs = len(csv_files) >= 3
    has_expected = bool(expected_files & found_files)
    
    print(f" Found {len(csv_files)} CSVs: {list(found_files)[:3]}...")
    return has_csvs and has_expected


def get_kaggle_dataset_version() -> Optional[str]:
    """Check Kaggle dataset last-modified via API."""
    try:
        api = kaggle.api
        api.authenticate()
        dataset_info = api.dataset_version("eoinamoore/historical-nba-data-and-player-box-scores")
        return dataset_info.get('lastUpdated', 'unknown')
    except:
        return None


def get_kaggle_dataset_version() -> Optional[str]:
    """Check Kaggle dataset last-modified via API."""
    try:
        api = kaggle.KaggleApi()
        api.authenticate()
        dataset_info = api.dataset_version(DATASET_HANDLE)
        return dataset_info.get('lastUpdated', 'unknown')
    except Exception as e:
        print(f"Kaggle version check failed: {e}")
        return None

def get_saved_kaggle_version(project_root: Path) -> Optional[str]:
    """Read saved Kaggle version from metadata."""
    version_file = project_root / "metadata" / "kaggle_version.txt"
    if version_file.exists():
        return version_file.read_text().strip()
    return None

def save_kaggle_version(project_root: Path, version: str) -> None:
    """Save current Kaggle version to metadata."""
    version_file = project_root / "metadata" / "kaggle_version.txt"
    version_file.parent.mkdir(parents=True, exist_ok=True)
    version_file.write_text(version)


