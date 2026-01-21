import os
import hashlib
from dotenv import load_dotenv

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

import kaggle
from pathlib import Path

# Get project root directory (go up one level from tests/)
PROJECT_ROOT = Path(__file__).parent.parent


def test_kaggle_dataset_download():
    """
    Test: Download the NBA dataset from Kaggle using the API to data/temp/v1/.
    This automates the download step: kaggle datasets download -d eoinamoore/historical-nba-data-and-player-box-scores -p data/temp/
    """
    dataset_handle = "eoinamoore/historical-nba-data-and-player-box-scores"
    version = "v1_nbadataset_temp_data"
    # Use absolute path from project root
    download_path = PROJECT_ROOT / "data" / "temp" / version
    
    try:
        # Authenticate with Kaggle
        api = kaggle.api
        api.authenticate()
        
        # Create the download directory if it doesn't exist
        download_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n📥 Downloading dataset to {download_path}...")
        
        # Download the dataset files
        # unzip=True extracts the files, unzip=False keeps as zip
        # Convert to string for Kaggle API
        api.dataset_download_files(dataset_handle, path=str(download_path), unzip=True)
        
        # Verify files were downloaded
        downloaded_files = [f for f in os.listdir(download_path) 
                           if os.path.isfile(os.path.join(download_path, f)) 
                           and not f.startswith('.')]
        
        print(f"✅ Download Successful!")
        print(f"📁 Files downloaded to: {download_path}")
        print(f"📊 Found {len(downloaded_files)} file(s).")
        if downloaded_files:
            print(f"📄 Files: {downloaded_files[:5]}")  # Show first 5 files
        
        # Logic check: If we found files, the download was successful
        assert len(downloaded_files) > 0, f"❌ No files found in {download_path} after download."
        
    except Exception as e:
        assert False, f"❌ Could not download dataset from Kaggle. Error: {e}"

def test_manual_md5_hash_per_file():
    """
    Test: Hash EACH CSV file individually using hashlib.md5().
    Manual validation step before automation - hash each file, not the folder.
    """
    version = "v1_nbadataset_temp_data"
    download_path = PROJECT_ROOT / "data" / "temp" / version
    
    try:
        # Get all CSV files
        csv_files = [f for f in download_path.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.csv' 
                    and not f.name.startswith('.')]
        
        assert len(csv_files) > 0, f"❌ No CSV files found in {download_path}"
        
        print(f"\n🔐 Computing MD5 hash for each file...")
        
        file_hashes = {}
        for csv_file in sorted(csv_files):
            # Hash each file individually
            md5_hash = hashlib.md5()
            with open(csv_file, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    md5_hash.update(chunk)
            
            file_hash = md5_hash.hexdigest()
            file_hashes[csv_file.name] = file_hash
            
            file_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            print(f"📄 {csv_file.name}")
            print(f"   MD5: {file_hash}")
            print(f"   Size: {file_size:.2f} MB")
        
        # Verify we hashed files
        assert len(file_hashes) > 0, "❌ No files were hashed"
        
        print(f"\n✅ Hashed {len(file_hashes)} file(s) individually")
        
    except Exception as e:
        assert False, f"❌ Error hashing files. Error: {e}"