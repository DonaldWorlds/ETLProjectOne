import os
from dotenv import load_dotenv

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

import kaggle
from pathlib import Path

# Get project root directory (go up one level from tests/)
PROJECT_ROOT = Path(__file__).parent.parent

def test_nba_dataset_reachability():
    """
    Test: Verify we can talk to Kaggle and see the file list without downloading.
    """
    dataset_handle = "eoinamoore/historical-nba-data-and-player-box-scores"
    
    try:
        # Authenticate with the server
        api = kaggle.api
        api.authenticate()
        
        # This only fetches the metadata/file list (Lightweight)
        files = api.dataset_list_files(dataset_handle).files
        
        file_names = [f.name for f in files]
        
        print(f"\n✅ Connection Successful!")
        print(f"📊 Dataset contains {len(file_names)} files.")
        print(f"📄 Sample files: {file_names[:3]}") # Just shows the first 3 names
        
        # Logic check: If we found files, the dataset definitely exists
        assert len(file_names) > 0
        
    except Exception as e:
        assert False, f"❌ Could not reach Kaggle. Error: {e}"

def test_manual_download_validation():
    """
    Test: Verify that the dataset has been manually downloaded to data/temp/.
    This validates Step 1 manual download step of downloading data to temp folder from source
    """
    temp_dir = "data/temp"
    
    try:
        # Check if temp directory exists
        assert os.path.exists(temp_dir), f"❌ Directory {temp_dir} does not exist. Please download the dataset manually first."
        
        # Get list of files in temp directory
        files = [f for f in os.listdir(temp_dir) if os.path.isfile(os.path.join(temp_dir, f))]
        
        # Filter out hidden files and common temp files
        visible_files = [f for f in files if not f.startswith('.')]
        
        print(f"\n✅ Manual Download Validation!")
        print(f"📁 Directory {temp_dir} exists.")
        print(f"📊 Found {len(visible_files)} file(s) in temp directory.")
        if visible_files:
            print(f"📄 Files: {visible_files[:5]}")  # Show first 5 files
        
        # Logic check: If we found files, the manual download was successful
        assert len(visible_files) > 0, f"❌ No files found in {temp_dir}. Please download the dataset manually first."
        
    except AssertionError as e:
        raise e
    except Exception as e:
        assert False, f"❌ Error validating manual download. Error: {e}"

