import os
from dotenv import load_dotenv

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

import kaggle

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