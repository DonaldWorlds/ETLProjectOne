import os
import pytest
import kaggle
from dotenv import load_dotenv

# 1. LOAD THE DOTENV FIRST
load_dotenv()

def test_kaggle_authentication():
    # 2. MANUALLY SET THE ENV VARS (Double-check for the library)
    # This ensures Kaggle doesn't go looking for the hidden .json file
    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')
    
    try:
        kaggle.api.authenticate()
        auth_success = True
    except Exception:
        auth_success = False
    
    assert auth_success is True, "Kaggle API key in .env is missing or invalid!"
    print("kaggle authentication test passed")