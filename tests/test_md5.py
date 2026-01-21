import os
import sys
import hashlib
import re
from datetime import datetime
from dotenv import load_dotenv

# Step 1: Set up secrets
load_dotenv()
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

import kaggle
from pathlib import Path

# Get project root directory (go up one level from tests/)
PROJECT_ROOT = Path(__file__).parent.parent

# Add src directory to path for imports
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from compare import compare_and_decide, log_decision, get_project_root


def test_manual_md5_hash_per_file():
    """
    Test: Hash EACH CSV file individually using hashlib.md5().
    Saves metadata to:
    - metadata/hashes/{filename}.md5
    - metadata/row_counts/{filename}.rows
    - metadata/ingestion_log.md (append-only)
    """
    version = "v1_nbadataset_temp_data"
    download_path = PROJECT_ROOT / "data" / "temp" / version
    
    try:
        # Get all CSV files
        csv_files = [f for f in download_path.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.csv' 
                    and not f.name.startswith('.')]
        
        assert len(csv_files) > 0, f"❌ No CSV files found in {download_path}"
        
        # Create metadata directories
        hashes_dir = PROJECT_ROOT / "metadata" / "hashes"
        row_counts_dir = PROJECT_ROOT / "metadata" / "row_counts"
        hashes_dir.mkdir(parents=True, exist_ok=True)
        row_counts_dir.mkdir(parents=True, exist_ok=True)
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_data = []  # Store all file metadata for log entry
        
        print(f"\n🔐 Computing MD5 hash and row counts for each file...")
        
        for csv_file in sorted(csv_files):
            # Hash each file individually
            md5_hash = hashlib.md5()
            with open(csv_file, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    md5_hash.update(chunk)
            
            file_hash = md5_hash.hexdigest()
            file_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            
            # Count rows (includes header)
            total_rows = 0
            with open(csv_file, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f)
            data_rows = total_rows - 1  # Exclude header
            
            # Get base filename without extension
            base_name = csv_file.stem
            
            # Save hash to metadata/hashes/{filename}.md5
            hash_file = hashes_dir / f"{base_name}.md5"
            with open(hash_file, 'w') as f:
                f.write(f"{current_date} {file_hash} {csv_file.name}\n")
            
            # Save row count to metadata/row_counts/{filename}.rows
            rows_file = row_counts_dir / f"{base_name}.rows"
            with open(rows_file, 'w') as f:
                f.write(f"{current_date}\n")
                f.write(f"Total rows: {total_rows}\n")
                f.write(f"Data rows: {data_rows}\n")
            
            # Store data for log entry
            file_data.append({
                'name': csv_file.name,
                'hash': file_hash,
                'size': file_size,
                'data_rows': data_rows
            })
            
            print(f"📄 {csv_file.name}")
            print(f"   MD5: {file_hash}")
            print(f"   Size: {file_size:.2f} MB")
            print(f"   Total rows: {total_rows:,} (Data: {data_rows:,})")
            print(f"   💾 Hash saved to: {hash_file}")
            print(f"   💾 Row count saved to: {rows_file}")
        
        # Append to ingestion log (append-only)
        ingestion_log = PROJECT_ROOT / "metadata" / "ingestion_log.md"
        
        # Build validation results table
        validation_table = "| File | Size (MB) | Rows | MD5 Hash |\n"
        validation_table += "|------|-----------|------|----------|\n"
        
        for data in file_data:
            validation_table += f"| {data['name']} | {data['size']:.2f} | {data['data_rows']:,} | {data['hash'][:12]}... |\n"
        
        log_entry = f"""## {current_date} — Initial Baseline Ingestion

**Source:** Kaggle – eoinamoore/historical-nba-data-and-player-box-scores  
**Run type:** Manual (baseline)  
**Dataset version:** {version}  
**Action:** Baseline recorded (no comparison)

### Files processed
"""
        for data in file_data:
            log_entry += f"- {data['name']}\n"
        
        log_entry += f"""
### Validation results
{validation_table}

### Notes
- First known stable snapshot
- Hashes stored in `metadata/hashes/`
- Row counts stored in `metadata/row_counts/`
- No ingestion to BigQuery performed at this stage

**Status:** ✅ Success

---
"""
        
        # Append to log file (create if doesn't exist, add header if new)
        if not ingestion_log.exists():
            with open(ingestion_log, 'w') as f:
                f.write("# Ingestion Log\n\n---\n\n")
        
        with open(ingestion_log, 'a') as f:
            f.write(log_entry)
        
        print(f"\n✅ Processed {len(csv_files)} file(s) individually")
        print(f"📝 Ingestion log updated: {ingestion_log}")
        
    except Exception as e:
        assert False, f"❌ Error hashing files. Error: {e}"

    




def test_view_saved_hashes():
    """
    Test: View the saved MD5 hashes from metadata/hashes/
    """
    hashes_dir = PROJECT_ROOT / "metadata" / "hashes"
    
    if not hashes_dir.exists():
        print(f"❌ No hashes directory found at {hashes_dir}")
        print("   Run test_manual_md5_hash_per_file() first to generate hashes.")
        return
    
    hash_files = sorted(hashes_dir.glob("*.md5"))
    
    if not hash_files:
        print(f"❌ No hash files found in {hashes_dir}")
        return
    
    print(f"\n📋 Saved Hash Records")
    print(f"{'File':<40} {'Date':<12} {'MD5 Hash':<35}")
    print("-" * 90)
    
    for hash_file in hash_files:
        with open(hash_file, 'r') as f:
            line = f.read().strip()
            parts = line.split(' ', 2)
            if len(parts) == 3:
                date, hash_val, filename = parts
                print(f"{filename:<40} {date:<12} {hash_val:<35}")
    
    print(f"\n💾 Hash files in: {hashes_dir}")





def test_ingestion_log_structure():
    """
    Test: Validate ingestion_log.md structure and format.
    Ensures it's append-only, human-readable, and answers key questions.
    """
    ingestion_log = PROJECT_ROOT / "metadata" / "ingestion_log.md"
    
    if not ingestion_log.exists():
        print(f"⚠️  Ingestion log not found at {ingestion_log}")
        print("   Run test_manual_md5_hash_per_file() first to create it.")
        return
    
    with open(ingestion_log, 'r') as f:
        content = f.read()
    
    # Check for required header
    assert content.startswith("# Ingestion Log"), "❌ Missing '# Ingestion Log' header"
    
    # Check for separator after header
    assert "---" in content.split("\n")[:5], "❌ Missing separator after header"
    
    # Find all log entries (## date — title format)
    entries = re.findall(r'## (\d{4}-\d{2}-\d{2})', content)
    
    assert len(entries) > 0, "❌ No log entries found"
    
    # Validate each entry structure
    entry_sections = content.split("## ")
    for i, entry in enumerate(entry_sections[1:], 1):  # Skip header section
        # Check required fields
        required_fields = [
            "**Source:**",
            "**Run type:**",
            "**Action:**",
            "**Status:**",
            "### Files processed",
            "### Validation results",
            "### Notes"
        ]
        
        missing_fields = [field for field in required_fields if field not in entry]
        assert len(missing_fields) == 0, f"❌ Entry {i} missing fields: {missing_fields}"
        
        # Check that entry answers key questions
        questions_answered = {
            "when": bool(re.search(r'\d{4}-\d{2}-\d{2}', entry)),
            "source": "**Source:**" in entry,
            "action": "**Action:**" in entry,
            "status": "**Status:**" in entry,
            "evidence": "MD5 Hash" in entry or "hashes stored" in entry.lower(),
        }
        
        missing_answers = [q for q, answered in questions_answered.items() if not answered]
        assert len(missing_answers) == 0, f"Entry {i} doesn't answer: {missing_answers}"
        
        # Check for validation results table
        assert "| File |" in entry, f"Entry {i} missing validation table"
    
    # Verify append-only: entries should be in chronological order
    dates = [datetime.strptime(date, "%Y-%m-%d") for date in entries]
    assert dates == sorted(dates), "Entries not in chronological order (possible overwrite)"
    
    # Check that log references other metadata without duplicating
    assert "metadata/hashes/" in content or "`metadata/hashes/`" in content, "Should reference hash files"
    assert "metadata/row_counts/" in content or "`metadata/row_counts/`" in content, "Should reference row count files"
    
    print(f"\n Ingestion log structure validated")
    print(f"Found {len(entries)} log entry/entries")
    print(f"Log file: {ingestion_log}")
    print(f" All entries answer required questions")
    print(f"Log is append-only (chronological order verified)")
    print(f"references other metadata files correctly")


def test_compare_and_decide():
    """
    Test: Use compare_and_decide() to detect changes and make decision.
    This tests the core change detection logic that will be used by cron.
    """
    version = "v1_nbadataset_temp_data"
    project_root = get_project_root()
    
    try:
        # Run comparison
        decision, reason, comparison_details = compare_and_decide(
            project_root=project_root,
            version=version
        )
        
        # Validate decision is one of the three allowed values
        assert decision in ["ingest", "skip", "alert"], f"Invalid decision: {decision}"
        
        # Log the decision
        log_decision(
            decision=decision,
            reason=reason,
            comparison_details=comparison_details,
            project_root=project_root,
            version=version
        )
        
        print(f"\n🔍 Change Detection Results")
        print(f"Decision: {decision.upper()}")
        print(f"Reason: {reason}")
        print(f"Timestamp: {comparison_details.get('timestamp', 'N/A')}")
        print(f"Baseline: {comparison_details.get('baseline', False)}")
        print(f"Files compared: {len(comparison_details.get('files_compared', []))}")
        
        if comparison_details.get('files_compared'):
            print(f"\n📊 File Comparison Details:")
            for fc in comparison_details['files_compared']:
                print(f"  📄 {fc.get('file', 'N/A')}")
                print(f"     Hash match: {fc.get('hash_match', 'N/A')}")
                print(f"     Row match: {fc.get('row_match', 'N/A')}")
                print(f"     Status: {fc.get('status', 'N/A')}")
        
        if comparison_details.get('schema_issues'):
            print(f"\n⚠️  Schema Issues Detected:")
            for issue in comparison_details['schema_issues']:
                print(f"  File: {issue['file']}")
                print(f"    Previous schema: {issue['previous']}")
                print(f"    Current schema: {issue['current']}")
        
        print(f"\n✅ Decision logged to ingestion_log.md")
        print(f"💡 Decision: {decision.upper()} - {reason}")
        
        # Assert that we got a valid decision
        assert decision is not None, "❌ Decision was None"
        assert reason is not None, "❌ Reason was None"
        
    except Exception as e:
        assert False, f"❌ Error in compare_and_decide. Error: {e}"

