"""
Change Detection and Decision Logic for ETL Pipeline

This module implements compare_and_decide() which compares previous state
vs current state and returns a deterministic decision: "ingest", "skip", or "alert".

All paths are configurable.
"""

import os
import hashlib
import csv
from datetime import datetime
from pathlib import Path
from shutil import ExecError
from typing import Dict, Optional, Tuple


# CONFIGURABLE PATHS 

def get_project_root() -> Path:
    """
    Get project root directory.
    Assumes this file is in src/ directory, so goes up one level.
    """
    return Path(__file__).parent.parent


def get_data_path(project_root: Path, version: str = None) -> Path:
    """
    Get path to data directory. Version is optional for dynamic discovery.
    
    Args:
        project_root: Root directory of the project
        version: Optional version string (e.g., "v1_nbadataset_temp_data")
    
    Returns:
        Path to data directory
    """
    if version:
        return project_root / "data" / "temp" / version
    # If no version, return base temp directory (for discovery)
    return project_root / "data" / "temp"

def get_metadata_paths(project_root: Path) -> Dict[str, Path]:
    """
    Get all metadata directory paths.
    
    Args:
        project_root: Root directory of the project
    
    Returns:
        Dictionary with 'hashes', 'row_counts', and 'ingestion_log' paths
    """
    return {
        'hashes': project_root / "metadata" / "hashes",
        'row_counts': project_root / "metadata" / "row_counts",
        'ingestion_log': project_root / "metadata" / "ingestion_log.md"
    }

def sanitize_filename(filename: str) -> str:
    """Remove problematic Unicode from filenames."""
    if not isinstance(filename, str):
        return "invalid_filename"
    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-."
    sanitized = ''.join(c if c in safe_chars else '_' for c in filename)
    return sanitized[:100]



# READ PREVIOUS STATE 

def read_previous_hash(filename: str, metadata_paths: Dict[str, Path]) -> Optional[Tuple[str, str]]:
    """
    Read previous hash for a file.
    
    Args:
        filename: Name of the CSV file
        metadata_paths: Dictionary of metadata paths
    
    Returns:
        (date, hash) tuple or None if not found
    """
    # input validation first 
    if not isinstance(filename, str) or not filename.strip():
        return None
    if not isinstance(metadata_paths, dict) or 'hashes' not in metadata_paths:
        return None
    hashes_dir = metadata_paths['hashes']
    if not isinstance(hashes_dir, Path) or not hashes_dir.exists() or not hashes_dir.is_dir():
        return None

    safe_filename = sanitize_filename(filename)
    base_name = Path(safe_filename).stem
    hash_file = hashes_dir / f"{base_name}.md5"
    
    if not hash_file.exists():
        return None
    
    try:
         with open(hash_file, 'r') as f:
            line = f.read().strip()
            if not line:
                return None
            
            # NEW: Handle timestamp with spaces
            parts = line.split(maxsplit=2)  # Split on first 2 spaces only
            if len(parts) < 2:
                return None
            date_part = parts[0]  # "2026-01-25"
            hash_part = ' '.join(parts[1:])  # "13:32:04 56d17daae91b..."
            
            if len(hash_part) < 10:
                return None
            return (date_part, hash_part)
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, ValueError):    
        return None
    except Exception:
        return None

           


def read_previous_row_count(filename: str, metadata_paths: Dict[str, Path]) -> Optional[int]:
    """
    Read previous row count for a file (flexible parsing).
    """
    if not isinstance(filename, str) or not filename.strip():
        return None
    if not isinstance(metadata_paths, dict) or 'row_counts' not in metadata_paths:
        return None
    row_counts_dir = metadata_paths['row_counts']
    if not isinstance(row_counts_dir, Path) or not row_counts_dir.exists() or not row_counts_dir.is_dir():
        return None
    
    safe_filename = sanitize_filename(filename)
    base_name = Path(safe_filename).stem
    rows_file = row_counts_dir / f"{base_name}.rows"
    
    if not rows_file.exists():
        return None
    
    try:
        with open(rows_file, 'r') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
            
            # FLEXIBLE: Search ANY line for "Data rows:"
            for line in lines:
                if "Data rows:" in line:
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        data_rows = int(parts[1].strip())
                        if data_rows >= 0:
                            return data_rows
            return None
    except (FileNotFoundError, PermissionError, UnicodeDecodeError, ValueError):
        return None
    except Exception:
        return None


  

def read_previous_state(data_path: Path, metadata_paths: Dict[str, Path]) -> Dict[str, Dict]:
    """
    Read previous state for all files in data directory.
    
    Args:
        data_path: Path to directory containing CSV files
        metadata_paths: Dictionary of metadata paths
    
    Returns:
        {filename: {'hash': str, 'hash_date': str, 'row_count': int, 'schema': list}}
    """
    # 1. Input validation FIRST
    if not isinstance(data_path, Path) or not data_path.exists() or not data_path.is_dir():
        return {}
    if not isinstance(metadata_paths, dict):
        return {}
    
    previous_state = {}
    
    csv_files = [f for f in data_path.iterdir() 
                 if f.is_file() and f.suffix.lower() == '.csv' 
                 and not f.name.startswith('.')]
    
    for csv_file in csv_files:
        filename = csv_file.name
        
        # These calls now can't crash - hardened above
        hash_info = read_previous_hash(filename, metadata_paths)
        row_count = read_previous_row_count(filename, metadata_paths)
        
        # 2. Validate before storing
        if hash_info and len(hash_info) == 2 and hash_info[1]:
            previous_state[filename] = {
                'hash': hash_info[1],
                'hash_date': hash_info[0],
                'row_count': row_count if row_count is not None else None,
                'schema': None
            }
        else:
            # Even if no previous state, record the filename for audit trail
            previous_state[filename] = {
                'hash': None,
                'hash_date': None,
                'row_count': None,
                'schema': None
            }
    
    return previous_state


# COMPUTE CURRENT STATE 

def compute_file_hash(file_path: Path) -> str:
    """
    Compute MD5 hash for a file.
    
    Args:
        file_path: Path to the file
    
    Returns:
        MD5 hash as hex string or "ERROR" if computation failed
    """
    # 1. Input validation
    if not isinstance(file_path, Path) or not file_path.exists() or not file_path.is_file():
        return "ERROR_INVALID_FILE"
    
    # 2. Specific exception handling
    try:
        md5_hash = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)
        hash_result = md5_hash.hexdigest()
        # Validate hash format
        if len(hash_result) == 32 and all(c in '0123456789abcdef' for c in hash_result):
            return hash_result
        return "ERROR_INVALID_HASH"
    except (PermissionError, OSError, IOError) as e:
        return f"ERROR_READ_{type(e).__name__}"
    except Exception:
        return "ERROR_UNKNOWN"

def compute_file_row_count(file_path: Path) -> int:
    """
    Count rows in a CSV file (excluding header).
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        Number of data rows (excluding header) or 0 if failed
    """
    # 1. Input validation
    if not isinstance(file_path, Path) or not file_path.exists() or not file_path.is_file():
        return 0
    
    # 2. Specific exception handling + robust counting
    try:
        total_rows = 0
        with open(file_path, 'r') as f:
            total_rows = sum(1 for _ in f)
        data_rows = max(0, total_rows - 1)  # Exclude header, ensure non-negative
        return data_rows
    except (PermissionError,OSError, IOError) as e:
        return 0
    except Exception:
        return 0


def compute_file_schema(file_path: Path) -> list:
    """
    Extract column names from CSV file.
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        List of column names or empty list if failed
    """
    # 1. Input validation
    if not isinstance(file_path, Path) or not file_path.exists() or not file_path.is_file():
        return []
    
    # 2. Specific exception handling + robust CSV parsing
    try:
        with open(file_path, 'r') as f:
            # Try multiple CSV dialects/approaches
            reader = csv.reader(f)
            header = next(reader, [])
            if not header:
                return []
            return [col.strip() for col in header if col.strip()]
    except (StopIteration, PermissionError, OSError, csv.Error) as e:
        return []
    except Exception:
        return []





def compute_current_state(data_path: Path) -> Dict[str, Dict]:
    """
    Compute current state for all files in data directory.
    
    Args:
        data_path: Path to directory containing CSV files
    
    Returns:
        {filename: {'hash': str, 'row_count': int, 'schema': list}}
    """
    # input validation
    if not isinstance(data_path, Path) or not data_path.exists() or not data_path.is_dir():
        return []

    current_state = {}

    # robust file discovery 
    try:

        csv_files = [f for f in data_path.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.csv' 
                    and not f.name.startswith('.')]
    except (PermissionError, OSError):
        return {}

    # 3. Process each file safely
    for csv_file in csv_files:
        filename = csv_file.name
        current_state[filename] = {
            'hash': compute_file_hash(csv_file),
            'row_count': compute_file_row_count(csv_file),
            'schema': compute_file_schema(csv_file)
        }
    
    return current_state









# COMPARE AND DECIDE (Core Logic)
def compare_and_decide(project_root: Path, version: str = None, data_path: Path = None) -> Tuple[str, str, Dict]:
    """
    Compare previous state vs current state and return decision.
    
    Args:
        project_root: Root directory of the project
        version: Optional version string for data directory
        data_path: Optional direct path to data directory (overrides version)
    
    Returns:
        (decision, reason, comparison_details)
    """
    # 1. Input validation
    if not isinstance(project_root, Path) or not project_root.exists() or not project_root.is_dir():
        return ("alert", "Invalid project_root", {})
    
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_date}] Starting comparison for version: {version or 'unknown'}")
    
    # 2. Get paths safely
    try:
        if data_path is None:
            data_path = get_data_path(project_root, version)
        if not data_path.exists() or not data_path.is_dir():
            print(f"ERROR: data_path does not exist: {data_path}")
            return ("alert", f"Data path missing: {data_path}", {})
            
        metadata_paths = get_metadata_paths(project_root)
        print(f"Data path: {data_path}")
        print(f"Metadata paths: {metadata_paths}")
        
    except Exception as e:
        print(f"ERROR getting paths: {type(e).__name__}: {e}")
        return ("alert", f"Path setup failed: {str(e)}", {})
    
    # 3. Compute states safely
    try:
        print("Reading previous state...")
        previous_state = read_previous_state(data_path, metadata_paths)
        print(f"Previous state files: {len(previous_state)}")
        
        print("Computing current state...")
        current_state = compute_current_state(data_path)
        print(f"Current state files: {len(current_state)}")
        
    except Exception as e:
        print(f"ERROR computing states: {type(e).__name__}: {e}")
        return ("alert", f"State computation failed: {str(e)}", {})

    print("Computing current state...")
    current_state = compute_current_state(data_path)
    print(f"Current state files: {len(current_state)}")

    
    # 4. Initialize comparison details
    comparison_details = {
        'timestamp': current_date,
        'files_compared': [],
        'baseline': len(previous_state) == 0,
        'decision': None,
        'reason': None,
        'data_path': str(data_path),
        'version': version,
        'previous_files': list(previous_state.keys()),
        'current_files': list(current_state.keys()),
        'current_state': current_state.copy()
    }
    
    # 5. BASELINE CASE (no previous state)
    if len(previous_state) == 0:
        print("→ BASELINE RUN: No previous state found")
        comparison_details['decision'] = "ingest"
        comparison_details['reason'] = f"First known snapshot - baseline ingestion ({len(current_state)} files)"
        comparison_details['files_compared'] = [
            {
                'file': filename,
                'previous_hash': None,
                'current_hash': state['hash'],
                'previous_rows': None,
                'current_rows': state['row_count'],
                'hash_match': None,
                'row_match': None,
                'schema_match': None,
                'status': 'new_file'
            }
            for filename, state in current_state.items()
        ]
        print(f"DECISION: ingest (baseline)")
        return ("ingest", comparison_details['reason'], comparison_details)
    
    # 6. FILESET MISMATCH CHECKS (alert conditions)
    missing_files = set(previous_state.keys()) - set(current_state.keys())
    unexpected_files = set(current_state.keys()) - set(previous_state.keys())
    
    if missing_files:
        print(f"→ ALERT: Missing files: {missing_files}")
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Files missing from source: {', '.join(missing_files)}"
        comparison_details['files_compared'] = [{'file': f, 'status': 'missing_file'} for f in missing_files]
        return ("alert", comparison_details['reason'], comparison_details)
    
    if unexpected_files:
        print(f"→ ALERT: Unexpected files: {unexpected_files}")
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Unexpected new files: {', '.join(unexpected_files)}"
        comparison_details['files_compared'] = [{'file': f, 'status': 'unexpected_file'} for f in unexpected_files]
        return ("alert", comparison_details['reason'], comparison_details)
    
    # 7. DETAILED FILE COMPARISON
    print("→ Comparing files...")
    all_hashes_match = True
    all_rows_match = True
    schema_issues = []
    file_comparisons = []
    
    for filename in sorted(current_state.keys()):
        prev = previous_state.get(filename, {})
        curr = current_state[filename]
        
        # Safe comparisons
        hash_match = (prev.get('hash') == curr['hash']) if prev.get('hash') else False
        row_match = (prev.get('row_count') == curr['row_count']) if prev.get('row_count') is not None else False
        
        # Schema comparison (only if both exist)
        schema_match = True
        if prev.get('schema') and curr['schema']:
            schema_match = prev['schema'] == curr['schema']
            if not schema_match:
                schema_issues.append({
                    'file': filename,
                    'previous': prev['schema'],
                    'current': curr['schema']
                })
        
        file_comparison = {
            'file': filename,
            'previous_hash': prev.get('hash'),
            'current_hash': curr['hash'],
            'previous_rows': prev.get('row_count'),
            'current_rows': curr['row_count'],
            'hash_match': hash_match,
            'row_match': row_match,
            'schema_match': schema_match,
            'status': 'compared'
        }
        file_comparisons.append(file_comparison)
        
        if not hash_match:
            all_hashes_match = False
        if not row_match:
            all_rows_match = False
        
        # Console summary per file
        status = "✅" if hash_match else "❌"
        print(f"  {filename}: hash={status}, rows={curr['row_count']}")
    
    comparison_details['files_compared'] = file_comparisons
    comparison_details['schema_issues'] = schema_issues
    
    # 8. DECISION RULES (clear order, no repetition)
    print("\n→ Applying decision rules...")
    
    # ALERT: Schema changes
    if schema_issues:
        print(f"→ ALERT: Schema issues: {len(schema_issues)} files")
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Schema changes in {len(schema_issues)} file(s): {', '.join([s['file'] for s in schema_issues])}"
        return ("alert", comparison_details['reason'], comparison_details)
    
    # ALERT: Hash unchanged but rows changed (corruption)
    hash_unchanged_row_changed = any(
        fc['hash_match'] and not fc['row_match'] 
        for fc in file_comparisons
    )
    if hash_unchanged_row_changed:
        print("→ ALERT: Hash unchanged but rows changed (corruption)")
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = "Hash unchanged but row count changed - data corruption suspected"
        return ("alert", comparison_details['reason'], comparison_details)
    
    # SKIP: Perfect match
    if all_hashes_match and all_rows_match:
        print("→ SKIP: No changes detected")
        comparison_details['decision'] = "skip"
        comparison_details['reason'] = f"No changes detected - {len(file_comparisons)} files identical to baseline"
        return ("skip", comparison_details['reason'], comparison_details)
    
    # INGEST: Content changes detected
    changed_files = [fc['file'] for fc in file_comparisons if not fc['hash_match']]
    print(f"→ INGEST: {len(changed_files)} files changed")
    comparison_details['decision'] = "ingest"
    comparison_details['reason'] = f"Changes detected in {len(changed_files)} file(s): {', '.join(changed_files[:3])}{'...' if len(changed_files) > 3 else ''}"
    
    print(f"FINAL DECISION: {comparison_details['decision']} - {comparison_details['reason']}")
    return ("ingest", comparison_details['reason'], comparison_details)








# LOG DECISION (Non-Negotiable)
def log_decision(
    decision: str,
    reason: str,
    comparison_details: Dict,
    project_root: Path,
    version: str = None
):
    """
    Log the decision to ingestion_log.md (append-only).
    """
    # 1. Input validation
    if not isinstance(decision, str) or decision not in ["ingest", "skip", "alert"]:
        print(f"ERROR: Invalid decision '{decision}'")
        return
        
    if not isinstance(reason, str) or not reason.strip():
        print("ERROR: Empty reason")
        return
        
    if not isinstance(project_root, Path) or not project_root.exists() or not project_root.is_dir():
        print(f"ERROR: Invalid project_root: {project_root}")
        return
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Logging decision: {decision} for version {version or 'unknown'}")
    
    # 2. Get paths safely
    try:
        metadata_paths = get_metadata_paths(project_root)
        ingestion_log = metadata_paths['ingestion_log']
    except Exception as e:
        print(f"ERROR getting metadata paths: {type(e).__name__}: {e}")
        return
    
    # 3. Build comparison table safely
    try:
        files_compared = comparison_details.get('files_compared', [])
        comparison_table = "| File | Previous Hash | Current Hash | Hash Match | Previous Rows | Current Rows | Row Match |\n"
        comparison_table += "|------|---------------|--------------|------------|---------------|--------------|----------|\n"
        
        for fc in files_compared:
            prev_hash = fc.get('previous_hash', 'N/A')
            curr_hash = fc.get('current_hash', 'N/A')
            hash_match = "✅" if fc.get('hash_match') is True else "❌" if fc.get('hash_match') is False else "N/A"
            prev_rows = fc.get('previous_rows', 'N/A')
            curr_rows = fc.get('current_rows', 'N/A')
            row_match = "✅" if fc.get('row_match') is True else "❌" if fc.get('row_match') is False else "N/A"
            
            # Format hash display (first 12 chars + error handling)
            prev_hash_display = f"{str(prev_hash)[:12]}..." if prev_hash and prev_hash != 'N/A' and prev_hash != 'ERROR_INVALID_FILE' else 'N/A'
            curr_hash_display = f"{str(curr_hash)[:12]}..." if curr_hash and curr_hash != 'N/A' and curr_hash != 'ERROR_INVALID_FILE' else 'N/A'
            
            comparison_table += f"| {fc.get('file', 'N/A')} | {prev_hash_display} | {curr_hash_display} | {hash_match} | {prev_rows} | {curr_rows} | {row_match} |\n"
        
    except Exception as e:
        print(f"ERROR building table: {type(e).__name__}: {e}")
        comparison_table = "| File | Status |\n|------|--------|\n| Error building table | ❌ |\n"
    
    # 4. Status emoji
    status_emoji = {"ingest": "✅", "skip": "⏭️", "alert": "⚠️"}
    
    # 5. Build log entry with accurate baseline info
    baseline_status = "True (first snapshot)" if comparison_details.get('baseline', False) else "False"
    prev_state_info = "none (first run)" if comparison_details.get('baseline', False) else "read from metadata/"
    
    log_entry = f"""## {current_date} — Change Detection Run

**Source:** Kaggle – eoinamoore/historical-nba-data-and-player-box-scores  
**Run type:** Automated comparison  
**Dataset version:** {version or comparison_details.get('version', 'auto-detected') or 'unknown'}  
**Action:** {decision.upper()} - {reason}

### Comparison results
{comparison_table}

### Decision details
- **Decision:** {decision.upper()}
- **Reason:** {reason}
- **Timestamp:** {comparison_details.get('timestamp', 'N/A')}
- **Baseline:** {baseline_status}
- **Files compared:** {len(files_compared)}
- **Previous files:** {len(comparison_details.get('previous_files', []))}
- **Current files:** {len(comparison_details.get('current_files', []))}

### Notes
- Previous state: {prev_state_info}
- Current state computed from: {comparison_details.get('data_path', 'N/A')}
- Schema issues: {len(comparison_details.get('schema_issues', []))}

**Status:** {status_emoji.get(decision, '❓')} {decision.upper()}

---
"""
    
    # 6. Ensure log file exists with header (atomic write)
    try:
        ingestion_log.parent.mkdir(parents=True, exist_ok=True)
        
        if not ingestion_log.exists():
            with open(ingestion_log, 'w', encoding='utf-8') as f:
                f.write("# Ingestion Log\n\n---\n\n")
        
        # Append with newline separation
        with open(ingestion_log, 'a', encoding='utf-8') as f:
            f.write(log_entry)
        
        print(f"Log written to {ingestion_log}")
        
    except (PermissionError, OSError, UnicodeEncodeError) as e:
        print(f"ERROR writing log: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"UNEXPECTED ERROR writing log: {type(e).__name__}: {e}")


def save_current_state(data_path: Path, metadata_paths: Dict[str, Path], current_state: Dict[str, Dict]):
    """
    After "ingest" decision: Save hashes/rows to metadata folders.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Directories already exist ✓ - just write files
    hashes_dir = metadata_paths['hashes']
    row_counts_dir = metadata_paths['row_counts']
    
    print(f"Saving {len(current_state)} files to metadata...")
    
    saved_count = 0
    for filename, state in current_state.items():
        safe_filename = sanitize_filename(filename)
        base_name = Path(safe_filename).stem
        
        # 1. Save hash: "2026-01-25 12:19:09 56d17daae91b..."
        hash_file = hashes_dir / f"{base_name}.md5"
        try:
            with open(hash_file, 'w') as f:
                f.write(f"{timestamp.replace(' ', '_')} {state['hash']}")  # NEW
            saved_count += 1
        except Exception:
            print(f"Failed to save hash: {hash_file}")
        
        # 2. Save row count: "Total rows: 72630\nData rows: 72629"
        rows_file = row_counts_dir / f"{base_name}.rows"
        try:
            with open(rows_file, 'w') as f:
                f.write(f"Total rows: {state['row_count'] + 1}\n")
                f.write(f"Data rows: {state['row_count']}\n")
            saved_count += 1
        except Exception:
            print(f"Failed to save rows: {rows_file}")
    
    print(f"Saved {saved_count} metadata files to metadata/hashes/ and metadata/row_counts/")
