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
from typing import Dict, Optional, Tuple


# ============================================================================
# CONFIGURABLE PATHS (No Hardcoding)
# ============================================================================

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


# ============================================================================
# READ PREVIOUS STATE (Snapshot A - Memory)
# ============================================================================

def read_previous_hash(filename: str, metadata_paths: Dict[str, Path]) -> Optional[Tuple[str, str]]:
    """
    Read previous hash for a file.
    
    Args:
        filename: Name of the CSV file
        metadata_paths: Dictionary of metadata paths
    
    Returns:
        (date, hash) tuple or None if not found
    """
    hashes_dir = metadata_paths['hashes']
    base_name = Path(filename).stem
    hash_file = hashes_dir / f"{base_name}.md5"
    
    if not hash_file.exists():
        return None
    
    try:
        with open(hash_file, 'r') as f:
            line = f.read().strip()
            parts = line.split(' ', 2)
            if len(parts) >= 2:
                return (parts[0], parts[1])  # (date, hash)
    except Exception:
        return None
    return None


def read_previous_row_count(filename: str, metadata_paths: Dict[str, Path]) -> Optional[int]:
    """
    Read previous row count for a file.
    
    Args:
        filename: Name of the CSV file
        metadata_paths: Dictionary of metadata paths
    
    Returns:
        data_rows (excluding header) or None if not found
    """
    row_counts_dir = metadata_paths['row_counts']
    base_name = Path(filename).stem
    rows_file = row_counts_dir / f"{base_name}.rows"
    
    if not rows_file.exists():
        return None
    
    try:
        with open(rows_file, 'r') as f:
            lines = f.readlines()
            if len(lines) >= 3:
                # Line 2 is "Total rows: X", line 3 is "Data rows: Y"
                data_rows_line = lines[2].strip()
                if "Data rows:" in data_rows_line:
                    return int(data_rows_line.split(":")[1].strip())
    except Exception:
        return None
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
    previous_state = {}
    
    csv_files = [f for f in data_path.iterdir() 
                 if f.is_file() and f.suffix.lower() == '.csv' 
                 and not f.name.startswith('.')]
    
    for csv_file in csv_files:
        filename = csv_file.name
        hash_info = read_previous_hash(filename, metadata_paths)
        row_count = read_previous_row_count(filename, metadata_paths)
        
        previous_state[filename] = {
            'hash': hash_info[1] if hash_info else None,
            'hash_date': hash_info[0] if hash_info else None,
            'row_count': row_count,
            'schema': None  # Will be computed during current state if needed
        }
    
    return previous_state


# ============================================================================
# COMPUTE CURRENT STATE (Snapshot B - Observation)
# ============================================================================

def compute_file_hash(file_path: Path) -> str:
    """
    Compute MD5 hash for a file.
    
    Args:
        file_path: Path to the file
    
    Returns:
        MD5 hash as hex string
    """
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def compute_file_row_count(file_path: Path) -> int:
    """
    Count rows in a CSV file (excluding header).
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        Number of data rows (excluding header)
    """
    total_rows = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f)
    return total_rows - 1  # Exclude header


def compute_file_schema(file_path: Path) -> list:
    """
    Extract column names from CSV file.
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        List of column names
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            return [col.strip() for col in header]
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
    current_state = {}
    
    csv_files = [f for f in data_path.iterdir() 
                 if f.is_file() and f.suffix.lower() == '.csv' 
                 and not f.name.startswith('.')]
    
    for csv_file in csv_files:
        filename = csv_file.name
        current_state[filename] = {
            'hash': compute_file_hash(csv_file),
            'row_count': compute_file_row_count(csv_file),
            'schema': compute_file_schema(csv_file)
        }
    
    return current_state


# ============================================================================
# COMPARE AND DECIDE (Core Logic)
# ============================================================================

def compare_and_decide( project_root: Path, version: str = None, data_path: Path = None ) -> Tuple[str, str, Dict]:
    """
    Compare previous state vs current state and return decision.
    
    This is the core function that implements the three decision rules:
    - "skip": No changes detected
    - "ingest": Changes detected, safe to proceed
    - "alert": Unexpected condition, human attention required
    
    Args:
        project_root: Root directory of the project
        version: Optional version string for data directory
        data_path: Optional direct path to data directory (overrides version)
    
    Returns:
        (decision, reason, comparison_details)
        - decision: "ingest" | "skip" | "alert"
        - reason: Human-readable explanation
        - comparison_details: Dict with full comparison results
    """
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get paths
    if data_path is None:
        data_path = get_data_path(project_root, version)
    
    metadata_paths = get_metadata_paths(project_root)
    
    # Step 1: Read Previous State (Memory)
    previous_state = read_previous_state(data_path, metadata_paths)
    
    # Step 2: Compute Current State (Observation)
    current_state = compute_current_state(data_path)
    
    # Step 3: Compare and Decide
    comparison_details = {
        'timestamp': current_date,
        'files_compared': [],
        'baseline': len(previous_state) == 0,
        'decision': None,
        'reason': None,
        'data_path': str(data_path),
        'version': version
    }
    
    # If no previous state exists → baseline → ingest
    if len(previous_state) == 0:
        comparison_details['decision'] = "ingest"
        comparison_details['reason'] = "First known snapshot - baseline ingestion"
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
        return ("ingest", comparison_details['reason'], comparison_details)
    
    # Check for missing files (alert condition)
    missing_files = set(previous_state.keys()) - set(current_state.keys())
    if missing_files:
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Files missing from source: {', '.join(missing_files)}"
        comparison_details['files_compared'] = [{'file': f, 'status': 'missing'} for f in missing_files]
        return ("alert", comparison_details['reason'], comparison_details)
    
    # Check for unexpected new files (alert condition)
    unexpected_files = set(current_state.keys()) - set(previous_state.keys())
    if unexpected_files:
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Unexpected new files detected: {', '.join(unexpected_files)}"
        comparison_details['files_compared'] = [{'file': f, 'status': 'unexpected'} for f in unexpected_files]
        return ("alert", comparison_details['reason'], comparison_details)
    
    # Compare each file
    all_hashes_match = True
    all_rows_match = True
    schema_issues = []
    file_comparisons = []
    
    for filename in sorted(current_state.keys()):
        prev = previous_state[filename]
        curr = current_state[filename]
        
        hash_match = prev['hash'] == curr['hash'] if prev['hash'] else False
        row_match = prev['row_count'] == curr['row_count'] if prev['row_count'] is not None else False
        
        # Schema comparison
        schema_match = True
        if prev['schema'] is not None and curr['schema']:
            schema_match = prev['schema'] == curr['schema']
            if not schema_match:
                schema_issues.append({
                    'file': filename,
                    'previous': prev['schema'],
                    'current': curr['schema']
                })
        
        file_comparison = {
            'file': filename,
            'previous_hash': prev['hash'],
            'current_hash': curr['hash'],
            'previous_rows': prev['row_count'],
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
    
    comparison_details['files_compared'] = file_comparisons
    comparison_details['schema_issues'] = schema_issues
    
    # Decision Rules
    
    # Rule 3: Alert conditions
    if schema_issues:
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = f"Schema changes detected in {len(schema_issues)} file(s)"
        return ("alert", comparison_details['reason'], comparison_details)
    
    # Suspicious: hash unchanged but rows changed
    hash_unchanged_row_changed = any(
        fc['hash_match'] and not fc['row_match'] 
        for fc in file_comparisons
    )
    if hash_unchanged_row_changed:
        comparison_details['decision'] = "alert"
        comparison_details['reason'] = "Hash unchanged but row count changed - data corruption suspected"
        return ("alert", comparison_details['reason'], comparison_details)
    
    # Rule 1: Skip (nothing changed)
    if all_hashes_match and all_rows_match:
        comparison_details['decision'] = "skip"
        comparison_details['reason'] = "No changes detected - all hashes and row counts match"
        return ("skip", comparison_details['reason'], comparison_details)
    
    # Rule 2: Ingest (changes detected)
    if not all_hashes_match:
        changed_files = [fc['file'] for fc in file_comparisons if not fc['hash_match']]
        comparison_details['decision'] = "ingest"
        comparison_details['reason'] = f"Changes detected in {len(changed_files)} file(s): {', '.join(changed_files)}"
        return ("ingest", comparison_details['reason'], comparison_details)
    
    # In compare_and_decide, AFTER normal comparison but BEFORE final decision: 
    # Check if this is a perfect duplicate of previous baseline
    if not all_hashes_match and all_rows_match and len(schema_issues) == 0:
        comparison_details['decision'] = "skip"
        comparison_details['reason'] = "Perfect duplicate of baseline detected"
        return ("skip", comparison_details['reason'], comparison_details)

    
    # Fallback (shouldn't reach here)
    comparison_details['decision'] = "alert"
    comparison_details['reason'] = "Unexpected comparison state"
    return ("alert", comparison_details['reason'], comparison_details)


# ============================================================================
# LOG DECISION (Non-Negotiable)
# ============================================================================

def log_decision(
    decision: str,
    reason: str,
    comparison_details: Dict,
    project_root: Path,
    version: str = None
):
    """
    Log the decision to ingestion_log.md (append-only).
    
    Args:
        decision: "ingest", "skip", or "alert"
        reason: Human-readable explanation
        comparison_details: Full comparison results dictionary
        project_root: Root directory of the project
        version: Optional version string
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    metadata_paths = get_metadata_paths(project_root)
    ingestion_log = metadata_paths['ingestion_log']
    
    # Build comparison table
    comparison_table = "| File | Previous Hash | Current Hash | Hash Match | Previous Rows | Current Rows | Row Match |\n"
    comparison_table += "|------|---------------|--------------|------------|---------------|--------------|----------|\n"
    
    for fc in comparison_details.get('files_compared', []):
        prev_hash = fc.get('previous_hash', 'N/A')
        curr_hash = fc.get('current_hash', 'N/A')
        hash_match = "✅" if fc.get('hash_match') else "❌" if fc.get('hash_match') is False else "N/A"
        prev_rows = fc.get('previous_rows', 'N/A')
        curr_rows = fc.get('current_rows', 'N/A')
        row_match = "✅" if fc.get('row_match') else "❌" if fc.get('row_match') is False else "N/A"
        
        # Format hash display (first 12 chars)
        prev_hash_display = f"{prev_hash[:12]}..." if prev_hash and prev_hash != 'N/A' else 'N/A'
        curr_hash_display = f"{curr_hash[:12]}..." if curr_hash and curr_hash != 'N/A' else 'N/A'
        
        comparison_table += f"| {fc.get('file', 'N/A')} | {prev_hash_display} | {curr_hash_display} | {hash_match} | {prev_rows} | {curr_rows} | {row_match} |\n"
    
    # Status emoji
    status_emoji = {
        "ingest": "✅",
        "skip": "⏭️",
        "alert": "⚠️"
    }
    
    log_entry = f"""## {current_date} — Change Detection Run

**Source:** Kaggle – eoinamoore/historical-nba-data-and-player-box-scores  
**Run type:** Automated comparison  
**Dataset version:** {version or comparison_details.get('version', 'auto-detected')}  
**Action:** {decision.upper()} - {reason}

### Comparison results
{comparison_table}

### Decision details
- **Decision:** {decision.upper()}
- **Reason:** {reason}
- **Timestamp:** {comparison_details.get('timestamp', 'N/A')}
- **Baseline:** {comparison_details.get('baseline', False)}
- **Files compared:** {len(comparison_details.get('files_compared', []))}

### Notes
- Comparison performed automatically
- Previous state read from `metadata/hashes/` and `metadata/row_counts/`
- Current state computed from source files

**Status:** {status_emoji.get(decision, '❓')} {decision.upper()}

---
"""
    
    # Ensure log file exists with header
    if not ingestion_log.exists():
        ingestion_log.parent.mkdir(parents=True, exist_ok=True)
        with open(ingestion_log, 'w') as f:
            f.write("# Ingestion Log\n\n---\n\n")
    
    # Append to log (append-only)
    with open(ingestion_log, 'a') as f:
        f.write(log_entry)
