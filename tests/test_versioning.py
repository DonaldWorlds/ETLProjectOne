
from pathlib import Path
from etl_project_package.compare import get_data_path
from etl_project_package.main import next_local_version_name 


def test_next_local_version_name_first_run(tmp_path: Path):
    project_root = tmp_path
    version = next_local_version_name(project_root)
    assert version == "v1_nbadataset_temp_data"
    temp_root = get_data_path(project_root)
    assert temp_root.is_dir()


def test_next_local_version_name_increment(tmp_path: Path):
    project_root = tmp_path
    temp_root = get_data_path(project_root)
    temp_root.mkdir(parents=True, exist_ok=True)
    
    # Simulate v1 and v2 existing
    (temp_root / "v1_nbadataset_temp_data").mkdir()
    (temp_root / "v2_nbadataset_temp_data").mkdir()
    (temp_root / "other_stuff").mkdir()  # ignored
    
    version = next_local_version_name(project_root)
    assert version == "v3_nbadataset_temp_data"
