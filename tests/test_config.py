import pytest
import nbformat
from pathlib import Path

CONFIG_NOTEBOOK_PATH = Path("utils/config.ipynb")

# Load type 'code' exist the notebooks
def load_config():
    with CONFIG_NOTEBOOK_PATH.open("r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
        code_sources = [cell['source'] for cell in nb.cells if cell['cell_type'] == "code"]
        config_code = "\n".join(code_sources)
        config_globals = {}
        exec(config_code, config_globals)
        return config_globals

# Test workspace in each notebook
def test_catalog_equals_workspace():
    config = load_config()
    assert config["CATALOG"] == "workspace"

# Test the keys name exist in notebook
def test_tables_keys():
    config = load_config()
    expected_keys = {"crm_cust", "crm_prd", "crm_sales", "erp_cust", "erp_loc", "erp_cat", "audit_log"}
    assert set(config["TABLES"].keys()) == expected_keys

# Test path(workspace, schema, table) in each notebook
def test_tables_values_fully_qualified():
    config = load_config()
    for value in config["TABLES"].values():
        assert isinstance(value, str)
        parts = value.split(".")
        assert len(parts) == 3
        assert parts[0] == "workspace"