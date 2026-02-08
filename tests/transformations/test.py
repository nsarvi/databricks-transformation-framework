import sys
from pathlib import Path
import importlib

# Absolute path to the src and tests/src directories
project_root = Path(__file__).resolve().parents[3]  # Navigate to root folder (dna-common-all-databricks-templates-1)
src_path = project_root / "adm_integration_framework/src"
tests_src_path = project_root / "adm_integration_framework/tests/src"

# Append these to sys.path
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(tests_src_path))

print("Final sys.path:")
print("\n".join(sys.path))

try:
    # Try importing the module
    importlib.import_module("adm.custom.transformations")
    print("Successfully imported adm.custom.transformations")
except ModuleNotFoundError as e:
    print(f"Module not found: {e}")
