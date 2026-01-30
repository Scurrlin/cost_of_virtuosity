# Pytest configuration for test suite

import sys
from pathlib import Path

# Add project root to Python path
# so we can import api_to_csv.py
# & api_to_sql.py files

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
