"""
Pytest configuration for the test suite.
Adds the project root to the Python path for imports.
"""

import sys
from pathlib import Path

# Add project root to Python path so we can import api_to_csv, api_to_sql
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
