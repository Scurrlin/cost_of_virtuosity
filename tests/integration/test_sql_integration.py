# =============================================================================
# Integration Tests for api_to_sql.py
# =============================================================================


import os
import sqlite3
import requests
from unittest.mock import patch, Mock

import api_to_sql as mod


# =============================================================================
# Helpers
# =============================================================================


# Create a mock requests.Response object
def make_response(payload, status_code=200):
    res = Mock()
    res.status_code = status_code
    res.json.return_value = payload
    res.raise_for_status.return_value = None
    return res


# Create a single API result item with default metrics
def make_api_result(unitid, year, **metrics):
    result = {
        "id": unitid,
        "school.name": mod.NAME_MAP.get(unitid, "Unknown School"),
    }
    defaults = {
        "student.size": 1000,
        "admissions.admission_rate.overall": 0.5,
        "student.retention_rate.four_year.full_time": 0.9,
        "completion.completion_rate_4yr_150nt": 0.75,
        "cost.tuition.in_state": 50000,
        "cost.avg_net_price.private": 30000,
    }
    for field, value in defaults.items():
        result[f"{year}.{field}"] = metrics.get(field, value)
    return result


# =============================================================================
# Integration Tests
# =============================================================================


class TestMainIntegration:

    # Integration Test 1: Full pipeline creates valid database with correct data
    def test_main_creates_valid_database_with_correct_data(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        
        # Use only 2 years for speed
        test_years = [2020, 2021]
        monkeypatch.setattr(mod, "YEARS", test_years)
        
        # Use only 2 schools for simplicity
        test_unitids = [164748, 192110]
        monkeypatch.setattr(mod, "UNITIDS", test_unitids)
        
        # Mock API responses
        def mock_get(url, params, timeout):
            year = None
            for field in params.get("fields", "").split(","):
                if field.startswith("20"):
                    year = int(field.split(".")[0])
                    break
            
            payload = {
                "results": [
                    make_api_result(164748, year, **{
                        "student.size": 1000 + (year - 2020) * 100,
                        "admissions.admission_rate.overall": 0.50,
                    }),
                    make_api_result(192110, year, **{
                        "student.size": 800 + (year - 2020) * 50,
                        "admissions.admission_rate.overall": 0.08,
                    }),
                ]
            }
            return make_response(payload)
        
        # Use temp directory for database
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            with patch("api_to_sql.requests.get", side_effect=mock_get):
                mod.main()
            
            # Find the created database
            db_files = list(tmp_path.glob("*.db"))
            assert len(db_files) == 1
            
            # Verify database content
            conn = sqlite3.connect(db_files[0])
            cursor = conn.cursor()
            
            # Check schools table
            cursor.execute("SELECT COUNT(*) FROM schools")
            assert cursor.fetchone()[0] == 5  # All 5 schools from NAME_MAP
            
            # Check metrics table (2 schools × 2 years = 4 rows)
            cursor.execute("SELECT COUNT(*) FROM school_metrics")
            assert cursor.fetchone()[0] == 4
            
            # Verify data transformation (decimals → percentages)
            cursor.execute("SELECT MAX(admission_rate) FROM school_metrics")
            max_rate = cursor.fetchone()[0]
            assert max_rate <= 100  # Converted to percentage
            
            # Verify views work
            cursor.execute("SELECT COUNT(*) FROM v_school_metrics")
            assert cursor.fetchone()[0] == 4
            
            cursor.execute("SELECT COUNT(*) FROM v_school_summary WHERE years_of_data > 0")
            assert cursor.fetchone()[0] == 2  # Only 2 schools have data
            
            conn.close()
            
        finally:
            os.chdir(original_cwd)

    # Integration Test 2: Handles partial API failures gracefully
    def test_main_handles_partial_api_failures(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        monkeypatch.setattr(mod, "YEARS", [2020, 2021, 2022])
        monkeypatch.setattr(mod, "UNITIDS", [164748])
        
        call_count = [0]
        
        def mock_get(url, params, timeout):
            call_count[0] += 1
            # Fail on second call (2021)
            if call_count[0] == 2:
                raise requests.exceptions.Timeout("Simulated timeout")
            
            year = None
            for field in params.get("fields", "").split(","):
                if field.startswith("20"):
                    year = int(field.split(".")[0])
                    break
            
            payload = {"results": [make_api_result(164748, year)]}
            return make_response(payload)
        
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            with patch("api_to_sql.requests.get", side_effect=mock_get):
                mod.main()
            
            db_files = list(tmp_path.glob("*.db"))
            conn = sqlite3.connect(db_files[0])
            cursor = conn.cursor()
            
            # Should have data for 2 years (2020 and 2022), not 3
            cursor.execute("SELECT COUNT(*) FROM school_metrics")
            assert cursor.fetchone()[0] == 2
            
            cursor.execute("SELECT year FROM school_metrics ORDER BY year")
            years = [row[0] for row in cursor.fetchall()]
            assert years == [2020, 2022]
            
            conn.close()
            
        finally:
            os.chdir(original_cwd)
