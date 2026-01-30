# =============================================================================
# Integration Tests for api_to_csv.py
# =============================================================================


import os
import pandas as pd
import requests
from unittest.mock import patch, Mock

import api_to_csv as mod


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

    # Integration Test 1: Full pipeline produces valid CSV with correct data
    def test_main_produces_valid_csv_with_correct_data(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        
        # Use only 2 years and 2 schools for simplicity
        test_years = [2020, 2021]
        monkeypatch.setattr(mod, "YEARS", test_years)
        
        test_unitids = [164748, 192110]
        monkeypatch.setattr(mod, "UNITIDS", test_unitids)
        
        # Mock API responses for each year
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
        
        # Change to temp directory for output
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            with patch("api_to_csv.requests.get", side_effect=mock_get):
                result_df = mod.main()
            
            # Verify DataFrame structure
            assert len(result_df) == 4  # 2 schools × 2 years
            assert set(result_df["institution"].unique()) == {
                "Berklee College of Music", 
                "The Juilliard School"
            }
            assert set(result_df["year"].unique()) == {2020, 2021}
            
            # Verify data was transformed (decimals → percentages)
            assert result_df["admission_rate"].max() <= 100
            assert result_df["admission_rate"].min() >= 0
            
            # Verify CSV file was created
            csv_files = list(tmp_path.glob("*.csv"))
            assert len(csv_files) == 1
            
            # Verify CSV content matches DataFrame
            saved_df = pd.read_csv(csv_files[0])
            assert len(saved_df) == len(result_df)
            assert set(saved_df.columns) == set(result_df.columns)
            
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
            with patch("api_to_csv.requests.get", side_effect=mock_get):
                result_df = mod.main()
            
            # Should have data for 2 years (2020 and 2022), not 3
            assert len(result_df) == 2
            assert set(result_df["year"].unique()) == {2020, 2022}
            
        finally:
            os.chdir(original_cwd)
