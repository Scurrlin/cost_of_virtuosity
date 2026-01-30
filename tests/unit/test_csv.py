# =============================================================================
# Unit Tests for api_to_csv.py
# =============================================================================

# Tests are designed to run offline using mocked API responses
# Covers fetch_year(), normalize_percentages(), and build_filename()

import pandas as pd
import pytest
import requests
from datetime import datetime
from unittest.mock import patch, Mock

import api_to_csv as mod


# =============================================================================
# Fixtures and Helpers
# =============================================================================

# Create a mock requests.Response object
def make_response(payload, status_code=200):
    res = Mock()
    res.status_code = status_code
    res.json.return_value = payload  # Return the payload as JSON
    res.raise_for_status.return_value = None  # No exception raised
    return res

# Helper to create a single API result item
# Defaults are provided for common metrics
def make_api_result(unitid, year, school_name=None, **metrics):
    if school_name is None:
        school_name = mod.NAME_MAP.get(unitid, "Unknown School")

    result = {
        "id": unitid,
        "school.name": school_name,
    }

    # Default metrics
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
# Tests for fetch_year()
# =============================================================================


class TestFetchYear:

    # Test 1 (Happy path): Returns a DataFrame with all expected columns on success
    def test_happy_path_returns_dataframe_with_expected_columns(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        payload = {
            "results": [
                make_api_result(164748, year),
                make_api_result(192110, year),
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748, 192110], year)

        # Verify DataFrame structure
        assert not df.empty
        assert len(df) == 2

        # Check for required columns
        expected_columns = {"institution", "unitid", "year"}
        assert expected_columns.issubset(df.columns)

        # Check for metric columns
        for metric in mod.FIELD_MAP.keys():
            assert metric in df.columns

    # Test 2 (Happy path): Maps institution names correctly
    def test_maps_institution_names_correctly(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        payload = {
            "results": [
                make_api_result(164748, year),
                make_api_result(192110, year),
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748, 192110], year)

        berklee_row = df[df["unitid"] == 164748].iloc[0]
        juilliard_row = df[df["unitid"] == 192110].iloc[0]

        assert berklee_row["institution"] == "Berklee College of Music"
        assert juilliard_row["institution"] == "The Juilliard School"

    # Test 3: Extracts metric values correctly
    def test_extracts_metric_values_correctly(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        payload = {
            "results": [
                {
                    "id": 164748,
                    "school.name": "Berklee College of Music",
                    f"{year}.student.size": 1234,
                    f"{year}.admissions.admission_rate.overall": 0.42,
                    f"{year}.student.retention_rate.four_year.full_time": 0.9,
                    f"{year}.completion.completion_rate_4yr_150nt": 0.75,
                    f"{year}.cost.tuition.in_state": 50000,
                    f"{year}.cost.avg_net_price.private": 30000,
                }
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748], year)

        row = df.iloc[0]
        assert row["enrollment_total"] == 1234
        assert row["admission_rate"] == 0.42
        assert row["retention_rate_ft"] == 0.9
        assert row["grad_rate_150"] == 0.75
        assert row["tuition_fees"] == 50000
        assert row["avg_net_price"] == 30000

    # Test 4 (Edge case): Skips results with missing 'id' field
    def test_skips_results_with_missing_id(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        payload = {
            "results": [
                {"school.name": "No ID School"},  # Missing id
                make_api_result(164748, year),
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748], year)

        assert len(df) == 1
        assert df["unitid"].iloc[0] == 164748

    # Test 5: Returns empty DataFrame when API returns no results
    def test_returns_empty_dataframe_on_empty_results(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        payload = {"results": []}

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    # Test 6 (Error path): Returns empty DataFrame on network timeout
    def test_returns_empty_dataframe_on_request_timeout(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    # Test 7: Returns empty DataFrame on connection error
    def test_returns_empty_dataframe_on_connection_error(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError("Network unreachable")
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    # Test 8 (Error path): Returns empty DataFrame on HTTP error
    def test_returns_empty_dataframe_on_http_error(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
            mock_get.return_value = mock_response
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    # Test 9: Returns empty DataFrame on invalid JSON
    def test_returns_empty_dataframe_on_invalid_json(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = mock_response
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    # Test 10 (Edge case): Uses fallback name for unknown unitid
    def test_uses_fallback_name_for_unknown_unitid(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        unknown_unitid = 999999
        payload = {
            "results": [
                {
                    "id": unknown_unitid,
                    "school.name": "Unknown School",
                    f"{year}.student.size": 500,
                }
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([unknown_unitid], year)

        assert df["institution"].iloc[0] == "Unknown School"


# =============================================================================
# Tests for normalize_percentages()
# =============================================================================


class TestNormalizePercentages:

    # Test 11 (Happy path): Converts decimals to percentages
    def test_converts_decimals_to_percentages(self):
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25, 0.1],
            "retention_rate_ft": [0.9, 0.85, 0.95],
            "grad_rate_150": [0.75, 0.6, 0.8],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [50.0, 25.0, 10.0]
        assert result["retention_rate_ft"].tolist() == [90.0, 85.0, 95.0]
        assert result["grad_rate_150"].tolist() == [75.0, 60.0, 80.0]

    # Test 12 (Edge case): Rounds to two decimal places

    # Note: Values don't hit exact midpoints
    # to avoid banker's rounding edge cases
    def test_rounds_to_two_decimal_places(self):
        df = pd.DataFrame({
            "admission_rate": [0.12344, 0.98766, 0.33333],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [12.34, 98.77, 33.33]

    # Test 13: Handles already percentage values
    def test_handles_already_percentage_values(self):
        df = pd.DataFrame({
            "admission_rate": [50.123, 25.456, -1.0],  # Already percentages or negative
        })

        result = mod.normalize_percentages(df)

        # Should just round, not multiply by 100
        assert result["admission_rate"].tolist() == [50.12, 25.46, -1.0]

    # Test 14 (Edge case): Handles None and NaN values
    def test_handles_none_and_nan_values(self):
        df = pd.DataFrame({
            "admission_rate": [0.5, None, 0.3],
            "retention_rate_ft": [0.9, float("nan"), 0.8],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].iloc[0] == 50.0
        assert pd.isna(result["admission_rate"].iloc[1])
        assert result["admission_rate"].iloc[2] == 30.0

    # Test 15: Handles string values
    def test_handles_string_values(self):
        df = pd.DataFrame({
            "admission_rate": ["0.5", "bad", "0.3"],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].iloc[0] == 50.0
        assert pd.isna(result["admission_rate"].iloc[1])
        assert result["admission_rate"].iloc[2] == 30.0

    # Test 16 (Edge case): Handles zero and one boundary values
    def test_handles_zero_and_one_boundary_values(self):
        df = pd.DataFrame({
            "admission_rate": [0.0, 1.0, 0.5],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [0.0, 100.0, 50.0]

    # Test 17: Does not modify original dataframe
    def test_does_not_modify_original_dataframe(self):
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25],
        })
        original_values = df["admission_rate"].tolist()

        mod.normalize_percentages(df)

        assert df["admission_rate"].tolist() == original_values

    # Test 18 (Edge case): Handles missing columns
    def test_handles_missing_columns(self):
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25],
            # Missing retention_rate_ft and grad_rate_150
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [50.0, 25.0]
        assert "retention_rate_ft" not in result.columns

    # Test 19: Handles custom percentage fields
    def test_custom_percentage_fields(self):
        df = pd.DataFrame({
            "custom_rate": [0.5, 0.25],
            "admission_rate": [0.9, 0.8],
        })

        result = mod.normalize_percentages(df, percentage_fields=["custom_rate"])

        # Only custom_rate should be converted
        assert result["custom_rate"].tolist() == [50.0, 25.0]
        # admission_rate should be unchanged (not in custom fields)
        assert result["admission_rate"].tolist() == [0.9, 0.8]

    # Test 20 (Edge case): Handles empty dataframe
    def test_handles_empty_dataframe(self):
        df = pd.DataFrame(columns=["admission_rate", "retention_rate_ft"])

        result = mod.normalize_percentages(df)

        assert result.empty
        assert "admission_rate" in result.columns


# =============================================================================
# Tests for build_filename()
# =============================================================================


class TestBuildFilename:

    # Test 21: Generates correct format
    def test_generates_correct_format(self):
        years = range(2012, 2022 + 1)
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2012_2022_20260128.csv"

    # Test 22 (Edge case): Handles single year
    def test_handles_single_year(self):
        years = [2020]
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2020_2020_20260128.csv"

    # Test 23: Handles non-contiguous years
    def test_handles_non_contiguous_years(self):
        years = [2012, 2015, 2020]
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2012_2020_20260128.csv"

    # Test 24 (Edge case): Uses current time when now parameter is None
    def test_uses_current_time_when_not_provided(self):
        years = range(2012, 2022 + 1)

        result = mod.build_filename(years)

        # Should contain today's date
        today = datetime.now().strftime("%Y%m%d")
        assert today in result
        assert result.startswith("music_school_data_2012_2022_")
        assert result.endswith(".csv")

    # Test 25 (Edge case): Different dates produce different filenames
    def test_different_dates_produce_different_filenames(self):
        years = range(2012, 2022 + 1)
        date1 = datetime(2026, 1, 28, 12, 0, 0)
        date2 = datetime(2026, 2, 15, 12, 0, 0)

        result1 = mod.build_filename(years, now=date1)
        result2 = mod.build_filename(years, now=date2)

        assert result1 != result2
        assert "20260128" in result1
        assert "20260215" in result2
