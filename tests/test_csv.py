"""
Tests for api_to_csv.py

Tests are designed to run offline using mocked API responses.
Covers: fetch_year, normalize_percentages, build_filename
"""

import pandas as pd
import pytest
from datetime import datetime
from unittest.mock import patch, Mock

import api_to_csv as mod


# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


def make_response(payload, status_code=200):
    """Create a mock requests.Response object."""
    r = Mock()
    r.status_code = status_code
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


def make_api_result(unitid, year, school_name=None, **metrics):
    """
    Helper to create a single API result item.
    Defaults are provided for common metrics.
    """
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
    """Tests for the fetch_year function."""

    def test_happy_path_returns_dataframe_with_expected_columns(self, monkeypatch):
        """fetch_year returns a DataFrame with all expected columns on success."""
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

        # Check required columns exist
        expected_columns = {"institution", "unitid", "year"}
        assert expected_columns.issubset(df.columns)

        # Check metric columns exist
        for metric in mod.FIELD_MAP.keys():
            assert metric in df.columns

    def test_maps_institution_names_correctly(self, monkeypatch):
        """fetch_year uses NAME_MAP to get institution names."""
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

    def test_extracts_metric_values_correctly(self, monkeypatch):
        """fetch_year correctly extracts metric values from API response."""
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

    def test_skips_results_with_missing_id(self, monkeypatch):
        """fetch_year skips results that don't have an 'id' field."""
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

    def test_returns_empty_dataframe_on_empty_results(self, monkeypatch):
        """fetch_year returns empty DataFrame when API returns no results."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        payload = {"results": []}

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    def test_returns_empty_dataframe_on_request_timeout(self, monkeypatch):
        """fetch_year returns empty DataFrame on network timeout."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            import requests
            mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    def test_returns_empty_dataframe_on_connection_error(self, monkeypatch):
        """fetch_year returns empty DataFrame on connection error."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            import requests
            mock_get.side_effect = requests.exceptions.ConnectionError("Network unreachable")
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    def test_returns_empty_dataframe_on_http_error(self, monkeypatch):
        """fetch_year returns empty DataFrame on HTTP error (e.g., 500)."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            import requests
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
            mock_get.return_value = mock_response
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    def test_returns_empty_dataframe_on_invalid_json(self, monkeypatch):
        """fetch_year returns empty DataFrame when response isn't valid JSON."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        with patch("api_to_csv.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = mock_response
            df = mod.fetch_year([164748], 2020)

        assert df.empty

    def test_uses_fallback_name_for_unknown_unitid(self, monkeypatch):
        """fetch_year uses school.name from API when unitid not in NAME_MAP."""
        monkeypatch.setattr(mod, "API_KEY", "fake-key")

        year = 2020
        unknown_unitid = 999999
        payload = {
            "results": [
                {
                    "id": unknown_unitid,
                    "school.name": "Unknown Music Academy",
                    f"{year}.student.size": 500,
                }
            ]
        }

        with patch("api_to_csv.requests.get") as mock_get:
            mock_get.return_value = make_response(payload)
            df = mod.fetch_year([unknown_unitid], year)

        assert df["institution"].iloc[0] == "Unknown Music Academy"


# =============================================================================
# Tests for normalize_percentages()
# =============================================================================


class TestNormalizePercentages:
    """Tests for the normalize_percentages function."""

    def test_converts_decimals_to_percentages(self):
        """Values between 0-1 are converted to 0-100."""
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25, 0.1],
            "retention_rate_ft": [0.9, 0.85, 0.95],
            "grad_rate_150": [0.75, 0.6, 0.8],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [50.0, 25.0, 10.0]
        assert result["retention_rate_ft"].tolist() == [90.0, 85.0, 95.0]
        assert result["grad_rate_150"].tolist() == [75.0, 60.0, 80.0]

    def test_rounds_to_two_decimal_places(self):
        """Converted percentages are rounded to 2 decimal places."""
        df = pd.DataFrame({
            # Using values that don't hit exact midpoints to avoid banker's rounding edge cases
            "admission_rate": [0.12344, 0.98766, 0.33333],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [12.34, 98.77, 33.33]

    def test_handles_already_percentage_values(self):
        """Values already in percentage format (>1 or <0) are just rounded."""
        df = pd.DataFrame({
            "admission_rate": [50.123, 25.456, -1.0],  # Already percentages or negative
        })

        result = mod.normalize_percentages(df)

        # Should just round, not multiply by 100
        assert result["admission_rate"].tolist() == [50.12, 25.46, -1.0]

    def test_handles_none_and_nan_values(self):
        """None and NaN values are preserved as NaN."""
        df = pd.DataFrame({
            "admission_rate": [0.5, None, 0.3],
            "retention_rate_ft": [0.9, float("nan"), 0.8],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].iloc[0] == 50.0
        assert pd.isna(result["admission_rate"].iloc[1])
        assert result["admission_rate"].iloc[2] == 30.0

    def test_handles_string_values(self):
        """String values that can be converted are handled; others become NaN."""
        df = pd.DataFrame({
            "admission_rate": ["0.5", "bad", "0.3"],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].iloc[0] == 50.0
        assert pd.isna(result["admission_rate"].iloc[1])
        assert result["admission_rate"].iloc[2] == 30.0

    def test_handles_zero_and_one_boundary_values(self):
        """Boundary values 0 and 1 are handled correctly."""
        df = pd.DataFrame({
            "admission_rate": [0.0, 1.0, 0.5],
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [0.0, 100.0, 50.0]

    def test_does_not_modify_original_dataframe(self):
        """normalize_percentages returns a copy, not modifying the original."""
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25],
        })
        original_values = df["admission_rate"].tolist()

        mod.normalize_percentages(df)

        assert df["admission_rate"].tolist() == original_values

    def test_handles_missing_columns(self):
        """Columns not in DataFrame are silently skipped."""
        df = pd.DataFrame({
            "admission_rate": [0.5, 0.25],
            # Missing retention_rate_ft and grad_rate_150
        })

        result = mod.normalize_percentages(df)

        assert result["admission_rate"].tolist() == [50.0, 25.0]
        assert "retention_rate_ft" not in result.columns

    def test_custom_percentage_fields(self):
        """Custom percentage fields can be specified."""
        df = pd.DataFrame({
            "custom_rate": [0.5, 0.25],
            "admission_rate": [0.9, 0.8],
        })

        result = mod.normalize_percentages(df, percentage_fields=["custom_rate"])

        # Only custom_rate should be converted
        assert result["custom_rate"].tolist() == [50.0, 25.0]
        # admission_rate should be unchanged (not in custom fields)
        assert result["admission_rate"].tolist() == [0.9, 0.8]

    def test_handles_empty_dataframe(self):
        """Empty DataFrame is handled gracefully."""
        df = pd.DataFrame(columns=["admission_rate", "retention_rate_ft"])

        result = mod.normalize_percentages(df)

        assert result.empty
        assert "admission_rate" in result.columns


# =============================================================================
# Tests for build_filename()
# =============================================================================


class TestBuildFilename:
    """Tests for the build_filename function."""

    def test_generates_correct_format(self):
        """Filename follows expected format with year range and timestamp."""
        years = range(2012, 2022 + 1)
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2012_2022_20260128.csv"

    def test_handles_single_year(self):
        """Single year produces same min and max in filename."""
        years = [2020]
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2020_2020_20260128.csv"

    def test_handles_non_contiguous_years(self):
        """Non-contiguous years still use min and max."""
        years = [2012, 2015, 2020]
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_filename(years, now=fixed_time)

        assert result == "music_school_data_2012_2020_20260128.csv"

    def test_uses_current_time_when_not_provided(self):
        """Filename uses current datetime when now parameter is None."""
        years = range(2012, 2022 + 1)

        result = mod.build_filename(years)

        # Should contain today's date
        today = datetime.now().strftime("%Y%m%d")
        assert today in result
        assert result.startswith("music_school_data_2012_2022_")
        assert result.endswith(".csv")

    def test_different_dates_produce_different_filenames(self):
        """Different dates produce different filenames."""
        years = range(2012, 2022 + 1)
        date1 = datetime(2026, 1, 28, 12, 0, 0)
        date2 = datetime(2026, 2, 15, 12, 0, 0)

        result1 = mod.build_filename(years, now=date1)
        result2 = mod.build_filename(years, now=date2)

        assert result1 != result2
        assert "20260128" in result1
        assert "20260215" in result2
