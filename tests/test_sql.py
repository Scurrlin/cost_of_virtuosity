"""
Tests for api_to_sql.py

Tests are designed to run offline using in-memory SQLite.
Covers: build_db_filename, database operations (create_database, insert_schools,
get_school_id, insert_metrics), and views.

Note: fetch_year and normalize_percentages are tested in test_csv.py since
both scripts share identical implementations of these functions.
"""

import pandas as pd
import pytest
import sqlite3
from datetime import datetime

import api_to_sql as mod


# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


@pytest.fixture
def in_memory_db():
    """Create an in-memory database with schema for testing."""
    conn = mod.create_database(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def db_with_schools(in_memory_db):
    """Database with schools already inserted."""
    mod.insert_schools(in_memory_db)
    return in_memory_db


# =============================================================================
# Tests for normalize_percentages() - sanity check only
# (Full test coverage is in test_csv.py since both scripts use identical logic)
# =============================================================================


class TestNormalizePercentages:
    """Sanity test for normalize_percentages - full coverage in test_csv.py."""

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


# =============================================================================
# Tests for build_db_filename()
# =============================================================================


class TestBuildDbFilename:
    """Tests for the build_db_filename function."""

    def test_generates_correct_format(self):
        """Filename follows expected format with timestamp."""
        fixed_time = datetime(2026, 1, 28, 12, 0, 0)

        result = mod.build_db_filename(now=fixed_time)

        assert result == "music_schools_20260128.db"

    def test_uses_current_time_when_not_provided(self):
        """Filename uses current datetime when now parameter is None."""
        result = mod.build_db_filename()

        today = datetime.now().strftime("%Y%m%d")
        assert today in result
        assert result.startswith("music_schools_")
        assert result.endswith(".db")

    def test_different_dates_produce_different_filenames(self):
        """Different dates produce different filenames."""
        date1 = datetime(2026, 1, 28, 12, 0, 0)
        date2 = datetime(2026, 2, 15, 12, 0, 0)

        result1 = mod.build_db_filename(now=date1)
        result2 = mod.build_db_filename(now=date2)

        assert result1 != result2
        assert "20260128" in result1
        assert "20260215" in result2


# =============================================================================
# Tests for create_database()
# =============================================================================


class TestCreateDatabase:
    """Tests for the create_database function."""

    def test_creates_schools_table(self, in_memory_db):
        """create_database creates the schools table with correct schema."""
        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schools'")
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == "schools"

    def test_creates_school_metrics_table(self, in_memory_db):
        """create_database creates the school_metrics table."""
        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='school_metrics'")
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == "school_metrics"

    def test_creates_indexes(self, in_memory_db):
        """create_database creates the expected indexes."""
        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]

        assert "idx_metrics_year" in indexes
        assert "idx_metrics_school" in indexes

    def test_creates_views(self, in_memory_db):
        """create_database creates the expected views."""
        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
        views = [row[0] for row in cursor.fetchall()]

        assert "v_school_metrics" in views
        assert "v_metrics_yoy" in views
        assert "v_school_summary" in views

    def test_schools_table_has_correct_columns(self, in_memory_db):
        """schools table has the expected columns."""
        cursor = in_memory_db.cursor()
        cursor.execute("PRAGMA table_info(schools)")
        columns = {row[1] for row in cursor.fetchall()}

        assert "school_id" in columns
        assert "unitid" in columns
        assert "institution_name" in columns

    def test_school_metrics_table_has_correct_columns(self, in_memory_db):
        """school_metrics table has the expected columns."""
        cursor = in_memory_db.cursor()
        cursor.execute("PRAGMA table_info(school_metrics)")
        columns = {row[1] for row in cursor.fetchall()}

        expected = {
            "school_id", "year", "enrollment_total", "admission_rate",
            "retention_rate_ft", "grad_rate_150", "tuition_fees", "avg_net_price"
        }
        assert expected.issubset(columns)


# =============================================================================
# Tests for insert_schools()
# =============================================================================


class TestInsertSchools:
    """Tests for the insert_schools function."""

    def test_inserts_all_schools_from_name_map(self, in_memory_db):
        """insert_schools inserts all schools from NAME_MAP."""
        mod.insert_schools(in_memory_db)

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM schools")
        count = cursor.fetchone()[0]

        assert count == len(mod.NAME_MAP)

    def test_inserts_correct_unitids(self, in_memory_db):
        """insert_schools inserts the correct unitids."""
        mod.insert_schools(in_memory_db)

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT unitid FROM schools")
        unitids = {row[0] for row in cursor.fetchall()}

        assert unitids == set(mod.NAME_MAP.keys())

    def test_inserts_correct_institution_names(self, in_memory_db):
        """insert_schools inserts the correct institution names."""
        mod.insert_schools(in_memory_db)

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT unitid, institution_name FROM schools")
        rows = cursor.fetchall()

        for unitid, name in rows:
            assert mod.NAME_MAP[unitid] == name

    def test_is_idempotent(self, in_memory_db):
        """insert_schools can be called multiple times without duplicating data."""
        mod.insert_schools(in_memory_db)
        mod.insert_schools(in_memory_db)  # Call again

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM schools")
        count = cursor.fetchone()[0]

        assert count == len(mod.NAME_MAP)


# =============================================================================
# Tests for get_school_id()
# =============================================================================


class TestGetSchoolId:
    """Tests for the get_school_id function."""

    def test_returns_correct_school_id(self, db_with_schools):
        """get_school_id returns the correct school_id for a known unitid."""
        # Get Berklee's school_id
        school_id = mod.get_school_id(db_with_schools, 164748)

        assert school_id is not None
        assert isinstance(school_id, int)

    def test_returns_none_for_unknown_unitid(self, db_with_schools):
        """get_school_id returns None for an unknown unitid."""
        school_id = mod.get_school_id(db_with_schools, 999999)

        assert school_id is None

    def test_returns_different_ids_for_different_schools(self, db_with_schools):
        """Different schools have different school_ids."""
        berklee_id = mod.get_school_id(db_with_schools, 164748)
        juilliard_id = mod.get_school_id(db_with_schools, 192110)

        assert berklee_id != juilliard_id


# =============================================================================
# Tests for insert_metrics()
# =============================================================================


class TestInsertMetrics:
    """Tests for the insert_metrics function."""

    def test_inserts_metrics_data(self, db_with_schools):
        """insert_metrics inserts data into school_metrics table."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music"],
            "unitid": [164748],
            "year": [2020],
            "enrollment_total": [1000],
            "admission_rate": [50.0],
            "retention_rate_ft": [90.0],
            "grad_rate_150": [75.0],
            "tuition_fees": [50000],
            "avg_net_price": [30000],
        })

        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT COUNT(*) FROM school_metrics")
        count = cursor.fetchone()[0]

        assert count == 1

    def test_inserts_correct_values(self, db_with_schools):
        """insert_metrics inserts the correct metric values."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music"],
            "unitid": [164748],
            "year": [2020],
            "enrollment_total": [1234],
            "admission_rate": [42.0],
            "retention_rate_ft": [90.0],
            "grad_rate_150": [75.0],
            "tuition_fees": [50000],
            "avg_net_price": [30000],
        })

        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("""
            SELECT enrollment_total, admission_rate, tuition_fees 
            FROM school_metrics WHERE year = 2020
        """)
        row = cursor.fetchone()

        assert row[0] == 1234
        assert row[1] == 42.0
        assert row[2] == 50000

    def test_handles_multiple_years(self, db_with_schools):
        """insert_metrics can insert data for multiple years."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music", "Berklee College of Music"],
            "unitid": [164748, 164748],
            "year": [2019, 2020],
            "enrollment_total": [900, 1000],
            "admission_rate": [45.0, 50.0],
            "retention_rate_ft": [88.0, 90.0],
            "grad_rate_150": [70.0, 75.0],
            "tuition_fees": [48000, 50000],
            "avg_net_price": [28000, 30000],
        })

        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT COUNT(*) FROM school_metrics")
        count = cursor.fetchone()[0]

        assert count == 2

    def test_handles_multiple_schools(self, db_with_schools):
        """insert_metrics can insert data for multiple schools."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music", "The Juilliard School"],
            "unitid": [164748, 192110],
            "year": [2020, 2020],
            "enrollment_total": [1000, 800],
            "admission_rate": [50.0, 8.0],
            "retention_rate_ft": [90.0, 95.0],
            "grad_rate_150": [75.0, 80.0],
            "tuition_fees": [50000, 55000],
            "avg_net_price": [30000, 25000],
        })

        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT COUNT(*) FROM school_metrics")
        count = cursor.fetchone()[0]

        assert count == 2

    def test_skips_unknown_unitids(self, db_with_schools):
        """insert_metrics skips rows with unitids not in the schools table."""
        df = pd.DataFrame({
            "institution": ["Unknown School", "Berklee College of Music"],
            "unitid": [999999, 164748],
            "year": [2020, 2020],
            "enrollment_total": [500, 1000],
            "admission_rate": [30.0, 50.0],
            "retention_rate_ft": [80.0, 90.0],
            "grad_rate_150": [60.0, 75.0],
            "tuition_fees": [40000, 50000],
            "avg_net_price": [20000, 30000],
        })

        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT COUNT(*) FROM school_metrics")
        count = cursor.fetchone()[0]

        # Only Berklee should be inserted
        assert count == 1

    def test_upserts_on_duplicate_key(self, db_with_schools):
        """insert_metrics updates existing rows on duplicate (school_id, year)."""
        df1 = pd.DataFrame({
            "institution": ["Berklee College of Music"],
            "unitid": [164748],
            "year": [2020],
            "enrollment_total": [1000],
            "admission_rate": [50.0],
            "retention_rate_ft": [90.0],
            "grad_rate_150": [75.0],
            "tuition_fees": [50000],
            "avg_net_price": [30000],
        })

        df2 = pd.DataFrame({
            "institution": ["Berklee College of Music"],
            "unitid": [164748],
            "year": [2020],
            "enrollment_total": [1100],  # Updated value
            "admission_rate": [52.0],    # Updated value
            "retention_rate_ft": [91.0],
            "grad_rate_150": [76.0],
            "tuition_fees": [51000],
            "avg_net_price": [31000],
        })

        mod.insert_metrics(db_with_schools, df1)
        mod.insert_metrics(db_with_schools, df2)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT COUNT(*) FROM school_metrics")
        count = cursor.fetchone()[0]
        assert count == 1  # Still only one row

        cursor.execute("SELECT enrollment_total, admission_rate FROM school_metrics WHERE year = 2020")
        row = cursor.fetchone()
        assert row[0] == 1100  # Updated value
        assert row[1] == 52.0  # Updated value


# =============================================================================
# Tests for views (integration tests)
# =============================================================================


class TestViews:
    """Integration tests for database views."""

    def test_v_school_metrics_joins_correctly(self, db_with_schools):
        """v_school_metrics view correctly joins schools and metrics."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music"],
            "unitid": [164748],
            "year": [2020],
            "enrollment_total": [1000],
            "admission_rate": [50.0],
            "retention_rate_ft": [90.0],
            "grad_rate_150": [75.0],
            "tuition_fees": [50000],
            "avg_net_price": [30000],
        })
        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("SELECT institution_name, year, enrollment_total FROM v_school_metrics")
        row = cursor.fetchone()

        assert row[0] == "Berklee College of Music"
        assert row[1] == 2020
        assert row[2] == 1000

    def test_v_school_summary_calculates_averages(self, db_with_schools):
        """v_school_summary view correctly calculates summary statistics."""
        df = pd.DataFrame({
            "institution": ["Berklee College of Music", "Berklee College of Music"],
            "unitid": [164748, 164748],
            "year": [2019, 2020],
            "enrollment_total": [900, 1100],
            "admission_rate": [48.0, 52.0],
            "retention_rate_ft": [88.0, 92.0],
            "grad_rate_150": [70.0, 80.0],
            "tuition_fees": [48000, 52000],
            "avg_net_price": [28000, 32000],
        })
        mod.insert_metrics(db_with_schools, df)

        cursor = db_with_schools.cursor()
        cursor.execute("""
            SELECT institution_name, years_of_data, avg_enrollment, avg_admission_rate 
            FROM v_school_summary 
            WHERE unitid = 164748
        """)
        row = cursor.fetchone()

        assert row[0] == "Berklee College of Music"
        assert row[1] == 2  # years_of_data
        assert row[2] == 1000  # avg_enrollment (900 + 1100) / 2
        assert row[3] == 50.0  # avg_admission_rate (48 + 52) / 2
