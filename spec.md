# Technical Specification

This document explains how every component of the codebase works, from data flow to testing strategy.

---

## Table of Contents

1. [Overview](#overview)
2. [Data Flow](#data-flow)
3. [Core Scripts](#core-scripts)
   3.1 [api_to_csv.py](#api_to_csvpy)
   3.2 [api_to_sql.py](#api_to_sqlpy)
4. [Shared Functions](#shared-functions)
5. [Testing Strategy](#testing-strategy)
6. [Test Suite Breakdown](#test-suite-breakdown)
7. [Docker & Reproducibility](#docker--reproducibility)
8. [Configuration Files](#configuration-files)

---

## Overview

This project fetches educational metrics for 5 elite music conservatories from the U.S. Department of Education's College Scorecard API. It provides two output formats:

1. **CSV** - Flat file for spreadsheet analysis
2. **SQLite** - Normalized database with analytical views

Both scripts share identical logic for API interaction and data transformation, ensuring consistency.

---

## Data Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  College        │     │                  │     │  CSV File       │
│  Scorecard API  │────▶│  fetch_year()    │────▶│  (api_to_csv)   │
│                 │     │                  │     │                 │
└─────────────────┘     │  For each year   │     └─────────────────┘
                        │  2012-2022       │
                        │                  │     ┌─────────────────┐
                        │  ▼               │     │  SQLite DB      │
                        │  normalize_      │────▶│  (api_to_sql)   │
                        │  percentages()   │     │                 │
                        └──────────────────┘     └─────────────────┘
```

### Data Retrieved Per School Per Year

| Metric | API Field | Description |
|--------|-----------|-------------|
| `enrollment_total` | `student.size` | Total student enrollment |
| `admission_rate` | `admissions.admission_rate.overall` | Overall admission rate |
| `retention_rate_ft` | `student.retention_rate.four_year.full_time` | 4-year full-time retention |
| `grad_rate_150` | `completion.completion_rate_4yr_150nt` | Graduation rate at 150% normal time |
| `tuition_fees` | `cost.tuition.in_state` | Published tuition + required fees |
| `avg_net_price` | `cost.avg_net_price.private` | What families actually pay after aid |

---

## Core Scripts

### api_to_csv.py

**Purpose**: Fetch data and export to a timestamped CSV file.

**Entry Point**: `main()`

**Execution Flow**:

```python
def main():
    # 1. Iterate through each year (2012-2022)
    for y in YEARS:
        df = fetch_year(UNITIDS, y)  # Call API for all 5 schools
        if not df.empty:
            frames.append(df)
    
    # 2. Combine all years into one DataFrame
    long_df = pd.concat(frames, ignore_index=True)
    
    # 3. Convert decimal rates to percentages
    long_df = normalize_percentages(long_df)
    
    # 4. Sort and save
    sorted_df = long_df.sort_values(['institution', 'year'])
    filename = build_filename(YEARS)  # e.g., "music_school_data_2012_2022_20260128.csv"
    sorted_df.to_csv(filename, index=False)
```

**Output**: `music_school_data_2012_2022_YYYYMMDD.csv`

---

### api_to_sql.py

**Purpose**: Fetch data and store in a normalized SQLite database with analytical views.

**Entry Point**: `main()`

**Execution Flow**:

```python
def main():
    # 1. Create database schema
    db_path = build_db_filename()  # e.g., "music_schools_20260128.db"
    conn = create_database(db_path)
    
    # 2. Populate schools dimension table
    insert_schools(conn)
    
    # 3. Fetch and insert metrics for each year
    for year in YEARS:
        df = fetch_year(UNITIDS, year)
        if not df.empty:
            df = normalize_percentages(df)
            all_data.append(df)
    
    # 4. Bulk insert all metrics
    combined_df = pd.concat(all_data, ignore_index=True)
    insert_metrics(conn, combined_df)
```

**Database Schema**:

```sql
-- Dimension table: One row per school
CREATE TABLE schools (
    school_id INTEGER PRIMARY KEY AUTOINCREMENT,
    unitid INTEGER UNIQUE NOT NULL,      -- Federal ID
    institution_name TEXT NOT NULL
);

-- Fact table: One row per school per year
CREATE TABLE school_metrics (
    school_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    enrollment_total INTEGER,
    admission_rate REAL,
    retention_rate_ft REAL,
    grad_rate_150 REAL,
    tuition_fees REAL,
    avg_net_price REAL,
    PRIMARY KEY (school_id, year),
    FOREIGN KEY (school_id) REFERENCES schools(school_id)
);
```

**Views Created**:

| View | Purpose |
|------|---------|
| `v_school_metrics` | Joins schools and metrics for easy querying |
| `v_metrics_yoy` | Year-over-year changes using `LAG()` window function |
| `v_school_summary` | Aggregate statistics per school (avg, min, max, count) |

---

## Shared Functions

Both scripts use identical implementations of these functions:

### `fetch_year(institution_ids, year)`

Fetches data for all specified schools for a single year.

```python
def fetch_year(institution_ids, year):
    # Build API request with year-prefixed fields
    fields = ["id", "school.name"]
    fields += [f"{year}.{f}" for f in FIELD_MAP.values()]
    
    params = {
        "api_key": API_KEY,
        "id__in": ",".join(map(str, institution_ids)),
        "fields": ",".join(fields),
        "per_page": 100,
    }
    
    # res = response
    # ex = exception
    try:
        res = requests.get(API, params=params, timeout=30)
        res.raise_for_status()
        js = res.json()
    except requests.exceptions.RequestException as ex:
        return pd.DataFrame()  # Return empty on any error
    
    # Parse results into rows
    rows = []
    for item in js.get("results", []):
        row_id = item.get("id")
        if not row_id:
            continue  # Skip malformed results
        
        row = {
            "institution": NAME_MAP.get(row_id, item.get("school.name", "Unknown")),
            "unitid": row_id,
            "year": year,
        }
        for metric, field_suffix in FIELD_MAP.items():
            row[metric] = item.get(f"{year}.{field_suffix}")
        rows.append(row)
    
    return pd.DataFrame(rows)
```

**Key Design Decisions**:
- Returns empty DataFrame on any error (timeout, connection, HTTP, JSON parsing)
- Skips results without an `id` field
- Uses `NAME_MAP` for friendly names, falls back to API-provided name

---

### `normalize_percentages(df, percentage_fields=None)`

Converts decimal rates (0-1) to percentages (0-100).

```python
# c = column
# s = series
def normalize_percentages(df, percentage_fields=None):
    if percentage_fields is None:
        percentage_fields = ["admission_rate", "retention_rate_ft", "grad_rate_150"]
    
    out = df.copy()  # Never modify original
    
    for c in percentage_fields:
        if c in out.columns:
            s = pd.to_numeric(out[c], errors="coerce")  # Handle strings/bad data
            if s.notna().any():
                valid = s[s.notna()]
                if len(valid) > 0 and valid.max() <= 1.0 and valid.min() >= 0:
                    # Convert decimals to percentages
                    out[c] = (s * 100).round(2)
                else:
                    # Already in percentage format, just round
                    out[c] = s.round(2)
    
    return out
```

**Key Design Decisions**:
- Returns a copy, never mutates the input DataFrame
- Auto-detects whether data is decimal or percentage format
- Handles edge cases: strings, NaN, None, negative values
- Rounds to 2 decimal places

---

### `build_filename(years, now=None)` / `build_db_filename(now=None)`

Generates timestamped output filenames.

```python
def build_filename(years, now=None):
    if now is None:
        now = datetime.now()
    year_range = f"{min(years)}_{max(years)}"
    timestamp = now.strftime("%Y%m%d")
    return f"music_school_data_{year_range}_{timestamp}.csv"
```

**Key Design Decision**: Accepts optional `now` parameter for testability—allows tests to inject a fixed datetime instead of relying on system time.

---

## Testing Strategy

### Philosophy

1. **Offline Testing**: All API calls are mocked—tests never hit the real network
2. **Deterministic**: No randomness, no system time dependencies (injected via parameters)
3. **Fast**: All 54 tests complete in ~2 seconds
4. **Isolated**: Database tests use in-memory SQLite (`:memory:`), integration tests use temp directories

### Test Organization

```
tests/
├── __init__.py
├── conftest.py              # Path setup for imports
├── unit/
│   ├── __init__.py
│   ├── test_csv.py          # 25 unit tests for api_to_csv.py
│   └── test_sql.py          # 25 unit tests for api_to_sql.py
└── integration/
    ├── __init__.py
    ├── test_csv_integration.py   # 2 integration tests for api_to_csv.py
    └── test_sql_integration.py   # 2 integration tests for api_to_sql.py
```

**Why the split?**

- `unit/test_csv.py` tests shared functions (`fetch_year`, `normalize_percentages`) thoroughly
- `unit/test_sql.py` skips duplicated tests and focuses on database-specific logic
- `integration/` contains end-to-end `main()` workflow tests
- Separation allows running `pytest tests/unit/` for fast feedback or `pytest tests/integration/` for full verification
- Total: 54 tests with no redundancy

---

## Test Suite Breakdown

### test_csv.py (27 tests)

#### TestFetchYear (10 tests)

Tests API interaction with mocked `requests.get`:

| Test | What It Verifies |
|------|------------------|
| `test_happy_path_returns_dataframe_with_expected_columns` | Successful response returns correct DataFrame structure |
| `test_maps_institution_names_correctly` | NAME_MAP is used for friendly institution names |
| `test_extracts_metric_values_correctly` | All metric fields are correctly extracted |
| `test_skips_results_with_missing_id` | Malformed results (no `id`) are skipped |
| `test_returns_empty_dataframe_on_empty_results` | Empty API response → empty DataFrame |
| `test_returns_empty_dataframe_on_request_timeout` | Network timeout → empty DataFrame |
| `test_returns_empty_dataframe_on_connection_error` | Connection error → empty DataFrame |
| `test_returns_empty_dataframe_on_http_error` | HTTP 500 → empty DataFrame |
| `test_returns_empty_dataframe_on_invalid_json` | Bad JSON → empty DataFrame |
| `test_uses_fallback_name_for_unknown_unitid` | Unknown unitid uses API-provided name |

**Mocking Pattern**:

```python
def test_happy_path(self, monkeypatch):
    monkeypatch.setattr(mod, "API_KEY", "fake-key")  # Inject fake API key
    
    payload = {"results": [make_api_result(164748, 2020)]}
    
    with patch("api_to_csv.requests.get") as mock_get:
        mock_get.return_value = make_response(payload)
        df = mod.fetch_year([164748], 2020)
    
    assert not df.empty
```

#### TestNormalizePercentages (10 tests)

Tests data transformation logic:

| Test | What It Verifies |
|------|------------------|
| `test_converts_decimals_to_percentages` | 0.5 → 50.0 |
| `test_rounds_to_two_decimal_places` | 0.12344 → 12.34 |
| `test_handles_already_percentage_values` | 50.123 → 50.12 (not 5012.3) |
| `test_handles_none_and_nan_values` | NaN stays NaN |
| `test_handles_string_values` | "0.5" → 50.0, "bad" → NaN |
| `test_handles_zero_and_one_boundary_values` | 0.0 → 0.0, 1.0 → 100.0 |
| `test_does_not_modify_original_dataframe` | Input DataFrame unchanged |
| `test_handles_missing_columns` | Missing columns silently skipped |
| `test_custom_percentage_fields` | Can specify custom field list |
| `test_handles_empty_dataframe` | Empty input → empty output |

#### TestBuildFilename (5 tests)

Tests filename generation:

| Test | What It Verifies |
|------|------------------|
| `test_generates_correct_format` | Format matches `music_school_data_2012_2022_20260128.csv` |
| `test_handles_single_year` | Single year works: `2020_2020` |
| `test_handles_non_contiguous_years` | Uses min/max: [2012, 2020] → `2012_2020` |
| `test_uses_current_time_when_not_provided` | Default uses `datetime.now()` |
| `test_different_dates_produce_different_filenames` | Different dates → different filenames |

---

### test_sql.py (25 tests)

#### TestNormalizePercentages (1 sanity test)

Single test to verify the SQL script's copy works:
- Full coverage is in `test_csv.py` since the function is identical

#### TestBuildDbFilename (3 tests)

Same pattern as `build_filename` tests.

#### TestCreateDatabase (6 tests)

Tests database schema creation:

| Test | What It Verifies |
|------|------------------|
| `test_creates_schools_table` | Table exists |
| `test_creates_school_metrics_table` | Table exists |
| `test_creates_indexes` | `idx_metrics_year`, `idx_metrics_school` exist |
| `test_creates_views` | All 3 views exist |
| `test_schools_table_has_correct_columns` | `school_id`, `unitid`, `institution_name` |
| `test_school_metrics_table_has_correct_columns` | All metric columns present |

**Fixture Pattern**:

```python
@pytest.fixture
def in_memory_db():
    conn = mod.create_database(":memory:")  # In-memory for speed
    yield conn
    conn.close()
```

#### TestInsertSchools (4 tests)

| Test | What It Verifies |
|------|------------------|
| `test_inserts_all_schools_from_name_map` | All 5 schools inserted |
| `test_inserts_correct_unitids` | Federal IDs match NAME_MAP |
| `test_inserts_correct_institution_names` | Names match NAME_MAP |
| `test_is_idempotent` | Calling twice doesn't duplicate |

#### TestGetSchoolId (3 tests)

| Test | What It Verifies |
|------|------------------|
| `test_returns_correct_school_id` | Known unitid → integer school_id |
| `test_returns_none_for_unknown_unitid` | Unknown unitid → None |
| `test_returns_different_ids_for_different_schools` | Each school has unique ID |

#### TestInsertMetrics (6 tests)

| Test | What It Verifies |
|------|------------------|
| `test_inserts_metrics_data` | Data appears in table |
| `test_inserts_correct_values` | Values match input |
| `test_handles_multiple_years` | Multiple years for same school |
| `test_handles_multiple_schools` | Multiple schools for same year |
| `test_skips_unknown_unitids` | Unknown unitids silently skipped |
| `test_upserts_on_duplicate_key` | Re-insert updates existing row |

#### TestViews (2 integration tests)

| Test | What It Verifies |
|------|------------------|
| `test_v_school_metrics_joins_correctly` | JOIN produces expected output |
| `test_v_school_summary_calculates_averages` | Aggregations are correct |

#### TestMainIntegration (2 integration tests)

Integration tests verify the complete `main()` workflow:

| Test | What It Verifies |
|------|------------------|
| `test_main_creates_valid_database_with_correct_data` | Full pipeline: API → transform → database with tables, data, and working views |
| `test_main_handles_partial_api_failures` | Graceful degradation when some API calls fail |

---

## Docker & Reproducibility

### Philosophy

- **venv** for daily development (fast iteration)
- **Docker** for verification (reproducible execution)

### Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and tests
COPY api_to_csv.py api_to_sql.py ./
COPY pytest.ini ./
COPY tests/ tests/

# Run tests by default
CMD ["pytest", "-v"]
```

**Design Decisions**:
- `python:3.11-slim` - Minimal image, matches local dev
- Requirements copied first for Docker layer caching
- Only necessary files copied (see `.dockerignore`)
- Default command runs tests

### Usage

```bash
# Build image
docker build -t scorecard-tests .

# Run tests in container
docker run --rm scorecard-tests

# Or use Make
make docker-test
```

### What This Proves

When tests pass in Docker:
- No "works on my machine" issues
- Clean environment with no hidden dependencies
- Anyone can reproduce results with one command

---

## Configuration Files

### requirements.txt

```
pandas>=2.0.0
requests>=2.28.0
python-dotenv>=1.0.0
pytest>=7.4.0
pytest-cov>=4.1.0
```

### pytest.ini

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short
```

- `testpaths` - Only look in `tests/` directory
- `addopts` - Verbose output, short tracebacks by default

### Makefile

The Makefile provides standardized entry points for common tasks. Anyone can run `make test` or `make docker-test` without reading documentation—this is especially important for reproducible workflows when evaluating code or models across different environments.

```makefile
test:               # Run all tests
    pytest -v

test-unit:          # Run unit tests only (fast)
    pytest tests/unit/ -v

test-integration:   # Run integration tests only
    pytest tests/integration/ -v

test-cov:           # Run with coverage report
    pytest --cov=. --cov-report=term-missing

docker-build:       # Build Docker image
    docker build -t scorecard-tests .

docker-test:        # Build and run in container
    docker build -t scorecard-tests .
    docker run --rm scorecard-tests

clean:              # Clean up generated files
    rm -rf __pycache__ .pytest_cache
    find tests -type d -name __pycache__ -exec rm -rf {} +
    rm -f *.csv *.db
```

### .dockerignore

Excludes from Docker image:
- `.venv/` - Virtual environment (huge, not needed)
- `__pycache__/` - Python bytecode cache
- `.env` - Secrets (API key)
- `*.csv`, `*.db` - Output files
- `.git/` - Version control

---

## Summary

| Component | Purpose |
|-----------|---------|
| `api_to_csv.py` | Fetch → Transform → CSV |
| `api_to_sql.py` | Fetch → Transform → SQLite with views |
| `tests/test_csv.py` | 27 tests (25 unit + 2 integration) |
| `tests/test_sql.py` | 27 tests (25 unit + 2 integration) |
| `Dockerfile` | Reproducible test execution |
| `Makefile` | Developer convenience commands |

**Total**: 54 tests (50 unit + 4 integration), ~2 seconds, fully offline, fully deterministic.
