# The Cost of Virtuosity

This project uses data from the U.S. Department of Education's College Scorecard API to analyze key metrics for top music conservatories from 2012-2022.

## Featured Schools

- **Berklee College of Music**
- **The Juilliard School** 
- **New England Conservatory**
- **Manhattan School of Music**
- **Curtis Institute of Music**

## 📊 **[View Report →](https://scurrlin.github.io/cost_of_virtuosity/)**

![Dashboard Preview](https://public.tableau.com/static/images/Ac/AcceptanceDash/AcceptanceDash/1.png)

## Key Metrics Tracked

- Total Enrollment
- Admission Rate
- Retention Rate (Full-time, 4-year)
- Graduation Rate (150% normal time)
- Tuition & Fees
- Average Net Price

## Project Structure

```
college_scorecard_api/
├── api_to_csv.py        # Fetch data and export to CSV
├── api_to_sql.py        # Fetch data and store in SQLite database
├── requirements.txt     # Python dependencies
├── pytest.ini           # Pytest configuration
├── Dockerfile           # Containerized test execution
├── Makefile             # Development convenience commands
└── tests/
    ├── unit/
    │   ├── test_csv.py      # Unit tests for CSV export (25 tests)
    │   └── test_sql.py      # Unit tests for SQL database (25 tests)
    └── integration/
        ├── test_csv_integration.py   # Integration tests for CSV (2 tests)
        └── test_sql_integration.py   # Integration tests for SQL (2 tests)
```

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/scurrlin/cost_of_virtuosity.git
cd cost_of_virtuosity
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure API Key

Get a free API key from [api.data.gov](https://api.data.gov/signup), then create a `.env` file:

```
SCORECARD_API_KEY=your_key_here
```

### 4. Run Scripts

```bash
# Export to CSV
python api_to_csv.py

# Export to SQLite database
python api_to_sql.py
```

## Testing

The project includes a comprehensive test suite with 54 tests covering:

- **API interaction** - Mocked requests for offline, deterministic testing
- **Error handling** - Timeouts, connection errors, invalid responses
- **Data transformations** - Percentage conversion, edge cases
- **Database operations** - Schema creation, CRUD, upserts, views
- **Integration tests** - End-to-end `main()` workflow verification

### Run Tests Locally

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=. --cov-report=term-missing
```

### Run Tests in Docker

For reproducible execution in a controlled environment:

```bash
# Build and run tests in container
docker build -t scorecard-tests .
docker run --rm scorecard-tests
```

Or if you prefer using Make:

```bash
make test              # Run all tests
make test-unit         # Run unit tests only
make test-integration  # Run integration tests only
make test-cov          # Run with coverage
make docker-test       # Run tests in Docker
```

## Database Schema (api_to_sql.py)

The SQL script creates a normalized database with:

**Tables:**
- `schools` - Institution dimension table (unitid, name)
- `school_metrics` - Yearly metrics fact table

**Views:**
- `v_school_metrics` - All data with school names joined
- `v_metrics_yoy` - Year-over-year comparisons
- `v_school_summary` - Summary statistics per school

Example query:
```bash
sqlite3 -header -column music_schools_20260128.db "SELECT * FROM v_school_summary;"
```

## Dependencies

- `pandas` - Data manipulation
- `requests` - API calls
- `python-dotenv` - Environment variable management
- `pytest` - Testing framework
- `pytest-cov` - Test coverage

---

*Data Source: [College Scorecard](https://collegescorecard.ed.gov/) - U.S. Department of Education*
