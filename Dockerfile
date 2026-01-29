# Dockerfile for running tests in a reproducible environment
# Usage:
#   docker build -t scorecard-tests .
#   docker run --rm scorecard-tests

FROM python:3.11-slim

WORKDIR /app

# Copy only requirements first (better layer caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and tests
COPY api_to_csv.py api_to_sql.py ./
COPY pytest.ini ./
COPY tests/ tests/

# Run tests by default
CMD ["pytest", "-v"]
