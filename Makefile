# Makefile for running tests

.PHONY: test test-unit test-integration test-cov docker-test docker-build clean

# Run all tests
test:
	pytest -v

# Run unit tests only (fast)
test-unit:
	pytest tests/unit/ -v

# Run integration tests only
test-integration:
	pytest tests/integration/ -v

# Run tests with coverage
test-cov:
	pytest --cov=. --cov-report=term-missing

# Build Docker image
docker-build:
	docker build -t scorecard-tests .

# Run tests in Docker
docker-test: docker-build
	docker run --rm scorecard-tests

# Clean up generated files
clean:
	rm -rf __pycache__ .pytest_cache
	find tests -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f *.csv *.db
