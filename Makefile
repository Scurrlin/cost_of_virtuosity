# Makefile for common development tasks

.PHONY: test docker-test docker-build clean

# Run tests locally (fast, daily use)
test:
	pytest -v

# Run tests with coverage
test-cov:
	pytest --cov=. --cov-report=term-missing

# Build Docker image
docker-build:
	docker build -t scorecard-tests .

# Run tests in Docker (verification mode)
docker-test: docker-build
	docker run --rm scorecard-tests

# Clean up generated files
clean:
	rm -rf __pycache__ .pytest_cache
	rm -rf tests/__pycache__
	rm -f *.csv *.db
