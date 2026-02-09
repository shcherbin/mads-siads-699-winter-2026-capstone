set dotenv-required
set dotenv-load

# Install Python prod and dev dependencies.
install-python-dependencies:
	uv sync --frozen

# Run Python tests
test:
	pytest --cov=./src --cov-report=term-missing --ruff --ruff-format ./tests


code-quality:
	ruff check src tests notebooks
