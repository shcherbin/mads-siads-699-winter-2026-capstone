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


# Download the source datasets from an S3 bucket
download-source-data:
	aws s3 sync s3://$AWS_S3_BUCKET/ notebooks/data


upload-source-data-dryrun:
	aws s3 sync --dryrun notebooks/data s3://$AWS_S3_BUCKET/


upload-source-data:
	@echo "\n\ndoing dry run of upload for confirmation of files to be uploaded"
	aws s3 sync --dryrun notebooks/data s3://$AWS_S3_BUCKET/ --exclude ".*" --exclude "*/.*"
	@just _confirm-upload

_confirm-upload:
	#!/bin/bash
	read -p "Do you want to proceed with upload? (y/N): " confirm
	if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
	    echo "syncing data to s3..."
	    aws s3 sync notebooks/data s3://$AWS_S3_BUCKET/ --exclude ".*" --exclude "*/.*"
	else
	    echo "exiting without upload"
	fi

compute-feature-dependency-count:
	python ./src/capstone/features/compute_dependency_count.py

compute-feature-compute_repo_age_and_staleness:
	python ./src/capstone/features/compute_repo_age_and_staleness.py

publish-feature-libraries-io:
	python ./src/capstone/features/publish_libaries_io_features.py

publish-feature-ossf-scorecard:
	python ./src/capstone/features/publish_ossf_scorecard.py

publish-feature-pypi-downloads:
	python ./src/capstone/features/publish_pypi_downloads.py
