.PHONY: package-dbt sync-dags build_deps help

# Default target
help:
	@echo "Available targets:"
	@echo "  package-dbt    - Package dbt folder and upload to MWAA artifacts (s3://your-bucket/dbt_project/dbt.zip)"
	@echo "  sync-dags      - Sync all DAGs to MWAA S3 bucket (s3://aws-sg-nedp-dev-mwaa/dags/)"
	@echo "  build_deps     - Download dependencies (.whl and .tar.gz) using Python 3.12 and sync to MWAA S3 bucket (s3://aws-sg-nedp-dev-mwaa/dependencies/)"

# Package dbt folder and upload to MWAA artifacts
package-dbt:
	@echo "Packaging dbt folder..."
	@cd dbt && zip -r ../dbt.zip . -x "*.pyc" -x "__pycache__/*" -x "*.git/*" -x ".DS_Store"
	@echo "Uploading to MWAA artifacts..."
	@aws s3 cp dbt.zip s3://aws-sg-nedp-dev-mwaa/dbt_projects/dbt.zip
	@echo "Cleaning up local zip file..."
	@rm -f dbt.zip
	@echo "Done! dbt.zip uploaded to s3://aws-sg-nedp-dev-mwaa/dbt_project/dbt.zip"

# Sync all DAGs to MWAA S3 bucket
sync-dags:
	@echo "Syncing DAGs to MWAA..."
	@aws s3 sync dags/ s3://aws-sg-nedp-dev-mwaa/dags/ --delete
	@echo "Done! DAGs synced to s3://aws-sg-nedp-dev-mwaa/dags/"

# Download dependencies (.whl and .tar.gz) using Python 3.12 and sync to MWAA S3 bucket
build-deps:
	@echo "Creating temporary directory..."
	@mkdir -p /tmp/mwaa-deps
	@echo "Downloading dependencies (.whl and .tar.gz) from requirements.txt using Python 3.12..."
	@python3.12 -m pip download -r dependencies/requirements.txt -d /tmp/mwaa-deps
	@echo "Syncing dependencies to MWAA..."
	@aws s3 sync /tmp/mwaa-deps/ s3://aws-sg-nedp-dev-mwaa/dependencies/
	@echo "Done! Dependencies synced to s3://aws-sg-nedp-dev-mwaa/dependencies/"
