"""
DBT Operator for MWAA
Handles installation of dbt packages from S3 and execution of dbt commands.
"""

import os
import subprocess
import zipfile
import uuid
from typing import Optional, List, Dict, Any
from enum import Enum
import boto3
from airflow.models import BaseOperator


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    SILENT = 4


class DBTOperator(BaseOperator):
    """
    Airflow operator to install dbt-core and dbt-spark[PyHive] from S3 dependencies and execute dbt commands.

    Usage in DAG:
        dbt_task = DBTOperator(
            task_id='run_dbt_model',
            s3_bucket='aws-sg-nedp-dev-mwaa',
            s3_dependencies_prefix='dependencies/',
            s3_dbt_project_path='dbt_projects/dbt.zip',
            dbt_commands=[['debug'], ['run', '--select', 'hello_world']],
            log_level=LogLevel.INFO
        )
    """

    def __init__(
        self,
        task_id: str,
        s3_bucket: str,
        s3_dependencies_prefix: str,
        s3_dbt_project_path: str,
        dbt_commands: Optional[List[List[str]]] = None,
        temp_base_dir: str = '/tmp',
        log_level: LogLevel = LogLevel.INFO,
        **kwargs
    ):
        """
        Initialize the DBT Operator.

        Args:
            task_id: Airflow task identifier
            s3_bucket: S3 bucket name containing dependencies and dbt project
            s3_dependencies_prefix: S3 prefix for dependency files (wheels and tarballs)
            s3_dbt_project_path: S3 path to dbt project zip (e.g., 'dbt-projects/hlwdbt.zip')
            dbt_commands: List of dbt commands to execute (e.g., [['debug'], ['run']])
            temp_base_dir: Base directory for temporary files (default: /tmp)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, SILENT) (default: INFO)
            **kwargs: Additional arguments passed to BaseOperator
        """
        super().__init__(task_id=task_id, **kwargs)

        self.s3_bucket = s3_bucket
        self.s3_dependencies_prefix = s3_dependencies_prefix
        self.s3_dbt_project_path = s3_dbt_project_path
        self.dbt_commands = dbt_commands or [['debug']]
        self.temp_base_dir = temp_base_dir
        self.log_level = log_level

        # Generate unique directory for this operator instance
        self.unique_id = str(uuid.uuid4())[:8]
        self.temp_plugins_dir = os.path.join(temp_base_dir, f'dbt_plugins_{self.unique_id}')
        self.temp_project_dir = os.path.join(temp_base_dir, f'dbt_project_{self.unique_id}')
        self.temp_venv_dir = os.path.join(temp_base_dir, f'dbt_venv_{self.unique_id}')

        self._s3_client = None  # Lazy initialization
        self.initialized = False

        # Virtual environment paths
        self.venv_python = None
        self.venv_dbt = None

    @property
    def s3_client(self):
        """Lazy initialization of S3 client to avoid serialization issues."""
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        return self._s3_client

    def _log(self, message: str, level: LogLevel = LogLevel.INFO):
        """Log message using Airflow's logging system based on log level."""
        if level.value >= self.log_level.value:
            if level == LogLevel.DEBUG:
                self.log.debug(message)
            elif level == LogLevel.INFO:
                self.log.info(message)
            elif level == LogLevel.WARNING:
                self.log.warning(message)
            elif level == LogLevel.ERROR:
                self.log.error(message)

    def _log_debug(self, message: str):
        """Log debug message."""
        self._log(f"[DEBUG] {message}", LogLevel.DEBUG)

    def _log_info(self, message: str):
        """Log info message."""
        self._log(message, LogLevel.INFO)

    def _log_warning(self, message: str):
        """Log warning message."""
        self._log(f"⚠ {message}", LogLevel.WARNING)

    def _log_error(self, message: str):
        """Log error message."""
        self._log(f"✗ {message}", LogLevel.ERROR)

    def initialize(self) -> Dict[str, Any]:
        """
        Initialize the operator: download plugins from S3, install packages, and load dbt project.

        Returns:
            Dict with initialization status and details
        """
        if self.initialized:
            self._log_warning("DBT Operator already initialized")
            return {'status': 'already_initialized'}

        self._log_info("=" * 80)
        self._log_info(" " * 25 + "DBT OPERATOR INITIALIZATION")
        self._log_info("=" * 80)

        try:
            # Step 1: Create virtual environment
            self._log_info("\n[1/5] Creating virtual environment...")
            self._create_venv()
            self._log_info(f"✓ Virtual environment created at {self.temp_venv_dir}")

            # Step 2: Download dependencies from S3
            self._log_info("\n[2/5] Downloading dependencies from S3...")
            files_copied = self._download_dependencies()
            self._log_info(f"✓ Downloaded {files_copied} dependency files")

            # Step 3: Install build tools (setuptools, wheel)
            self._log_info("\n[3/5] Installing build tools...")
            self._install_build_tools()
            self._log_info("✓ Build tools installed")

            # Step 4: Install dbt packages (dbt-core and dbt-spark[PyHive])
            self._log_info("\n[4/5] Installing dbt-core and dbt-spark[PyHive]...")
            self._install_dbt_packages()
            self._log_info("✓ dbt packages installed")

            # Step 5: Download and extract dbt project
            self._log_info("\n[5/5] Loading dbt project from S3...")
            project_info = self._load_dbt_project()
            self._log_info(f"✓ dbt project loaded at {project_info['project_dir']}")

            self.initialized = True

            self._log_info("\n" + "=" * 80)
            self._log_info(" " * 20 + "✓ DBT OPERATOR INITIALIZED SUCCESSFULLY")
            self._log_info("=" * 80)

            return {
                'status': 'success',
                'files_copied': files_copied,
                'temp_plugins_dir': self.temp_plugins_dir,
                'project_dir': project_info['project_dir'],
                'profiles_dir': project_info['profiles_dir']
            }

        except Exception as e:
            self._log_error(f"Initialization failed: {str(e)}")
            raise

    def _create_venv(self):
        """Create a virtual environment for isolated package installation."""
        import venv

        self._log_debug(f"Creating virtual environment at {self.temp_venv_dir}")
        venv.create(self.temp_venv_dir, with_pip=True, clear=True)

        # Set paths to venv executables
        self.venv_python = os.path.join(self.temp_venv_dir, 'bin', 'python')
        self.venv_dbt = os.path.join(self.temp_venv_dir, 'bin', 'dbt')

        self._log_debug(f"Virtual environment Python: {self.venv_python}")
        self._log_debug(f"Virtual environment dbt: {self.venv_dbt}")

    def _download_dependencies(self) -> int:
        """Download dependency files from S3 to temp directory."""
        os.makedirs(self.temp_plugins_dir, exist_ok=True)
        self._log_debug(f"Created temp plugins directory: {self.temp_plugins_dir}")

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_dependencies_prefix)

        files_copied = 0
        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                s3_key = obj['Key']

                # Skip if it's just the prefix (directory)
                if s3_key == self.s3_dependencies_prefix:
                    continue

                filename = os.path.basename(s3_key)
                local_path = os.path.join(self.temp_plugins_dir, filename)

                self._log_debug(f"Downloading {filename}...")
                self.s3_client.download_file(self.s3_bucket, s3_key, local_path)
                files_copied += 1

        return files_copied

    def _install_build_tools(self):
        """Install setuptools and wheel from local files into venv."""
        # First upgrade pip itself to ensure compatibility with modern packages
        self._log_debug("Upgrading pip in venv...")
        pip_upgrade_result = subprocess.run(
            [
                self.venv_python, '-m', 'pip', 'install',
                '--upgrade',
                '--find-links', self.temp_plugins_dir,
                '--no-index',
                'pip'
            ],
            capture_output=True,
            text=True
        )

        if pip_upgrade_result.returncode != 0:
            self._log_error(f"Failed to upgrade pip: {pip_upgrade_result.stderr}")
            raise Exception(f"Failed to upgrade pip: {pip_upgrade_result.stderr}")

        self._log_debug("pip upgrade completed")

        # Now install setuptools and wheel
        self._log_debug("Running pip install for setuptools and wheel in venv...")
        result = subprocess.run(
            [
                self.venv_python, '-m', 'pip', 'install',
                '--find-links', self.temp_plugins_dir,
                '--no-index',
                'setuptools', 'wheel'
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            self._log_error(f"Failed to install build tools: {result.stderr}")
            raise Exception(f"Failed to install build tools: {result.stderr}")

        self._log_debug("Build tools installation completed")

    def _install_dbt_packages(self):
        """Install dbt-core and dbt-spark[PyHive] from local files into venv."""
        # Install dbt-core and dbt-spark with PyHive extras
        self._log_debug("Installing dbt-core and dbt-spark[PyHive] in venv...")
        result = subprocess.run(
            [
                self.venv_python, '-m', 'pip', 'install',
                '--find-links', self.temp_plugins_dir,
                '--no-index',
                'dbt-core==1.9.10',
                'dbt-spark[PyHive]==1.9.3'
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            self._log_error(f"Failed to install dbt packages: {result.stderr}")
            raise Exception(f"Failed to install dbt packages: {result.stderr}")

        self._log_debug("dbt packages installation completed")

    def _load_dbt_project(self) -> Dict[str, str]:
        """Download and extract dbt project from S3."""
        # Download zip file
        zip_path = os.path.join(self.temp_base_dir, f'dbt_project_{self.unique_id}.zip')
        self._log_debug(f"Downloading dbt project from s3://{self.s3_bucket}/{self.s3_dbt_project_path}")
        self.s3_client.download_file(self.s3_bucket, self.s3_dbt_project_path, zip_path)

        # Create project directory first
        project_name = os.path.splitext(os.path.basename(self.s3_dbt_project_path))[0]
        self.temp_project_dir = os.path.join(self.temp_base_dir, f'dbt_project_{self.unique_id}')
        os.makedirs(self.temp_project_dir, exist_ok=True)

        # Extract zip directly to project directory
        self._log_debug(f"Extracting zip to {self.temp_project_dir}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.temp_project_dir)

        # Set profiles directory
        profiles_dir = os.path.join(self.temp_project_dir, 'profiles')

        self._log_debug(f"dbt project directory: {self.temp_project_dir}")
        self._log_debug(f"Profiles directory: {profiles_dir}")

        return {
            'project_dir': self.temp_project_dir,
            'profiles_dir': profiles_dir
        }

    def run_dbt_command(
        self,
        dbt_args: List[str],
        capture_output: bool = True,
        print_output: bool = True
    ) -> Dict[str, Any]:
        """
        Run a dbt command with the specified arguments.

        Args:
            dbt_args: List of dbt command arguments (e.g., ['run', '--select', 'model_name'])
            capture_output: Whether to capture stdout/stderr
            print_output: Whether to print output to console

        Returns:
            Dict with command result, return code, stdout, and stderr

        Example:
            result = operator.run_dbt_command(['debug'])
            result = operator.run_dbt_command(['run', '--select', 'hello_world'])
            result = operator.run_dbt_command(['test'])
        """
        if not self.initialized:
            raise Exception("DBT Operator not initialized. Call initialize() first.")

        profiles_dir = os.path.join(self.temp_project_dir, 'profiles')

        # Build command using venv dbt executable
        cmd = [
            self.venv_dbt,
            *dbt_args,
            '--project-dir', self.temp_project_dir,
            '--profiles-dir', profiles_dir
        ]

        if print_output:
            self._log_info(f"\n{'=' * 60}")
            self._log_info(f"Running: {' '.join(cmd)}")
            self._log_info(f"{'=' * 60}")

        # Run command
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            cwd=self.temp_project_dir
        )

        # Print output if requested
        if print_output:
            if result.stdout:
                self._log_info(result.stdout)
            if result.stderr:
                self._log_warning(f"Errors/Warnings:\n{result.stderr}")

        # Determine success
        success = result.returncode == 0

        if print_output:
            if success:
                self._log_info(f"\n✓ SUCCESS (return code: {result.returncode})")
            else:
                self._log_error(f"\nFAILED (return code: {result.returncode})")
            self._log_info("=" * 60)

        return {
            'success': success,
            'return_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'command': ' '.join(cmd)
        }

    def cleanup(self):
        """Clean up temporary directories including virtual environment."""
        import shutil

        self._log_info("\nCleaning up temporary directories...")

        if os.path.exists(self.temp_venv_dir):
            shutil.rmtree(self.temp_venv_dir)
            self._log_info(f"✓ Removed virtual environment: {self.temp_venv_dir}")

        if os.path.exists(self.temp_plugins_dir):
            shutil.rmtree(self.temp_plugins_dir)
            self._log_info(f"✓ Removed {self.temp_plugins_dir}")

        if os.path.exists(self.temp_project_dir):
            shutil.rmtree(self.temp_project_dir)
            self._log_info(f"✓ Removed {self.temp_project_dir}")

        zip_path = os.path.join(self.temp_base_dir, f'dbt_project_{self.unique_id}.zip')
        if os.path.exists(zip_path):
            os.remove(zip_path)
            self._log_info(f"✓ Removed {zip_path}")

    def execute(self, context):
        """
        Execute the operator: initialize dbt environment, run commands, and cleanup.
        This is the main entry point called by Airflow when the task runs.

        Args:
            context: Airflow context dictionary

        Returns:
            Dict with execution results
        """
        try:
            # Initialize dbt environment
            init_result = self.initialize()

            # Run all dbt commands
            command_results = []
            for dbt_args in self.dbt_commands:
                result = self.run_dbt_command(dbt_args)
                command_results.append(result)

                # Stop on first failure
                if not result['success']:
                    self._log_error(f"Command failed: {' '.join(dbt_args)}")
                    raise Exception(f"dbt command failed: {' '.join(dbt_args)}")

            # Clean up
            self.cleanup()

            return {
                'status': 'success',
                'initialization': init_result,
                'command_results': command_results
            }

        except Exception as e:
            self._log_error(f"DBT Operator execution failed: {str(e)}")
            # Attempt cleanup even on failure
            try:
                self.cleanup()
            except Exception as cleanup_error:
                self._log_warning(f"Cleanup failed after execution error: {str(cleanup_error)}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()


# Example usage
if __name__ == "__main__":
    """
    Example usage of the DBT Operator.

    Usage in Airflow DAG (as native Airflow operator):

    from airflow import DAG
    from dbtoperator import DBTOperator, LogLevel
    from datetime import datetime

    with DAG(
        dag_id='dbt_example',
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False
    ) as dag:

        # Use DBTOperator directly in DAG definition
        dbt_task = DBTOperator(
            task_id='run_dbt_model',
            s3_bucket='aws-sg-nedp-dev-mwaa',
            s3_dependencies_prefix='dependencies/',
            s3_dbt_project_path='dbt_projects/dbt.zip',
            dbt_commands=[
                ['debug'],
                ['run', '--select', 'hello_world']
            ],
            log_level=LogLevel.INFO
        )

    # Advanced: Use as context manager in PythonOperator if needed:
    def run_dbt_with_context():
        with DBTOperator(
            task_id='manual_dbt',
            s3_bucket='aws-sg-nedp-dev-mwaa',
            s3_dependencies_prefix='dependencies/',
            s3_dbt_project_path='dbt_projects/dbt.zip'
        ) as operator:
            operator.run_dbt_command(['run'])
    """
    print("DBT Operator module loaded successfully")
    print(__doc__)
