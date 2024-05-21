import sys
import importlib
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from create_duckdb_file_plugin.config import CreateDuckdbDatabaseFileType, CreateDuckdbDatabaseFileModules
from create_duckdb_file_plugin.create import create_duckdb_database_file


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def create_duckdb_file_plugin(options: CreateDuckdbDatabaseFileType):
    setup_plugin()
    modules = CreateDuckdbDatabaseFileModules(
        utils_types=importlib.import_module('utils.types'),
        alpconnection_dbutils=importlib.import_module('alpconnection.dbutils'),
        dao_DBDao=importlib.import_module('dao.DBDao'),
    )
    create_duckdb_database_file(options, modules)
