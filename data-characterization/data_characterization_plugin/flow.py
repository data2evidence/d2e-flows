import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow
from prefect_shell import ShellOperation
from data_characterization_plugin.types import dcOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    # Install Achilles R package from plugin
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"install.packages('./data_characterization_plugin/Achilles-1.7.2', lib='{r_libs_user_directory}', repos = NULL, type='source')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_characterization_plugin(options: dcOptionsType):
    setup_plugin()
    # DC flow file has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    dc_flow_module = importlib.import_module('flows.alp_data_characterization.flow')
    dc_flow_module.execute_data_characterization_flow(options)