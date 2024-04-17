import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow
from prefect_shell import ShellOperation
from r_cdm_plugin.types import rCDMOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    # Install CommonDataModel R package from plugin
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"install.packages('./r_cdm_plugin/CommonDataModel-5.4.1', lib='{r_libs_user_directory}', repos = NULL, type='source')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def r_cdm_plugin(options: rCDMOptionsType):
    setup_plugin()
    # CommonDataModel has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    omop_cdm_flow_module = importlib.import_module('flows.omop_cdm.flow')
    omop_cdm_flow_module.create_omop_cdm(options)