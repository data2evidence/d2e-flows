import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation
from dqd_plugin.types import dqdOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    # Install dqd R package from plugin
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"install.packages('./dqd_plugin/DataQualityDashboard-2.6.0', lib='{r_libs_user_directory}', repos = NULL, type='source')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, task_runner=SequentialTaskRunner, timeout_seconds=3600)
def dqd_plugin(options: dqdOptionsType):
    setup_plugin()
    # DQD has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    dqd_flow_module = importlib.import_module('flows.alp_dqd.flow')
    dqd_flow_module.execute_dqd_flow(options)