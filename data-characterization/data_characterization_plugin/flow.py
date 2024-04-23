import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow
from prefect_shell import ShellOperation
from data_characterization_plugin.types import dcOptionsType


async def drop_data_characterization_schema(flow, flow_run, state):
    options = dcOptionsType(**flow_run.parameters['options'])
    
    try:
        dbsvc_module = importlib.import_module('d2e_dbsvc')
        await dbsvc_module.drop_data_characterization_schema(options)
    except Exception as e:
        raise e

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


@flow(log_prints=True, 
      persist_result=True,
      task_runner=SequentialTaskRunner,
      on_failure=[drop_data_characterization_schema],
      on_cancellation=[drop_data_characterization_schema]
      )
def data_characterization_plugin(options: dcOptionsType):
    setup_plugin()
    # DC flow file has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    dc_flow_module = importlib.import_module('flows.alp_data_characterization.flow')
    dc_flow_module.execute_data_characterization_flow(options)