import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')



@flow(log_prints=True, task_runner=SequentialTaskRunner)
def dataflow_ui_plugin(json_graph, options):
    setup_plugin()
    dataflow_flow_module = importlib.import_module('flows.dataflow.flow')
    dataflow_flow_module.exec_flow(json_graph, options)