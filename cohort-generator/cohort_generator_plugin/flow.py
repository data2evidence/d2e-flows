import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow
from prefect_shell import ShellOperation
from cohort_generator_plugin.types import cohortGeneratorOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def cohort_generator_plugin(options: cohortGeneratorOptionsType):
    setup_plugin()
    # Cohort Gen has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    cohort_gen_flow_module = importlib.import_module('flows.cohort_generator.flow')
    cohort_gen_flow_module.execute_cohort_generator(options)