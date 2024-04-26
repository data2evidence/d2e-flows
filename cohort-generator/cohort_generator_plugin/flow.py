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
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CohortGenerator@0.8.1',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def cohort_generator_plugin(options: cohortGeneratorOptionsType):
    setup_plugin()
    # Cohort Gen has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    cohort_gen_flow_module = importlib.import_module('flows.cohort_generator.flow')
    cohort_gen_flow_module.execute_cohort_generator(options)