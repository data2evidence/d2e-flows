import os
import sys
import importlib
from prefect.task_runners import SequentialTaskRunner
from prefect import flow
from prefect_shell import ShellOperation
from cohort_survival_plugin.types import cohortSurvivalOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append("/app/pysrc")
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    if r_libs_user_directory:
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_version('snakecase', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('broom', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('visOmopResults', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('dbplyr', version='2.4.0', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('rjson', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('omopgenerics', version='0.2.1', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('CDMConnector', version='1.4.0', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('PatientProfiles', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('CohortSurvival', version='0.5.1', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('CohortSurvival', version='0.5.1', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_version('RPostgres', version='1.4.5', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            ]
        ).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def cohort_survival_plugin(options: cohortSurvivalOptionsType):
    setup_plugin()
    # Cohort Gen has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    cohort_survival_flow_module = importlib.import_module("flows.cohort_survival.flow")
    cohort_survival_flow_module.execute_cohort_survival(options)
