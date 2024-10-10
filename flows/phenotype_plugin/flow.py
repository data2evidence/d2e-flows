from rpy2 import robjects

from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.phenotype_plugin.types_new import PhenotypeOptionsType

from shared_utils.types import UserType
from shared_utils.dao.DBDao import DBDao
from shared_utils.api.AnalyticsSvcAPI import AnalyticsSvcAPI


def setup_plugin():
    r_libs_user_directory = Variable.get("r_libs_user").value
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CohortGenerator@0.11.1',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_github('OHDSI/PhenotypeLibrary@3.32.0',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"remotes::install_github('OHDSI/DatabaseConnector@6.3.2',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\"",
                f"Rscript -e \"install.packages('glue')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def phenotype_plugin(options: PhenotypeOptionsType):
    logger = get_run_logger()
    logger.info('Running Phenotype')
        
    # database_code = options.databaseCode
    # cdmschema_name = options.cdmschemaName
    # cohortschema_name = options.cohortschemaName
    # cohorttable_name = options.cohorttableName
    # cohorts_id = options.cohortsId

    database_code = 'alpdev_pg'
    cdmschema_name = "cdm_5pct_9a0f90a32250497d9483c981ef1e1e70"
    cohortschema_name = "cdm_5pct_zhimin"
    cohorttable_name = "cohorts_test4_phenotype"
    cohorts_id = '25,3,4'
    cohorts_id_str = f'as.integer(c({cohorts_id}))'

   
    use_cache_db = options.use_cache_db
    user = UserType.ADMIN_USER

    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code, 
                  schema_name=cdmschema_name)
    set_db_driver_env_string = dbdao.set_db_driver_env()
    set_connection_string = dbdao.get_database_connector_connection_string(
        user_type=user
    )
   
    logger.info(f'{set_connection_string}')
    r_libs_user_directory = Variable.get("r_libs_user").value
    logger.info('phenotype_donellla')