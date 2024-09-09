import os
import sys
import json
import importlib

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.serializers import JSONSerializer
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import RemoteFileSystem as RFS

from cohort_survival_plugin.types import CohortSurvivalOptionsType


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
                f"Rscript -e \"remotes::install_version('RPostgres', version='1.4.5', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            ]
        ).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def cohort_survival_plugin(options: CohortSurvivalOptionsType):
    setup_plugin()

    logger = get_run_logger()
    logger.info("Running Cohort Survival")

    dbdao_module = importlib.import_module('dao.DBDao')

    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db
    target_cohort_definition_id = options.targetCohortDefinitionId
    outcome_cohort_definition_id = options.outcomeCohortDefinitionId

    dbdao = dbdao_module.DBDao(use_cache_db=use_cache_db,
                               database_code=database_code,
                               schema_name=schema_name)

    generate_cohort_survival_data(
        dbdao,
        target_cohort_definition_id,
        outcome_cohort_definition_id
    )


@task(
    result_storage=RFS.load(
        os.getenv("DATAFLOW_MGMT__FLOWS__RESULTS_SB_NAME")),
    result_storage_key="{flow_run.id}_km.json",
    result_serializer=JSONSerializer(),
    persist_result=True,
)
def generate_cohort_survival_data(
    dbdao,
    target_cohort_definition_id: int,
    outcome_cohort_definition_id: int,
):
    filename = f"{dbdao.database_code}_{dbdao.schema_name}"
    r_libs_user_directory = os.getenv("R_LIBS_USER")

    # Get credentials for database code
    robjects = importlib.import_module('rpy2.robjects')
    db_credentials = dbdao.tenant_configs

    with robjects.conversion.localconverter(robjects.default_converter):
        result = robjects.r(
            f"""
            # Function to generate a random string of specified length
            .libPaths(c('{r_libs_user_directory}',.libPaths()))
            library(CDMConnector)
            library(CohortSurvival)
            library(dplyr)
            library(ggplot2)
            library(rjson)
            library(tools)
            library(RPostgres)
            # Run R console inside the dataflow agent container to run these code


            # VARIABLES
            target_cohort_definition_id <- {target_cohort_definition_id}
            outcome_cohort_definition_id <- {outcome_cohort_definition_id}
            pg_host <- "{db_credentials['host']}"
            pg_port <- "{db_credentials['port']}"
            pg_dbname <- "{db_credentials['databaseName']}"
            pg_user <- "{db_credentials['adminUser']}"
            pg_password <- "{db_credentials['adminPassword']}"
            pg_schema <- "{dbdao.schema_name}"

            con <- NULL
            tryCatch(
                {{ 
                    pg_con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = pg_dbname,
                        host = pg_host,
                        user = pg_user,
                        password = pg_password,
                        options=sprintf("-c search_path=%s", pg_schema))

                    # Begin transaction to run below 2 queries as is required for cohort survival but are not needed to be commited to database
                    DBI::dbBegin(pg_con)
                    # Remove these when cohorts functionality are improved
                    query <- sprintf("
                        UPDATE cohort
                        SET cohort_start_date = death_date, cohort_end_date = death.death_date
                        FROM death
                        WHERE subject_id = death.person_id
                        AND death_date IS NOT NULL
                        AND COHORT_DEFINITION_ID=%d", outcome_cohort_definition_id)
                    DBI::dbExecute(pg_con, query)

                    query <- sprintf("
                        UPDATE cohort
                        SET cohort_end_date = cohort_start_date
                        WHERE COHORT_DEFINITION_ID=%d", target_cohort_definition_id)
                        
                    DBI::dbExecute(pg_con, query)

                    # cdm_from_con is from CDMConnection
                    cdm <- CDMConnector::cdm_from_con(
                        con = pg_con,
                        write_schema = pg_schema,
                        cdm_schema = pg_schema,
                        cohort_tables = c("cohort"),
                        .soft_validation = TRUE
                    )

                    death_survival <- estimateSingleEventSurvival(cdm,
                        targetCohortId = target_cohort_definition_id,
                        outcomeCohortId = outcome_cohort_definition_id,
                        targetCohortTable = "cohort",
                        outcomeCohortTable = "cohort",
                        estimateGap = 30
                    )
                    
                    # Rollback queries done above after cohort survival is done
                    DBI::dbRollback(pg_con)

                    plot <- plotSurvival(death_survival)
                    plot_data <- ggplot_build(plot)$data[[1]]
                    # Convert data to a list if not already
                    plot_data <- as.list(plot_data)

                    # Add a key to the list
                    plot_data[["status"]] <- "SUCCESS"
                    plot_data_json <- toJSON(plot_data)

                    print(plot_data_json)
                    cdm_disconnect(cdm)
                    return(plot_data_json) }},
                error = function(e) {{ print(e)
                    data <- list(status = "ERROR", e$message)
                    return(toJSON(data)) }},
                finally = {{ if (!is.null(con)) {{ DBI::dbDisconnect(con)
                    print("Disconnected the database.") }}
                }}
            )
            """
        )
        # Parsing the json from R and returning to prevent double serialization
        # of the string
        result_dict = json.loads(str(result[0]))
        return result_dict
