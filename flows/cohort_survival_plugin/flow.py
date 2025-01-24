import json
from rpy2 import robjects

from prefect import flow, task
from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect.logging import get_run_logger
from prefect.serializers import JSONSerializer
from prefect.filesystems import RemoteFileSystem as RFS

from flows.cohort_survival_plugin.types import CohortSurvivalOptionsType

from shared_utils.dao.DBDao import DBDao


def prepend_libpaths(commands, r_libs_user_directory):
    """
    Prepend the .libPaths() update to each Rscript command in the list and print skipped commands.

    Args:
        commands (list): List of Rscript commands as strings.
        r_libs_user_directory (str): The custom library path to prepend.

    Returns:
        list: Updated commands with .libPaths() prepended.
    """
    libpaths_prefix = f".libPaths(c('{r_libs_user_directory}', .libPaths())); "
    updated_commands = []

    for cmd in commands:
        # Extract package name from the command
        package_start = cmd.find("'") + 1
        package_end = cmd.find("'", package_start)
        package_name = cmd[package_start:package_end]

        # Extract version if specified in the command
        version_start = cmd.find("version=")
        if version_start != -1:
            version_start += len("version='")
            version_end = cmd.find("'", version_start)
            desired_version = cmd[version_start:version_end]
        else:
            desired_version = None

        # Extract the original install logic (everything after "Rscript -e ")
        install_logic = cmd.split("Rscript -e ")[1].strip('"')

        # Check if "force=TRUE" is in the command
        force_install = "force=TRUE" in cmd

        # Construct the updated command with version check
        if desired_version:
            version_different = f"(\'{desired_version}\' != as.character(packageVersion(\'{package_name}\')))"
        else:
            version_different = "FALSE"  # No version check if version is not specified

        # Combine all checks: requireNamespace, version, and force
        updated_cmd = (
            f'Rscript -e "{libpaths_prefix}'
            f'if (!requireNamespace(\'{package_name}\') || ({version_different} && {"TRUE" if force_install else "FALSE"})) {{ {install_logic} }} '
            f'else {{ message(\'Skipping installation for {package_name}, already installed and matches the required version.\') }}"'
        )

        updated_commands.append(updated_cmd)
    
    return updated_commands




@task
def setup_plugin():
    r_libs_user_directory = Variable.get("r_libs_user")
    if r_libs_user_directory:
        commands = [
            f"Rscript -e \"remotes::install_version('snakecase', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('broom', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('dbplyr', version='2.4.0', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('rjson', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('CDMConnector', version='1.4.0', quiet=TRUE, upgrade='never', force=FALSE, dependencies=NA, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('PatientProfiles', version='1.0.0', quiet=TRUE, upgrade='never', force=FALSE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('CohortSurvival', version='0.5.1', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('RPostgres', version='1.4.5', quiet=TRUE, upgrade='never', force=FALSE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            # force=TRUE used to override the packages PatientProfiles installs which versions are too high
            # force=TRUE packages should be put at the end of this list
            f"Rscript -e \"remotes::install_version('visOmopResults', version='0.3.0', quiet=TRUE, upgrade='never', force=TRUE, dependencies=TRUE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
            f"Rscript -e \"remotes::install_version('omopgenerics', version='0.2.1', quiet=TRUE, upgrade='never', force=TRUE, dependencies=FALSE, repos='https://cloud.r-project.org', lib='{r_libs_user_directory}')\"",
        ]
        updated_commands = prepend_libpaths(commands, r_libs_user_directory)
        ShellOperation(commands=updated_commands).run()
    else:
        raise ValueError("Prefect variable: 'r_libs_user' is empty.")


@flow(log_prints=True, persist_result=True)
def cohort_survival_plugin(options: CohortSurvivalOptionsType):
    setup_plugin()
    
    logger = get_run_logger()
    logger.info("Running Cohort Survival")
    
    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db
    target_cohort_definition_id = options.targetCohortDefinitionId
    outcome_cohort_definition_id = options.outcomeCohortDefinitionId
    
    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code, 
                  schema_name=schema_name)    
    
    generate_cohort_survival_data(
        dbdao,
        target_cohort_definition_id,
        outcome_cohort_definition_id
    )
    
@task(
    result_storage=RFS.load(Variable.get("flows_results_sb_name")),
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
    r_libs_user_directory = Variable.get("r_libs_user")

    # Get credentials for database code
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
            pg_host <- "{db_credentials.host}"
            pg_port <- "{db_credentials.port}"
            pg_dbname <- "{db_credentials.databaseName}"
            pg_user <- "{db_credentials.adminUser}"
            pg_password <- "{db_credentials.adminPassword.get_secret_value()}"
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
