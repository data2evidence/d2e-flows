from rpy2 import robjects

from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.phenotype_plugin.types import PhenotypeOptionsType

from shared_utils.types import UserType
from shared_utils.dao.DBDao import DBDao
from shared_utils.api.AnalyticsSvcAPI import AnalyticsSvcAPI
import os
import time
import logging
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger

# Set up a logger
logging.basicConfig()
rpy2_logger.setLevel(logging.DEBUG)
    
def setup_plugin():
    r_libs_user_directory = Variable.get("r_libs_user")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"""Rscript -e "install.packages('CohortGenerator', quiet=FALSE, upgrade='never', force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')" """,
                f"Rscript -e \"remotes::install_github('OHDSI/PhenotypeLibrary@v3.32.0',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")

def validate_integer_string(input_string):
    input_string = input_string.strip()
    logger = get_run_logger()
    numbers = input_string.split(',')
    for num in numbers:
        num = num.strip()  # Remove any surrounding whitespace
        if not num.isdigit():
            error_message = f"""Input CohortsId: {input_string} is not supported, use ',' as seperator, e.g.: '3,4,25' """
            logger.error(error_message)
            raise ValueError(error_message)
    return True

@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def phenotype_plugin(options: PhenotypeOptionsType):
    logger = get_run_logger()
    logger.info('Running Phenotype')
    setup_plugin()
    logger.info('Setup Done')

    database_code = options.databaseCode
    cdmschema_name = options.cdmschemaName
    cohortschema_name = options.cohortschemaName
    cohorttable_name = options.cohorttableName
    cohorts_id = options.cohortsId
    
    if cohorts_id == 'default':
        set_cohorts_id_str = "cohorts_id <- 'default'"
    elif validate_integer_string(cohorts_id):
        set_cohorts_id_str = f'cohorts_id <- as.integer(c({cohorts_id}))'
   
    use_cache_db = options.use_cache_db
    user = UserType.ADMIN_USER

    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code, 
                  schema_name=cdmschema_name)
    set_db_driver_env_string = dbdao.set_db_driver_env()
    set_connection_string = dbdao.get_database_connector_connection_string(
        user_type=user
    )
   
    r_libs_user_directory = Variable.get("r_libs_user")

    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
                print('start object')
                .libPaths(c('{r_libs_user_directory}',.libPaths()))
                library('CohortGenerator', lib.loc = '{r_libs_user_directory}')
                library('PhenotypeLibrary', lib.loc = '{r_libs_user_directory}')
                library('DatabaseConnector', lib.loc = '/usr/local/lib/R/site-library')
                {set_db_driver_env_string}
                {set_connection_string}

                create_cohort_definitionsets <- function(cohorts_ID) {{
                    # For multiple cohorts
                    if (is.character(cohorts_ID) && cohorts_ID == 'default') {{
                        cohorts <- getPhenotypeLog()  # showHidden=FALSE
                        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts$cohortId[1:nrow(cohorts)])
                        # To solve the 921.json problem
                        cohortDefinitionSets <- cohortDefinitionSets[cohortDefinitionSets$cohortId!=921,]
                        print('Complete creating cohortDefinitionSets')
                    }} else if (class(cohorts_ID) == "integer") {{
                        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts_ID)
                        print('Complete creating cohortDefinitionSets')
                    }} else {{
                        print('Invalid cohorts_ID, should be either "default" or integer string')
                    }}
                    return(cohortDefinitionSets)
                }}

                create_cohorts <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {{
                    # Create the cohort tables to hold the cohort generation results
                    cohortTableNames <- getCohortTableNames(cohortTable = cohort_table_name)
                    CohortGenerator::createCohortTables(connection = connection,
                                                        cohortDatabaseSchema = cohortschema,
                                                        cohortTableNames = cohortTableNames)
                                        
                    print('Complete creating the cohort tables')
                    # Generate the cohorts
                    cohortsGenerated <- CohortGenerator::generateCohortSet(connection = connection,
                                                                        cdmDatabaseSchema = cdmschema,
                                                                        cohortDatabaseSchema = cohortschema,
                                                                        cohortTableNames = cohortTableNames,
                                                                        cohortDefinitionSet = cohortDefinitionSets)

                    # Get the cohort counts
                    cohortCounts <- CohortGenerator::getCohortCounts(connection = connection,
                                                                    cohortDatabaseSchema = cohortschema,
                                                                    cohortTable = cohortTableNames$cohortTable)
                
                    print(cohortCounts)
                    print('Complete generating the cohort tables')
                    # Save cohortgenerator result
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}}, "_cohortgenerated"),
                        data = cohortsGenerated,
                        createTable = TRUE,
                        tempTable = FALSE
                    )
                    print(paste0("Complete saving cohort table to ", {{cohortschema}}))
                    return(list(cohortsGenerated=cohortsGenerated, cohortCounts=cohortCounts))
                }}

                create_result_tables <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {{
                    # Extract patient id
                    person_sql <- paste0("SELECT person_id FROM ", {{cdmschema}}, ".person")
                    person_id <- renderTranslateQuerySql(connection = connection, sql = person_sql)
                    cohort_sql <- paste0("SELECT subject_id, cohort_definition_id FROM ", {{cohortschema}}, ".", {{cohort_table_name}})
                    cohort_data <- renderTranslateQuerySql(connection = connection, sql = cohort_sql)
                    cohorts_id <- cohortDefinitionSets$cohortId
                    
                    # Initialize table
                    result_matrix <- matrix(0, 
                                        nrow = nrow(person_id), 
                                        ncol = length(cohorts_id),
                                        dimnames = list(person_id$PERSON_ID, paste0("CohortID_", cohorts_id)))
                    result_df <- data.frame(result_matrix, check.names = FALSE)

                    # Assign integer 1 to subjects those belong to certain cohorts
                    if (nrow(cohort_data) > 0) {{
                        for (i in 1:nrow(cohort_data)) {{
                            subject <- as.character(cohort_data$SUBJECT_ID[i])
                            cohort <- paste0("CohortID_", cohort_data$COHORT_DEFINITION_ID[i])
                            result_df[subject, cohort] <- 1
                            }}
                    }}

                    # Add PERSON_ID as the first column
                    result_df <- cbind(PERSON_ID = rownames(result_df), result_df)
                    rownames(result_df) <- NULL
                    print('Complete creating result table')

                    # Save result table
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}},"_result_all"),
                        data = result_df,
                        createTable = TRUE,
                        tempTable = FALSE
                    )
                    # Save master table, showHidden=TRUE, display each required cohort in master table
                    master_table <- data.frame(getPhenotypeLog(cohorts_id, showHidden=TRUE)) 
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}},"_result_master"),
                        data = master_table,
                        createTable = TRUE,
                        tempTable = FALSE
                    )
                    print(paste0("Complete saving result table to ", {{cdmschema}}))
                }}

                # Connect to Postgres database using hostname
                connection <- connect(connectionDetails)
                print('Complete connecting to Database')
                                
                cdmschema <- '{cdmschema_name}'
                cohortschema <- '{cohortschema_name}'
                cohort_table_name <- '{cohorttable_name}'
                {set_cohorts_id_str}

                cohortDefinitionSets <- create_cohort_definitionsets(cohorts_id)
                cohorts <- create_cohorts(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
                create_result_tables(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
                
        ''')

    logger.info('phenotype_donellla')
