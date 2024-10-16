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
    r_libs_user_directory = Variable.get("r_libs_user").value
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        print(r_libs_user_directory)
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
        
    database_code = options.databaseCode
    cdmschema_name = options.cdmschemaName
    cohortschema_name = options.cohortschemaName
    cohorttable_name = options.cohorttableName
    cohorts_id = options.cohortsId

    # setup_plugin()
    # print('setup_done')

    # database_code = 'alpdev_pg'
    # cdmschema_name = "cdmdefault"
    # cohortschema_name = "cdmdefault"
    # cohorttable_name = "data_1098_phenotype"
    # cohorts_id = '25,3,4'
    
    if cohorts_id == 'default':
        cohorts_id_str = cohorts_id
    elif validate_integer_string(cohorts_id):
        cohorts_id_str = f'as.integer(c({cohorts_id}))'
    logger.info(f'{cohorts_id_str}')
   
    use_cache_db = options.use_cache_db
    user = UserType.ADMIN_USER

    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code, 
                  schema_name=cdmschema_name)
    set_db_driver_env_string = dbdao.set_db_driver_env()
    set_connection_string = dbdao.get_database_connector_connection_string(
        user_type=user
    )
   
    r_libs_user_directory = Variable.get("r_libs_user").value
    logger.info(f'{set_db_driver_env_string}')
    logger.info(f'{set_connection_string}')
    logger.info(f'{r_libs_user_directory}')

    # print("Starting sleep...")
    # time.sleep(6000)
    # print("Finished sleeping after 20 seconds.")


    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
                print('start object')
                .libPaths(c('{r_libs_user_directory}',.libPaths()))
                library('CohortGenerator', lib.loc = '{r_libs_user_directory}')
                library('PhenotypeLibrary', lib.loc = '{r_libs_user_directory}')
                library('DatabaseConnector', lib.loc = '/usr/local/lib/R/site-library')
                {set_db_driver_env_string}
                {set_connection_string}
               
                print('end string')
                # print(connectionDetails)

                create_cohort_definitionsets <- function(cohorts_ID) {{
                    # For multiple cohorts
                    print(class(cohorts_ID))
                    if (is.character(cohorts_ID) && cohorts_ID == 'default') {{
                        # To solve the 921.json problem
                        cohorts <- getPhenotypeLog()
                        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts$cohortId[1:nrow(cohorts)])
                        print('end cohortDefinitionSets')
                        cohortDefinitionSets <- cohortDefinitionSets[cohortDefinitionSets$cohortId!=921,]
                    }} else if (class(cohorts_ID) == "integer") {{
                        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts_ID)
                        print('end cohortDefinitionSets')
                    }} else {{
                        print('invalid cohorts_ID, should be either default or integer')
                    }}
                    return(cohortDefinitionSets)
                }}
                                
                create_cohorts <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {{
                    # Create the cohort tables to hold the cohort generation results
                    print('start get tablenames')
                    cohortTableNames <- getCohortTableNames(cohortTable = cohort_table_name)
                    print('start create tables')
                    # print(cohortTableNames)
                    print(cohortschema)
                    CohortGenerator::createCohortTables(connection = connection,
                                                        cohortDatabaseSchema = cohortschema,
                                                        cohortTableNames = cohortTableNames)
                                        
                    print('end create the cohor tables')
                    # Generate the cohorts
                    cohortsGenerated <- CohortGenerator::generateCohortSet(connection = connection,
                                                                        cdmDatabaseSchema = cdmschema,
                                                                        cohortDatabaseSchema = cohortschema,
                                                                        cohortTableNames = cohortTableNames,
                                                                        cohortDefinitionSet = cohortDefinitionSets)

                    print('end generate cohortset')
                     # # Get the cohort counts
                    cohortCounts <- CohortGenerator::getCohortCounts(connection = connection,
                                                                    cohortDatabaseSchema = cohortschema,
                                                                    cohortTable = cohortTableNames$cohortTable)
                
                    print(cohortCounts)
                    print('end cohort coutns')
                    # save cohortgenerator result
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}}, "_cohortgenerated"),
                        data = cohortsGenerated,
                        createTable = TRUE,
                        tempTable = FALSE
                    )

                    return(list(cohortsGenerated=cohortsGenerated, cohortCounts=cohortCounts))
                }}

                create_result_tables <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {{
                    # extract patient id
                    person_sql <- paste0("SELECT person_id FROM ", {{cdmschema}}, ".person")
                    person_id <- renderTranslateQuerySql(connection = connection, sql = person_sql)

                    cohort_sql <- paste0("SELECT subject_id, cohort_definition_id FROM ", {{cohortschema}}, ".", {{cohort_table_name}})
                    cohort_data <- renderTranslateQuerySql(connection = connection, sql = cohort_sql)
                    
                    cohorts_id <- cohortDefinitionSets$cohortId

                    result_matrix <- matrix(0, 
                                        nrow = nrow(person_id), 
                                        ncol = length(cohorts_id),
                                        dimnames = list(person_id$PERSON_ID, paste0("CohortID_", cohorts_id)))

                    result_df <- data.frame(result_matrix, check.names = FALSE)

                    for (i in 1:nrow(cohort_data)) {{
                        subject <- cohort_data$SUBJECT_ID[i]
                        cohort <- paste0("CohortID_", cohort_data$COHORT_DEFINITION_ID[i])
                        result_df[subject, cohort] <- 1
                        }}

                    # Add PERSON_ID as the first column
                    result_df <- cbind(PERSON_ID = rownames(result_df), result_df)
                    rownames(result_df) <- NULL

                    # View the result
                    print(head(result_df))

                    # save result_df
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}},"_result_all"),
                        data = result_df,
                        createTable = TRUE,
                        tempTable = FALSE
                    )
                    # master table
                    master_table <- data.frame(getPhenotypeLog(cohorts_id))
                    DatabaseConnector::insertTable(
                        connection = connection,
                        databaseSchema = cohortschema,
                        tableName = paste0({{cohort_table_name}},"_result_master"),
                        data = master_table,
                        createTable = TRUE,
                        tempTable = FALSE
                    )
                }}

                print('start connect')
                # Connect to Postgres database using hostname
                # connectionDetails <- createConnectionDetails(
                #     dbms = "postgresql",
                #     server = "127.0.0.1/alpdev_pg", # This will be the local end of the SSH tunnel
                #     user = "postgres",
                #     password = "Toor1234",
                #     port = 41191, # Local port for the SSH tunnel, if postgres docker is running, the 41190 port will be occupied, then change to 41191
                #     pathToDriver = "~/Documents/D4L/202409_Phenotype/jdbcDrivers/"
                # )
                connection <- connect(connectionDetails)
                                
                print('end connect')
                # cdmschema <- "cdm_5pct_9a0f90a32250497d9483c981ef1e1e70"
                # cohortschema <- "cdm_5pct_zhimin"
                # cohort_table_name <- "cohorts_trextest1_phenotype"
                                
                cdmschema <- '{cdmschema_name}'
                cohortschema <- '{cohortschema_name}'
                cohort_table_name <- '{cohorttable_name}'
                cohorts_id <- '{cohorts_id_str}'
                print(cohorts_id)

                print('start create_cohort_definitiaonsets')
                cohortDefinitionSets <- create_cohort_definitionsets(cohorts_id)
                print('end create_cohort_definitiaonsets')
                cohorts <- create_cohorts(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
                print('end create_cohorts')
                cohorts_details_table <- getPhenotypeLog(cohortIds = cohorts$cohortsGenerated$cohortId)
                print('start creat result table')
                create_result_tables(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
                print('end creat result table')

        ''')

    logger.info('phenotype_donellla')
