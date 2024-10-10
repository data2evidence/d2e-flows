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
   
    r_libs_user_directory = Variable.get("r_libs_user").value

    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
                   
library(DatabaseConnector)
library(CohortGenerator)
library(PhenotypeLibrary)
library(glue)

create_cohort_definitionsets <- function(cohorts_ID) {{
    # For multiple cohorts
    if (is.character(cohorts_ID) && cohorts_ID == 'default') {{
        # To solve the 921.json problem
        cohorts <- getPhenotypeLog()
        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts$cohortId[1:nrow(cohorts)])
        cohortDefinitionSets <- cohortDefinitionSets[cohortDefinitionSets$cohortId!=921,]
    }} else if (class(cohorts_ID) == "integer") {{
        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts_ID)
        
        # for test if two cohorts exist
        cohortJsonFileName <- "/Users/zhi.mindata4life-asia.care/Documents/Projects/PhenotypeLibrary/inst/cohorts/25.json"
        cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
        cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
        cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
        cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = TRUE))
        cohortDefinitionSets <- rbind(cohortDefinitionSets, data.frame(cohortId = 0,
                                                    cohortName = 'duplicate-25', 
                                                    json = cohortJson,
                                                    sql = cohortSql,
                                                    stringsAsFactors = FALSE))
    }} else {{
        print('invalid cohorts_ID, should be either default or integer')
    }}
    return(cohortDefinitionSets)
}}
                   
create_cohorts <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {{
    # Create the cohort tables to hold the cohort generation results
    cohortTableNames <- getCohortTableNames(cohortTable = cohort_table_name)
    CohortGenerator::createCohortTables(connection = connection,
                                        cohortDatabaseSchema = cohortschema,
                                        cohortTableNames = cohortTableNames)
                        

    # Generate the cohorts
    cohortsGenerated <- CohortGenerator::generateCohortSet(connection = connection,
                                                        cdmDatabaseSchema = cdmschema,
                                                        cohortDatabaseSchema = cohortschema,
                                                        cohortTableNames = cohortTableNames,
                                                        cohortDefinitionSet = cohortDefinitionSets)

    # # Get the cohort counts
    cohortCounts <- CohortGenerator::getCohortCounts(connection = connection,
                                                    cohortDatabaseSchema = cohortschema,
                                                    cohortTable = cohortTableNames$cohortTable)
 
    # save cohortgenerator result
    DatabaseConnector::insertTable(
        connection = connection,
        databaseSchema = cohortschema,
        tableName = glue("{{cohort_table_name}}_cohortgenerated"),
        data = cohortsGenerated,
        createTable = TRUE,
        tempTable = FALSE
    )

    return(list(cohortsGenerated=cohortsGenerated, cohortCounts=cohortCounts))
}}

# Connect to Postgres database using hostname
connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "127.0.0.1/alpdev_pg", # This will be the local end of the SSH tunnel
    user = "postgres",
    password = "Toor1234",
    port = 41191, # Local port for the SSH tunnel, if postgres docker is running, the 41190 port will be occupied, then change to 41191
    pathToDriver = "~/Documents/D4L/202409_Phenotype/jdbcDrivers/"
)
connection <- connect(connectionDetails)
                   
# cdmschema <- "cdm_5pct_9a0f90a32250497d9483c981ef1e1e70"
# cohortschema <- "cdm_5pct_zhimin"
# cohort_table_name <- "cohorts_trextest1_phenotype"
                   
cdmschema <- '{cdmschema_name}'
cohortschema <- '{cohortschema_name}'
cohort_table_name <- '{cohorttable_name}'
cohorts_id <- {cohorts_id_str}

cohortDefinitionSets <- create_cohort_definitionsets(cohorts_id)
cohorts <- create_cohorts(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
cohorts_details_table <- getPhenotypeLog(cohortIds = cohorts$cohortsGenerated$cohortId)

# extract patient id
person_sql <- glue("SELECT person_id FROM {{cdmschema}}.person")
person_id <- renderTranslateQuerySql(connection = connection, sql = person_sql)

cohort_sql <- glue("SELECT subject_id, cohort_definition_id FROM {{cohortschema}}.{{cohort_table_name}}")
cohort_data <- renderTranslateQuerySql(connection = connection, sql = cohort_sql)

# Initialize the table
cohorts_id <- as.integer(c(cohorts_id,0))
result_matrix <- matrix(0, 
                        nrow = nrow(person_id), 
                        ncol = length(cohorts_id),
                        dimnames = list(person_id$PERSON_ID, paste0("CohortID_", cohorts_id)))
result_df <- data.frame(result_matrix, check.names = FALSE)

# Assign the subject to each cohort
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
    tableName = glue("{{cohort_table_name}}_result_all"),
    data = result_df,
    createTable = TRUE,
    tempTable = FALSE
)
# master table
master_table <- data.frame(getPhenotypeLog(cohorts_id))
DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortschema,
    tableName = glue("{{cohort_table_name}}_result_master"),
    data = master_table,
    createTable = TRUE,
    tempTable = FALSE
)
        ''')

if __name__ == "__main__":
    options = {
        # "databaseCode":"alpdev_pg",
        # "cdmschemaName":"cdm_5pct_9a0f90a32250497d9483c981ef1e1e70",
        # "cohortschemaName": "cdm_5pct_zhimin",
        # "cohorttableName": "cohorts_trextest2_phenotype",
        # "cohortsId": "default",
        "description": 'test',  
        "owner": "testowner"
               }
    phenotype_plugin(options=options)