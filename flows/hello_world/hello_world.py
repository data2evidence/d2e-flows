   # an HTTP client library and dependency of Prefect
from prefect import flow, task
from rpy2 import robjects

from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner


@task(retries=2)
def get_repo_info(repo_owner: str, repo_name: str):
    """Get info about a repo - will retry twice after failing"""
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
    api_response = httpx.get(url)
    api_response.raise_for_status()
    repo_info = api_response.json()
    return repo_info

@task
def get_contributors(repo_info: dict):
    """Get contributors for a repo"""
    contributors_url = repo_info["contributors_url"]
    response = httpx.get(contributors_url)
    response.raise_for_status()
    contributors = response.json()
    return contributors

@flow(log_prints=True)
def log_repo_info(repo_owner: str = "PrefectHQ", repo_name: str = "prefect"):
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
cdmschema <- "cdm_5pct_9a0f90a32250497d9483c981ef1e1e70"
cohortschema <- "cdm_5pct_zhimin"
cohort_table_name <- "cohorts_trextest1_phenotype"
cohorts_ID <- as.integer(c(25,3,4))


# cdmschema <- {{cdmschema_name}}
# cohortschema <- {{cohortschema_name}}
# cohort_table_name <- {{cohorttable_name}}
# cohorts_ID <- {{cohorts_id}}
cohortDefinitionSets <- create_cohort_definitionsets(cohorts_ID)
cohorts <- create_cohorts(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
cohorts_details_table <- getPhenotypeLog(cohortIds = cohorts$cohortsGenerated$cohortId)

# extract patient id
person_sql <- glue("SELECT person_id FROM {{cdmschema}}.person")
person_id <- renderTranslateQuerySql(connection = connection, sql = person_sql)

cohort_sql <- glue("SELECT subject_id, cohort_definition_id FROM {{cohortschema}}.{{cohort_table_name}}")
cohort_data <- renderTranslateQuerySql(connection = connection, sql = cohort_sql)

# Initialize the table
cohorts_ID <- as.integer(c(25,3,4, 0))
result_matrix <- matrix(0, 
                        nrow = nrow(person_id), 
                        ncol = length(cohorts_ID),
                        dimnames = list(person_id$PERSON_ID, paste0("CohortID_", cohorts_ID)))
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
master_table <- data.frame(getPhenotypeLog(cohorts_ID))
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
    log_repo_info()
