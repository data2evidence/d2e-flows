# print('load rJavay')
# library(rJava)
# .jinit()
# .jcall("java/lang/Runtime", "Ljava/lang/Runtime;", "getRuntime")$totalMemory()
# .jcall("java/lang/Runtime", "Ljava/lang/Runtime;", "getRuntime")$maxMemory()

print('start object')
# .libPaths(c('{r_libs_user_directory}',.libPaths()))
# r_libs_user_directory <- '/home/docker/plugins/R/site-library'
print('load database')
library('DatabaseConnector')
# library('DatabaseConnector', lib.loc = '/usr/local/lib/R/site-library/')
print('load cohortgenerator')
library('CohortGenerator', lib.loc = {r_libs_user_directory})
library('PhenotypeLibrary', lib.loc = {r_libs_user_directory})
print('end loading')
Sys.setenv('DATABASECONNECTOR_JAR_FOLDER' = '/app/inst/drivers')
library(glue)
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = 'postgresql', connectionString = 'jdbc:postgresql://alp-minerva-postgres-1:5432/alpdev_pg', user = 'postgres_tenant_admin_user', password = 'Toor1234', pathToDriver = '/app/inst/drivers')

print('end string')
# print(connectionDetails)

create_cohort_definitionsets <- function(cohorts_ID) {
    # For multiple cohorts
    print(class(cohorts_ID))
    if (is.character(cohorts_ID) && cohorts_ID == 'default') {
        # To solve the 921.json problem
        cohorts <- getPhenotypeLog()
        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts$cohortId[1:nrow(cohorts)])
        print('end cohortDefinitionSets')
        cohortDefinitionSets <- cohortDefinitionSets[cohortDefinitionSets$cohortId!=921,]
    } else if (class(cohorts_ID) == "integer") {
        cohortDefinitionSets <- getPlCohortDefinitionSet(cohorts_ID)
        print('end cohortDefinitionSets')
    } else {
        print('invalid cohorts_ID, should be either default or integer')
    }
    return(cohortDefinitionSets)
}
                
create_cohorts <- function(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets) {
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

    print('end cohort coutns')
    # save cohortgenerator result
    DatabaseConnector::insertTable(
        connection = connection,
        databaseSchema = cohortschema,
        tableName = glue("{cohort_table_name}_cohortgenerated"),
        data = cohortsGenerated,
        createTable = TRUE,
        tempTable = FALSE
    )

    return(list(cohortsGenerated=cohortsGenerated, cohortCounts=cohortCounts))
}

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
                
cdmschema <- 'cdm_5pct_9a0f90a32250497d9483c981ef1e1e70'
cohortschema <- 'cdm_5pct_zhimin'
cohort_table_name <- 'cohorts_test4_phenotype'
cohorts_id <- as.integer(c(25,3,4))
print(cohorts_id)

print('start create_cohort_definitiaonsets')
cohortDefinitionSets <- create_cohort_definitionsets(cohorts_id)
print('end create_cohort_definitiaonsets')
cohorts <- create_cohorts(connection, cdmschema, cohortschema, cohort_table_name, cohortDefinitionSets)
print('end create_cohorts')
cohorts_details_table <- getPhenotypeLog(cohortIds = cohorts$cohortsGenerated$cohortId)

print('end cohort generator')
# extract patient id
person_sql <- glue("SELECT person_id FROM {cdmschema}.person")
person_id <- renderTranslateQuerySql(connection = connection, sql = person_sql)

cohort_sql <- glue("SELECT subject_id, cohort_definition_id FROM {cohortschema}.{cohort_table_name}")
cohort_data <- renderTranslateQuerySql(connection = connection, sql = cohort_sql)

# Initialize the table
cohorts_id <- as.integer(c(cohorts_id,0))
result_matrix <- matrix(0, 
                        nrow = nrow(person_id), 
                        ncol = length(cohorts_id),
                        dimnames = list(person_id$PERSON_ID, paste0("CohortID_", cohorts_id)))
result_df <- data.frame(result_matrix, check.names = FALSE)

# Assign the subject to each cohort
for (i in 1:nrow(cohort_data)) {
subject <- cohort_data$SUBJECT_ID[i]
cohort <- paste0("CohortID_", cohort_data$COHORT_DEFINITION_ID[i])
result_df[subject, cohort] <- 1
}

# Add PERSON_ID as the first column
result_df <- cbind(PERSON_ID = rownames(result_df), result_df)
rownames(result_df) <- NULL

# View the result
print(head(result_df))

# save result_df
DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortschema,
    tableName = glue("{cohort_table_name}_result_all"),
    data = result_df,
    createTable = TRUE,
    tempTable = FALSE
)
# master table
master_table <- data.frame(getPhenotypeLog(cohorts_id))
DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortschema,
    tableName = glue("{cohort_table_name}_result_master"),
    data = master_table,
    createTable = TRUE,
    tempTable = FALSE
)
