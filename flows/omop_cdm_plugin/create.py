from rpy2 import robjects
from datetime import datetime
from sqlalchemy import BigInteger, String, select

from shared_utils.types import UserType
from shared_utils.dao.DBDao import DBDao
from shared_utils.dao.UserDao import UserDao
from shared_utils.create_dataset_tasks import *

from prefect import task
from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect.cache_policies import NONE

from flows.omop_cdm_plugin.types import CDMVersion, RELEASE_VERSION_MAPPING


@task(log_prints=True)
def setup_plugin_task(release_version):
    r_libs_user_directory = Variable.get("r_libs_user")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CommonDataModel@{release_version}',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Prefect variable: 'r_libs_user' is empty.")

    
@task(log_prints=True, 
      timeout_seconds=1800,
      cache_policy=NONE,
      task_run_name="create_datamodel_parent_task-{schema_dao.schema_name}")
def create_datamodel_parent_task(cdm_version: str, 
                                 schema_dao: DBDao,
                                 userdao: UserDao, 
                                 vocab_schema: str):
    '''
    Parent task to run R package to create tables and assign permissions
    '''
    logger = get_run_logger()
    #setup_plugin(release_version=release_version)
    create_cdm_tables(schema_dao, cdm_version, logger)
    create_concept_recommended_table(schema_dao, logger)
    create_and_assign_roles_task(userdao=userdao)
    if cdm_version == CDMVersion.OMOP54:
        # v5.3 does not have cohort table
        # Grant write cohort and cohort_definition table privileges to read role
        grant_cohort_write_privileges(userdao, logger)

    if schema_dao.schema_name != vocab_schema:
        # Insert CDM Version
        vocab_schema_dao = DBDao(use_cache_db=schema_dao.use_cache_db,
                                 database_code=schema_dao.database_code, 
                                 schema_name=vocab_schema)

        insert_cdm_version(
            cdm_version=cdm_version,
            schema_dao=schema_dao,
            vocab_schema_dao=vocab_schema_dao
        )
    else:
        # If newly created schema is also the vocab schema
        # Todo: Add insertion of cdm version to update flow
        logger.info(f"Skipping insertion of CDM Version '{cdm_version}'. Please load vocabulary data first.")

     
@task(log_prints=True,
      task_run_name="create_cdm_tables-{dbdao.schema_name}")
def create_cdm_tables(dbdao: DBDao, cdm_version: str, logger):
    # currently only supports pg dialect
    r_libs_user_directory = Variable.get("r_libs_user")

    admin_user =  UserType.ADMIN_USER
    set_connection_string = dbdao.get_database_connector_connection_string(
        user_type=admin_user
    )
    
    logger.info(f"Running CommonDataModel version '{cdm_version}' on schema '{dbdao.schema_name}' in database '{dbdao.database_code}'")
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(
            f'''
            .libPaths(c('{r_libs_user_directory}',.libPaths()))
            library('CommonDataModel', lib.loc = '{r_libs_user_directory}')
            {set_connection_string}
            cdm_version <- "{cdm_version}"
            schema_name <- "{dbdao.schema_name}"
            CommonDataModel::executeDdl(connectionDetails = connectionDetails, cdmVersion = cdm_version, cdmDatabaseSchema = schema_name, executeDdl = TRUE, executePrimaryKey = TRUE, executeForeignKey = FALSE)
            '''
        )
    logger.info(f"Succesfully ran CommonDataModel version '{cdm_version}' on schema '{dbdao.schema_name}' in database '{dbdao.database_code}'")


@task(log_prints=True,
      task_run_name="create_concept_recommended_table-{dbdao.schema_name}")
def create_concept_recommended_table(dbdao: DBDao, logger):
    table_name = "concept_recommended"
    columns_to_create = {
            "concept_id_1": BigInteger,
            "concept_id_2": BigInteger,
            "relationship_id": String(20)
    }
    logger.info(f"Creating '{table_name}' table..")
    dbdao.create_table(table_name, columns_to_create)
    logger.info(f"Sucessfully created '{table_name}' table!")


@task(log_prints=True,
      task_run_name="grant_cohort_write_privileges-{userdao.schema_name}")
def grant_cohort_write_privileges(userdao: UserDao, logger):
    logger.info(f"Granting cohort write privileges to '{userdao.read_role}' role")
    userdao.grant_cohort_write_privileges(userdao.read_role)

@task(log_prints=True)
def insert_cdm_version(cdm_version: str, schema_dao, vocab_schema_dao):  
    logger = get_run_logger() 
    
    # Populate 'cdm_version_concept_id' and 'vocabulary_version' values from vocab
    # https://ohdsi.github.io/CommonDataModel/cdm54.html#cdm_source
    
    cdm_concept_code = "CDM " + RELEASE_VERSION_MAPPING.get(cdm_version)
    
    concept_column_names = ["concept_id", "vocabulary_id", "concept_class_id", "concept_code"]
    vocabulary_column_names = ["vocabulary_version", "vocabulary_id"]
    
    concept_columns = vocab_schema_dao.get_sqlalchemy_columns("concept", concept_column_names)
    vocabulary_columns = vocab_schema_dao.get_sqlalchemy_columns("vocabulary", vocabulary_column_names)
    
    get_cdm_version_concept_id_statement = select(concept_columns["concept_id"]) \
                                            .where(concept_columns["vocabulary_id"] == "CDM") \
                                            .where(concept_columns["concept_class_id"] == "CDM") \
                                            .where(concept_columns["concept_code"] == cdm_concept_code)
                                            
    get_vocabulary_version_statement = select(vocabulary_columns["vocabulary_version"]) \
                                            .where(vocabulary_columns["vocabulary_id"] == "None") \
    
    
    cdm_version_concept_id = vocab_schema_dao.execute_sqlalchemy_statement(get_cdm_version_concept_id_statement,
                                                                     vocab_schema_dao.get_single_value)
    logger.info(f"Retrieved cdm_version_concept_id '{cdm_version_concept_id}' from vocab schema '{vocab_schema_dao.schema_name}' with cdm_concept_code '{cdm_concept_code}'..")

    
    vocabulary_version = vocab_schema_dao.execute_sqlalchemy_statement(get_vocabulary_version_statement,
                                                                 vocab_schema_dao.get_single_value)
    logger.info(f"Retrieved vocabulary_version '{vocabulary_version}' from vocab schema '{vocab_schema_dao.schema_name}' with cdm_concept_code '{cdm_concept_code}'..")


    values_to_insert = {
        "cdm_source_name": schema_dao.schema_name,
        "cdm_source_abbreviation": schema_dao.schema_name[0:25],
        "cdm_holder": "D4L",
        "source_release_date": datetime.now(),
        "cdm_release_date": datetime.now(),
        "cdm_version": cdm_version,
        "vocabulary_version": vocabulary_version,
    }
    if cdm_version == CDMVersion.OMOP54:
        # v5.3 does not have 'cdm_version_concept_id' column
        values_to_insert["cdm_version_concept_id"] = cdm_version_concept_id
    
    logger.info(f"Inserting CDM Version into 'cdm_source' table..")
    schema_dao.insert_values_into_table("cdm_source", values_to_insert)
    