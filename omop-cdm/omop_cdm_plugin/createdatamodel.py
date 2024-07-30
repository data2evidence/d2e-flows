from datetime import datetime
from sqlalchemy import select

from prefect import task, get_run_logger

from omop_cdm_plugin.types import RELEASE_VERSION_MAPPING

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
    
    logger.info(f"Retrieving 'cdm_version_concept_id' from vocab schema {vocab_schema_dao.schema_name} with cdm_concept_code '{cdm_concept_code}'..")
    cdm_version_concept_id = vocab_schema_dao.execute_sqlalchemy_statement(get_cdm_version_concept_id_statement,
                                                                     vocab_schema_dao.get_single_value)

    logger.info(f"Retrieving 'cdm_version_concept_id' from vocab schema {vocab_schema_dao.schema_name} with cdm_concept_code '{cdm_concept_code}'..")
    vocabulary_version = vocab_schema_dao.execute_sqlalchemy_statement(get_vocabulary_version_statement,
                                                                 vocab_schema_dao.get_single_value)


    values_to_insert = {
        "cdm_source_name": schema_dao.schema_name,
        "cdm_source_abbreviation": schema_dao.schema_name[0:25],
        "cdm_holder": "D4L",
        "source_release_date": datetime.now(),
        "cdm_release_date": datetime.now(),
        "cdm_version": cdm_version,
        "vocabulary_version": vocabulary_version,
    }
    if cdm_version == "5.4":
        # v5.3 does not have 'cdm_version_concept_id' column
        values_to_insert["cdm_version_concept_id"] = cdm_version_concept_id
    
    logger.info(f"Inserting CDM Version into 'cdm_source' table..")
    schema_dao.insert_values_into_table("cdm_source", values_to_insert)




@task(log_prints=True)
def create_and_assign_roles(userdao, tenant_configs, cdm_version: str):
    logger = get_run_logger()
    # Check if schema read role exists

    schema_read_role = f"{userdao.schema_name}_read_role"

    schema_read_role_exists = userdao.check_role_exists(schema_read_role)
    if schema_read_role_exists:
        logger.info(f"'{schema_read_role}' role already exists")
    else:
        logger.info(f"{schema_read_role} does not exist")
        userdao.create_read_role(schema_read_role)
    # grant schema read role read privileges to schema
    logger.info(f"Granting read privileges to '{schema_read_role}'")
    userdao.grant_read_privileges(schema_read_role)

    # Check if read user exists
    read_user = tenant_configs.get("readUser")

    read_user_exists = userdao.check_user_exists(read_user)
    if read_user_exists:
        logger.info(f"{read_user} user already exists")
    else:
        logger.info(f"{read_user} user does not exist")
        read_password = tenant_configs.get("readPassword")
        logger.info(f"Creating user '{read_user}'")
        userdao.create_user(read_user, read_password)

    # Check if read role exists
    read_role = tenant_configs.get("readRole")

    read_role_exists = userdao.check_role_exists(read_role)
    if read_role_exists:
        logger.info(f"'{read_role}' role already exists")
    else:
        logger.info(f"'{read_role}' role does not exist")
        logger.info(
            f"'Creating '{read_role}' role and assigning to '{read_user}' user")
        userdao.create_and_assign_role(read_user, read_role)

    # Grant read role read privileges
    logger.info(f"'Granting read privileges to '{read_role}' role")
    userdao.grant_read_privileges(read_role)
    
    if cdm_version > "5.3":
        # v5.3 does not have cohort table
        # Grant write cohort and cohort_definition table privileges to read role
        logger.info(f"'Granting cohort write privileges to '{read_role}' role")
        userdao.grant_cohort_write_privileges(read_role)