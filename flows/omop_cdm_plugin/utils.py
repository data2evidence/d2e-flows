from sqlalchemy import select
from datetime import datetime

from prefect import task
from prefect.logging import get_run_logger

from flows.omop_cdm_plugin.types import RELEASE_VERSION_MAPPING, CDMVersion


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

    logger.info(f"Retrieving 'vocabulary_version' from vocab schema {vocab_schema_dao.schema_name} with cdm_concept_code '{cdm_concept_code}'..")
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
    if cdm_version == CDMVersion.OMOP54:
        # v5.3 does not have 'cdm_version_concept_id' column
        values_to_insert["cdm_version_concept_id"] = cdm_version_concept_id
    
    logger.info(f"Inserting CDM Version into 'cdm_source' table..")
    schema_dao.insert_values_into_table("cdm_source", values_to_insert)