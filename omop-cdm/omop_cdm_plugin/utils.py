from typing import Dict
from omop_cdm_plugin.types import *

# List of tables linked to person table
NON_PERSON_ENTITIES = {
    "observation_period": "observation_period_id",
    "death": "person_id",
    "visit_occurrence": "visit_occurrence_id",
    "visit_detail": "visit_detail_id",
    "condition_occurrence": "condition_occurrence_id",
    "drug_exposure": "drug_exposure_id",
    "procedure_occurrence": "procedure_occurrence_id",
    "device_exposure": "device_exposure_id",
    "measurement": "measurement_id",
    "observation": "observation_id",
    "note": "note_id",
    "episode": "episode_id",
    "specimen": "specimen_id"
}


def get_cdm_release_date(dbdao, logger) -> str:
    try:
        patient_count = dbdao.get_value(table_name="cdm_source", column_name="cdm_release_date")
    except Exception as e:
        error_msg = f"Error retrieving patient count"
        logger.error(f"{error_msg}: {e}")
        patient_count = error_msg
    return str(patient_count)

def get_patient_count(dbdao, logger) -> str:
    try:
        patient_count = dbdao.get_distinct_count("person", "person_id")
    except Exception as e:
        error_msg = f"Error retrieving patient count"
        logger.error(f"{error_msg}: {e}")
        patient_count = error_msg
    return str(patient_count)


def get_total_entity_count(entity_count_distribution: Dict, logger) -> str:
    try:
        total_entity_count = 0
        for entity, entity_count in entity_count_distribution.items():
            # value could be str(int) or "error"
            if entity_count == "error":
                continue
            else:
                total_entity_count += int(entity_count)
    except Exception as e:
        error_msg = f"Error retrieving entity count"
        logger.error(f"{error_msg}: {e}")
        total_entity_count = error_msg
    return str(total_entity_count)


def get_entity_count_distribution(dbdao, logger) -> EntityCountDistributionType:
    entity_count_distribution = {}
    # retrieve count for each entity table
    for table, unique_id_column in NON_PERSON_ENTITIES.items():
        try:
            entity_count = dbdao.get_distinct_count(table, unique_id_column)
        except Exception as e:
            logger.error(f"Error retrieving entity count for {table}: {e}")
            entity_count = "error"
        entity_count_key = table.replace("_", " ").title() + " Count"
        if entity_count != "error":
            entity_count_distribution[entity_count_key] = str(entity_count)
    return entity_count_distribution


def get_cdm_version(dbdao, logger) -> str:
    try:
        cdm_version = dbdao.get_value("cdm_source", "cdm_version")
    except Exception as e:
        error_msg = f"Error retrieving CDM version"
        logger.error(f"{error_msg}: {e}")
        cdm_version = error_msg
    return str(cdm_version)