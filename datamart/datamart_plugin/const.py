# Tables to include that weren't provided in UI input
TABLES_TO_INCLUDE_REGEX = r"^(databasechangelog)$"

# Tables to exclude that weren't provided in UI input e.g. history tables
TABLES_TO_EXCLUDE_REGEX = r"^(A-Za-z)+.*_*(A-Za-z)*(_HISTORY)+$"

# Columns to include across all tables e.g. system columns
COLUMNS_TO_INCLUDE_REGEX = r"^$"

# Columns to exclude across all tables e.g. versioning columns
COLUMNS_TO_EXCLUDE_REGEX = r"^(system_valid_from|system_valid_to)$" # ["SYSTEM_VALID_FROM", "SYSTEM_VALID_TO"]

'''
# Todo: Missing tables in base config:
Other GDM tables, 
cohort_censor_stats, 
cohort_inclusion, 
cohort_summary_stats, 
concept_hierarchy,
episode,
episode_event,
location,
metadata,
survey_conduct,
visit_detail
'''


# Base tables in CDM Schema and the timestamp and person_id columns sto filter on
BASE_CONFIG_LIST = {
    "attribute_definition": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "care_site": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "cdm_source": {
      "timestamp_column": "",
      "person_id_column": ""
    }, 
    "cohort": {
      "timestamp_column": "cohort_start_date",
      "person_id_column": ""
    },
    "cohort_attribute": {
      "timestamp_column": "cohort_start_date",
      "person_id_column": ""
    },
    "cohort_definition": {
      "timestamp_column": "cohort_initiation_date",
      "person_id_column": ""
    },
    "concept": {
      "timestamp_column": "valid_start_date",
      "person_id_column": ""
    },
    "concept_ancestor": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "concept_class": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "concept_recommended": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "concept_relationship": {
      "timestamp_column": "valid_start_date",
      "person_id_column": ""
    },
    "concept_synonym": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "condition_era": {
      "timestamp_column": "condition_era_start_date",
      "person_id_column": "person_id"
    },
    "condition_occurrence": {
      "timestamp_column": "condition_start_datetime",
      "person_id_column": "person_id"
    },
    "cost": {
      "timestamp_column": "",
      "person_id_column": "paid_by_patient"
    },
    "death": {
      "timestamp_column": "death_date",
      "person_id_column": "person_id"
    },
    "device_exposure": {
      "timestamp_column": "device_exposure_start_datetime",
      "person_id_column": "person_id"
    },
    "domain": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "dose_era": {
      "timestamp_column": "dose_era_start_date",
      "person_id_column": "person_id"
    },
    "drug_era": {
      "timestamp_column": "drug_era_start_date",
      "person_id_column": "person_id"
    },
    "drug_exposure": {
      "timestamp_column": "drug_exposure_start_datetime",
      "person_id_column": "person_id"
    },
    "drug_strength": {
      "timestamp_column": "valid_start_date",
      "person_id_column": ""
    },
    "fact_relationship": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "GDM.QUESTIONNAIRE_RESPONSE": {
      "timestamp_column": "authored",
      "person_id_column": "person_id"
    },
    "gdm_questionnaire_response": {
      "timestamp_column": "authored",
      "person_id_column": "person_id"
    },
    "GDM.RESEARCH_SUBJECT": {
      "timestamp_column": "",
      "person_id_column": "person_id"
    },
    "gdm_research_subject": {
      "timestamp_column": "",
      "person_id_column": "person_id"
    },
    "measurement": {
      "timestamp_column": "measurement_date",
      "person_id_column": "person_id"
    },
    "note": {
      "timestamp_column": "note_date",
      "person_id_column": "person_id"
    },
    "note_nlp": {
      "timestamp_column": "nlp_date",
      "person_id_column": ""
    },    
    "observation": {
      "timestamp_column": "observation_date",
      "person_id_column": "person_id"
    },
    "observation_period": {
      "timestamp_column": "observation_period_start_date",
      "person_id_column": "person_id"
    },
    "payer_plan_period": {
      "timestamp_column": "payer_plan_period_start_date",
      "person_id_column": "person_id"
    },
    "person": {
      "timestamp_column": "",
      "person_id_column": "person_id"
    },
    "procedure_occurrence": {
      "timestamp_column": "procedure_datetime",
      "person_id_column": "person_id"
    },
    "provider": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "relationship": {
      "timestamp_column": "",
      "person_id_column": ""
    },
    "source_to_concept_map": {
      "timestamp_column": "valid_start_date",
      "person_id_column": ""
    },
    "specimen": {
      "timestamp_column": "specimen_date",
      "person_id_column": "person_id"
    },
    "visit_occurrence": {
      "timestamp_column": "visit_start_date",
      "person_id_column": "person_id"
    }
}


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