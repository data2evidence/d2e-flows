# DICOM ETL Plugin
.

## Pre-requisities to running DICOM ETL Flow
  - Create medical imaging schema with data load plugin (Link)
  - Load DICOM vocab using this plugin

## How to run the plugin:

- Trigger from jobs page e.g.
```
{
  "options": {
    "flow_action_type": "ingest_metadata",
    "database_code": "alpdev_pg",
    "medical_imaging_schema_name": "cdmmedicalimaging",
    "cdm_schema_name": "cdmdefault",
    "vocab_schema_name": "cdmvocab",
    "dicom_files_abs_path": "/tmp/files/",
    "to_truncate": False,
    "upload_files": False,
    "missing_person_id_options": "use_id_zero",
    "PersonPatientMapping": {
      schema_name: "cdmmedicalimaging",
      table_name: "mappingtable",
      person_id_column_name: "source_person_id",
      patient_id_column_name: "source_patient_id"
    }
  }
}
```

- Flow Action Type `ingest_metadata` to ingest the metadata into `dicom_file_metadata` table
  - With option upload_files: False/True to upload files to server
- Flow Action Type `load_vocab` to load DICOM vocab in vocab schema
  - With option to_truncate: False/True to truncate DICOM vocab
- missing_person_id_options
  - `SKIP` raises an error if person id cannot be found
  - `USE_ID_ZERO` use person_id 0 to create procedure occurrence and image occurrence records
- person_to_patient_mapping: mapping table used to map `patient_id` in DICOM file to `person_id` in `person` table
  - schema_name: schema that contains the mapping table
  - table_name: mapping table name
  - person_id_column_name: column in mapping table that contains the `person_id` in `person` table
  - patient_id_column_name: column in mapping table that contains the `patient_id` in the .DCM file


## Flow Structure
  - Load DICOM vocabulary 
    - `Vocabulary` table
    - `Concept Class` table
    - `Concept` table 
  - Load DICOM Data Elements in `dicom_data_element` table
  - Process each DICOM file in supplied folder parameter
    - Extract attributes to get/create `person` record
      - Use `
    - Extract attributes to create `procedure_occurrence` record
      - Requires `person_id`
    - Extract attributes to create `image_occurrence` record
      - Requires `person_id` and `procedure_occurrence_id`
    - Extract attributes and ingest into `dicom_file_metadata` table
      - Requires `image_occurrence_id`
  - Upload file to DICOM server (Optional) 
    - Store uploaded metadata in `file_upload_metadata` table
    - Requires `image_occurrence_id`, `sop_instance_id`

## DICOM Vocabulary Source / Folder Structure
- CSV files for DICOM vocabularies, concepts, and data elements sourced from https://github.com/paulnagy/DICOM2OMOP are stored in `external` folder
- Additional scripts used to modify CSV in `scripts` folder



