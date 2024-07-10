# DICOM ETL Plugin
.

## Pre-requisities to running DICOM ETL Flow
  - Create medical imaging schema with data load plugin (Link)

## How to run the plugin:

- Trigger from jobs page e.g.
```
{
  "options": {
    "flow_action_type": "insert_metadata_data",
    "database_code": "alpdev_pg",
    "medical_imaging_schema_name": "cdmmedicalimaging",
    "cdm_schema_name": "cdmdefault",
    "vocab_schema_name": "cdmvocab",
    "root_folder": "/tmp/files",
    "upload_files": Optional[bool] = False # Set to true to upload file to dicom server
  }
}
```

## Flow Structure
  - Load DICOM vocabulary 
    - `Vocabulary` table
    - `Concept Class` table
    - `Concept` table 
  - Load DICOM Data Elements in `dicom_data_element` table
  - Process each DICOM file in supplied folder parameter
    - Extract attributes to get/create `person` record
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



