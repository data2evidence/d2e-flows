--liquibase formatted sql
--changeset alp:V5.4.1.2.7__update_cohort_to_omop_standard
-- Drop owner and cohort_modification_date column
ALTER TABLE "COHORT_DEFINITION"
DROP ("COHORT_MODIFICATION_DATE");

ALTER TABLE "COHORT_DEFINITION"
DROP ("OWNER");

-- Set null values to 0
UPDATE "COHORT_DEFINITION"
SET
    "DEFINITION_TYPE_CONCEPT_ID" = 0
WHERE
    "DEFINITION_TYPE_CONCEPT_ID" IS NULL;

-- Set null values to 0
UPDATE "COHORT_DEFINITION"
SET
    "SUBJECT_CONCEPT_ID" = 0
WHERE
    "SUBJECT_CONCEPT_ID" IS NULL;

-- Set DEFINITION_TYPE_CONCEPT_ID and SUBJECT_CONCEPT_ID to NOT NULL
ALTER TABLE "COHORT_DEFINITION"
ALTER ("DEFINITION_TYPE_CONCEPT_ID" INTEGER NOT NULL);

ALTER TABLE "COHORT_DEFINITION"
ALTER ("SUBJECT_CONCEPT_ID" INTEGER NOT NULL);

-- Alter cohort_initiation_date from timestamp to date
ALTER TABLE "COHORT_DEFINITION"
ALTER ("COHORT_INITIATION_DATE" DATE);

-- Drop trigger on COHORT_DEFINITION_ID
DROP TRIGGER "SET_COHORT_DEFINITION_ID";

-- Drop sequence 
DROP SEQUENCE "COHORT_DEFINITION_ID_SEQ";

-- Alter cohort table to set timestamp columns to date, and set cohort_end_date to NOT NULL
ALTER TABLE "COHORT"
ALTER ("COHORT_START_DATE" DATE NOT NULL);

ALTER TABLE "COHORT"
ALTER ("COHORT_END_DATE" DATE NOT NULL);