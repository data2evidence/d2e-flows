--liquibase formatted sql
--changeset alp:V1.0.0.0.4__apply_v5.4.sql labels:frankfurt


-- This changeset will update the OMOP tables in accordance with CDM Version 5.4
-- Please refer to http://ohdsi.github.io/CommonDataModel/cdm54Changes.html for list of changes


ALTER TABLE "VISIT_OCCURRENCE"
RENAME COLUMN "ADMITTING_SOURCE_CONCEPT_ID" TO "ADMITTED_FROM_CONCEPT_ID";

ALTER TABLE "VISIT_OCCURRENCE"
RENAME COLUMN "ADMITTING_SOURCE_VALUE" TO "ADMITTED_FROM_SOURCE_VALUE";

ALTER TABLE "VISIT_OCCURRENCE"
RENAME COLUMN "DISCHARGE_TO_CONCEPT_ID" TO "DISCHARGED_TO_CONCEPT_ID";

ALTER TABLE "VISIT_OCCURRENCE"
RENAME COLUMN "DISCHARGE_TO_SOURCE_VALUE" TO "DISCHARGED_TO_SOURCE_VALUE";


ALTER TABLE "VISIT_DETAIL"
RENAME COLUMN "ADMITTING_SOURCE_CONCEPT_ID" TO "ADMITTED_FROM_CONCEPT_ID";

ALTER TABLE "VISIT_DETAIL"
RENAME COLUMN "ADMITTING_SOURCE_VALUE" TO "ADMITTED_FROM_SOURCE_VALUE";

ALTER TABLE "VISIT_DETAIL"
RENAME COLUMN "DISCHARGE_TO_CONCEPT_ID" TO "DISCHARGED_TO_CONCEPT_ID";

ALTER TABLE "VISIT_DETAIL"
RENAME COLUMN "DISCHARGE_TO_SOURCE_VALUE" TO "DISCHARGED_TO_SOURCE_VALUE";

ALTER TABLE "VISIT_DETAIL"
RENAME COLUMN "VISIT_DETAIL_PARENT_ID" TO "PARENT_VISIT_DETAIL_ID";


ALTER TABLE "PROCEDURE_OCCURRENCE"
ADD COLUMN "PROCEDURE_END_DATE" DATE NULL,
ADD COLUMN "PROCEDURE_END_DATETIME" TIMESTAMP NULL;


ALTER TABLE "DEVICE_EXPOSURE"
ALTER COLUMN "UNIQUE_DEVICE_ID" TYPE VARCHAR(255);


ALTER TABLE "DEVICE_EXPOSURE"
ADD COLUMN "PRODUCTION_ID" VARCHAR(255) NULL,
ADD COLUMN "UNIT_CONCEPT_ID" INTEGER NULL,
ADD COLUMN "UNIT_SOURCE_VALUE" VARCHAR(50) NULL,
ADD COLUMN "UNIT_SOURCE_CONCEPT_ID" INTEGER NULL;


ALTER TABLE "MEASUREMENT"
ADD COLUMN "UNIT_SOURCE_CONCEPT_ID" INTEGER NULL,
ADD COLUMN "MEASUREMENT_EVENT_ID" INTEGER NULL,
ADD COLUMN "MEAS_EVENT_FIELD_CONCEPT_ID" INTEGER NULL;


ALTER TABLE "OBSERVATION"
ADD COLUMN "VALUE_SOURCE_VALUE" VARCHAR(50) NULL;
--ADD COLUMN "OBSERVATION_EVENT_ID" INTEGER NULL,
--ADD COLUMN "OBS_EVENT_FIELD_CONCEPT_ID" INTEGER NULL;


ALTER TABLE "NOTE"
ADD COLUMN "NOTE_EVENT_ID" INTEGER NULL,
ADD COLUMN "NOTE_EVENT_FIELD_CONCEPT_ID" INTEGER NULL;


ALTER TABLE "LOCATION"
ADD COLUMN "COUNTRY_CONCEPT_ID" INTEGER NULL,
ADD COLUMN "COUNTRY_SOURCE_VALUE" VARCHAR(80) NULL,
ADD COLUMN "LATITUDE" NUMERIC NULL,
ADD COLUMN "LONGITUDE" NUMERIC NULL;


CREATE TABLE "EPISODE" (
	"EPISODE_ID" INTEGER NOT NULL,
	"PERSON_ID" INTEGER NOT NULL,
	"EPISODE_CONCEPT_ID" INTEGER NOT NULL,
	"EPISODE_START_DATE" DATE NOT NULL,
	"EPISODE_START_DATETIME" TIMESTAMP NULL,
	"EPISODE_END_DATE" DATE NULL,
	"EPISODE_END_DATETIME" TIMESTAMP NULL,
	"EPISODE_PARENT_ID" INTEGER NULL,
	"EPISODE_NUMBER" INTEGER NULL,
	"EPISODE_OBJECT_CONCEPT_ID" INTEGER NOT NULL,
	"EPISODE_TYPE_CONCEPT_ID" INTEGER NOT NULL,
	"EPISODE_SOURCE_VALUE" VARCHAR(50) NULL,
	"EPISODE_SOURCE_CONCEPT_ID" INTEGER NULL
);


CREATE TABLE "EPISODE_EVENT" (
	"EPISODE_ID" INTEGER NOT NULL,
	"EVENT_ID" INTEGER NOT NULL,
	"EPISODE_EVENT_FIELD_CONCEPT_ID" INTEGER NOT NULL
);


ALTER TABLE "METADATA"
ADD COLUMN "METADATA_ID" INTEGER NOT NULL,
ADD COLUMN "VALUE_AS_NUMBER" NUMERIC NULL;


ALTER TABLE "CDM_SOURCE"
ADD COLUMN "CDM_VERSION_CONCEPT_ID" INTEGER NOT NULL DEFAULT 0;


ALTER TABLE "CDM_SOURCE"
ALTER COLUMN "CDM_SOURCE_NAME" SET NOT NULL,
ALTER COLUMN "CDM_SOURCE_ABBREVIATION" SET NOT NULL,
ALTER COLUMN "CDM_HOLDER" SET NOT NULL,
ALTER COLUMN "SOURCE_RELEASE_DATE" SET NOT NULL,
ALTER COLUMN "CDM_RELEASE_DATE" SET NOT NULL;


ALTER TABLE "VOCABULARY"
ALTER COLUMN "VOCABULARY_REFERENCE" DROP NOT NULL,
ALTER COLUMN "VOCABULARY_VERSION" DROP NOT NULL;


/*
DROP TABLE IF EXISTS "ATTRIBUTE_DEFINITION";


CREATE TABLE "COHORT" (
    "COHORT_DEFINITION_ID" INTEGER NOT NULL,
    "SUBJECT_ID" INTEGER NOT NULL,
    "COHORT_START_DATE" DATE NOT NULL,
    "COHORT_END_DATE" DATE NOT NULL
);


CREATE TABLE "COHORT_DEFINITION" (
	"COHORT_DEFINITION_ID" INTEGER NOT NULL,
	"COHORT_DEFINITION_NAME" VARCHAR(255) NOT NULL,
	"COHORT_DEFINITION_DESCRIPTION" TEXT NULL,
	"DEFINITION_TYPE_CONCEPT_ID" INTEGER NOT NULL,
	"COHORT_DEFINITION_SYNTAX" TEXT NULL,
	"SUBJECT_CONCEPT_ID INTEGER" NOT NULL,
	"COHORT_INITIATION_DATE" DATE NULL 
);
*/


--rollback ALTER TABLE "VISIT_OCCURRENCE" RENAME COLUMN "ADMITTED_FROM_CONCEPT_ID" TO "ADMITTING_SOURCE_CONCEPT_ID";
--rollback ALTER TABLE "VISIT_OCCURRENCE" RENAME COLUMN "ADMITTED_FROM_SOURCE_VALUE" TO "ADMITTING_SOURCE_VALUE";
--rollback ALTER TABLE "VISIT_OCCURRENCE" RENAME COLUMN "DISCHARGED_TO_CONCEPT_ID" TO "DISCHARGE_TO_CONCEPT_ID";
--rollback ALTER TABLE "VISIT_OCCURRENCE" RENAME COLUMN "DISCHARGED_TO_SOURCE_VALUE" TO "DISCHARGE_TO_SOURCE_VALUE";

--rollback ALTER TABLE "VISIT_DETAIL" RENAME COLUMN "ADMITTED_FROM_CONCEPT_ID" TO "ADMITTING_SOURCE_CONCEPT_ID";
--rollback ALTER TABLE "VISIT_DETAIL" RENAME COLUMN "ADMITTED_FROM_SOURCE_VALUE" TO "ADMITTING_SOURCE_VALUE";
--rollback ALTER TABLE "VISIT_DETAIL" RENAME COLUMN "DISCHARGED_TO_CONCEPT_ID" TO "DISCHARGE_TO_CONCEPT_ID";
--rollback ALTER TABLE "VISIT_DETAIL" RENAME COLUMN "DISCHARGED_TO_SOURCE_VALUE" TO "DISCHARGE_TO_SOURCE_VALUE";
--rollback ALTER TABLE "VISIT_DETAIL" RENAME COLUMN "PARENT_VISIT_DETAIL_ID" TO "VISIT_DETAIL_PARENT_ID";

--rollback ALTER TABLE "PROCEDURE_OCCURRENCE" DROP COLUMN "PROCEDURE_END_DATE";
--rollback ALTER TABLE "PROCEDURE_OCCURRENCE" DROP COLUMN "PROCEDURE_END_DATETIME";

--rollback ALTER TABLE "DEVICE_EXPOSURE" DROP COLUMN "PRODUCTION_ID";
--rollback ALTER TABLE "DEVICE_EXPOSURE" DROP COLUMN "UNIT_CONCEPT_ID";
--rollback ALTER TABLE "DEVICE_EXPOSURE" DROP COLUMN "UNIT_SOURCE_VALUE";
--rollback ALTER TABLE "DEVICE_EXPOSURE" DROP COLUMN "UNIT_SOURCE_CONCEPT_ID";

--rollback ALTER TABLE "MEASUREMENT" DROP COLUMN "UNIT_SOURCE_CONCEPT_ID";
--rollback ALTER TABLE "MEASUREMENT" DROP COLUMN "MEASUREMENT_EVENT_ID";
--rollback ALTER TABLE "MEASUREMENT" DROP COLUMN "MEAS_EVENT_FIELD_CONCEPT_ID";

--rollback ALTER TABLE "OBSERVATION" DROP COLUMN "VALUE_SOURCE_VALUE";

--rollback ALTER TABLE "NOTE" DROP COLUMN "NOTE_EVENT_ID";
--rollback ALTER TABLE "NOTE" DROP COLUMN "NOTE_EVENT_FIELD_CONCEPT_ID";

--rollback ALTER TABLE "LOCATION" DROP COLUMN "COUNTRY_CONCEPT_ID";
--rollback ALTER TABLE "LOCATION" DROP COLUMN "COUNTRY_SOURCE_VALUE";
--rollback ALTER TABLE "LOCATION" DROP COLUMN "LATITUDE";
--rollback ALTER TABLE "LOCATION" DROP COLUMN "LONGITUDE";

--rollback DROP TABLE "EPISODE";
--rollback DROP TABLE "EPISODE_EVENT";

--rollback ALTER TABLE "METADATA" DROP COLUMN "METADATA_ID";
--rollback ALTER TABLE "METADATA" DROP COLUMN "VALUE_AS_NUMBER";

--rollback ALTER TABLE "CDM_SOURCE" DROP COLUMN "CDM_VERSION_CONCEPT_ID";

--rollback ALTER TABLE "CDM_SOURCE" ALTER COLUMN "CDM_SOURCE_NAME" DROP NOT NULL
--rollback ALTER TABLE "CDM_SOURCE" ALTER COLUMN "CDM_SOURCE_ABBREVIATION" DROP NOT NULL
--rollback ALTER TABLE "CDM_SOURCE" ALTER COLUMN "CDM_HOLDER" DROP NOT NULL
--rollback ALTER TABLE "CDM_SOURCE" ALTER COLUMN "SOURCE_RELEASE_DATE" DROP NOT NULL
--rollback ALTER TABLE "CDM_SOURCE" ALTER COLUMN "CDM_RELEASE_DATE" DROP NOT NULL

--rollback ALTER TABLE "VOCABULARY" ALTER COLUMN "VOCABULARY_REFERENCE" SET NOT NULL
--rollback ALTER TABLE "VOCABULARY" ALTER COLUMN "VOCABULARY_VERSION" SET NOT NULL