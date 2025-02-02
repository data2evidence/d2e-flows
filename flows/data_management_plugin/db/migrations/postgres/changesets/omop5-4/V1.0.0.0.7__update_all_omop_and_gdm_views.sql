--liquibase formatted sql
--changeset alp:V1.0.0.0.7__update_all_omop_and_gdm_views
-- preconditions onFail:MARK_RAN 
-- precondition-sql-check expectedResult:1 select COUNT(*) from information_schema.tables where table_schema = 'cdmvocab' and table_name = 'CONCEPT'
-- comment: /* Checks if cdmvocab is created in upper case or lower case. Marks as ran if cdmvocab is in lower case. */

-- Drop all views
DROP VIEW IF EXISTS "VIEW::OMOP.OBS";
DROP VIEW IF EXISTS "VIEW::OMOP.PROC";
DROP VIEW IF EXISTS "VIEW::OMOP.DRUG_EXP";
DROP VIEW IF EXISTS "VIEW::OMOP.COND";
DROP VIEW IF EXISTS "VIEW::OMOP.COND_ERA";
DROP VIEW IF EXISTS "VIEW::OMOP.COND_ICD10";
DROP VIEW IF EXISTS "VIEW::OMOP.PP_PER";
DROP VIEW IF EXISTS "VIEW::OMOP.OBS_PER";
DROP VIEW IF EXISTS "VIEW::OMOP.SPEC";
DROP VIEW IF EXISTS "VIEW::OMOP.VISIT";
DROP VIEW IF EXISTS "VIEW::OMOP.DRUG_ERA";
DROP VIEW IF EXISTS "VIEW::OMOP.DOSE_ERA";
DROP VIEW IF EXISTS "VIEW::OMOP.DEVICE_EXPOSURE";
DROP VIEW IF EXISTS "VIEW::OMOP.DEATH";
DROP VIEW IF EXISTS "VIEW::OMOP.PATIENT" CASCADE; -- This will drop "VIEW::OMOP.GDM.PATIENT" in ResearchSubject changeset
DROP VIEW IF EXISTS "VIEW::OMOP.CONCEPT";

-- Recreate all OMOP views
CREATE OR REPLACE VIEW "VIEW::OMOP.CONCEPT" AS SELECT * FROM "cdmvocab"."CONCEPT";

CREATE OR REPLACE VIEW "VIEW::OMOP.PATIENT" (
  "PATIENT_ID",
  "BIRTH_DATE",
  "MONTH_OF_BIRTH",
  "YEAR_OF_BIRTH",
  "DEATH_DATE",
  "GENDER",
  "RACE",
  "ETHNICITY",
  "STATE",
  "COUNTY",
  "GENDER_CONCEPT_CODE",
  "RACE_CONCEPT_CODE",
  "ETHNICITY_CONCEPT_CODE",
  "GENDER_CONCEPT_ID",
  "RACE_CONCEPT_ID",
  "ETHNICITY_CONCEPT_ID"
  ) AS (
  SELECT
    "p_$0"."PERSON_ID" AS "PATIENT_ID",
    TO_DATE (
      (
        (TO_CHAR("p_$0"."YEAR_OF_BIRTH", '0000') || TO_CHAR("p_$0"."MONTH_OF_BIRTH", '00')) || 
        TO_CHAR("p_$0"."DAY_OF_BIRTH", '00')
      ), 'yyyymmdd') AS "BIRTH_DATE",
    "p_$0"."MONTH_OF_BIRTH",
    "p_$0"."YEAR_OF_BIRTH",
    COALESCE ("d_$1"."DEATH_DATE", NULL) AS "DEATH_DATE",
    "gender_c_$2"."CONCEPT_NAME" AS "GENDER",
    "race_c_$3"."CONCEPT_NAME" AS "RACE",
    "ethnicity_c_$4"."CONCEPT_NAME" AS "ETHNICITY",
    "l_$5"."STATE" AS "STATE",
    "l_$5"."COUNTY" AS "COUNTY",
    "gender_c_$2"."CONCEPT_CODE" AS "GENDER_CONCEPT_CODE",
    "race_c_$3"."CONCEPT_CODE" AS "RACE_CONCEPT_CODE",
    "ethnicity_c_$4"."CONCEPT_CODE" AS "ETHNICITY_CONCEPT_CODE",
    "gender_c_$2"."CONCEPT_ID" AS "GENDER_CONCEPT_ID",
    "race_c_$3"."CONCEPT_ID" AS "RACE_CONCEPT_ID",
    "ethnicity_c_$4"."CONCEPT_ID" AS "ETHNICITY_CONCEPT_ID"
  FROM
    (
      (
        (
          (
            (
              "PERSON" AS "p_$0"
              LEFT OUTER JOIN "DEATH" AS "d_$1" ON("d_$1"."PERSON_ID" = "p_$0"."PERSON_ID")
            )
            LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "gender_c_$2" ON(
              "gender_c_$2"."CONCEPT_ID" = "p_$0"."GENDER_CONCEPT_ID"
            )
          )
          LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "race_c_$3" ON(
            "race_c_$3"."CONCEPT_ID" = "p_$0"."RACE_CONCEPT_ID"
          )
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "ethnicity_c_$4" ON(
            "ethnicity_c_$4"."CONCEPT_ID" = "p_$0"."ETHNICITY_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "LOCATION" AS "l_$5" ON("l_$5"."LOCATION_ID" = "p_$0"."LOCATION_ID")
    )
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.OBS" (
    "OBSERVATION_ID",
    "PATIENT_ID",
    "OBSERVATION_CONCEPT_ID",
    "OBSERVATION_NAME",
    "OBSERVATION_DATE",
    "OBSERVATION_DATETIME",
    "OBSERVATION_TYPE_CONCEPT_ID",
    "OBSERVATION_TYPE_NAME",
    "VALUE_AS_NUMBER",
    "VALUE_AS_STRING",
    "VALUE_AS_CONCEPT_ID",
    "VALUE_NAME",
    "QUALIFIER_CONCEPT_ID",
    "QUALIFIER_NAME",
    "UNIT_CONCEPT_ID",
    "UNIT_NAME",
    "PROVIDER_ID",
    "VISIT_OCCURRENCE_ID",
    "OBSERVATION_SOURCE_VALUE",
    "OBSERVATION_SOURCE_CONCEPT_ID",
    "UNIT_SOURCE_VALUE",
    "QUALIFIER_SOURCE_VALUE",
    "OBSERVATION_CONCEPT_CODE",
    "OBSERVATION_TYPE_CONCEPT_CODE",
    "VALUE_AS_CONCEPT_CODE",
    "QUALIFIER_CONCEPT_CODE",
    "UNIT_CONCEPT_CODE",
    "OBSERVATION_EVENT_ID",
    "OBS_EVENT_FIELD_CONCEPT_ID"
  ) AS (
  SELECT
    "o_$0"."OBSERVATION_ID",
    "o_$0"."PERSON_ID" AS "PATIENT_ID",
    "o_$0"."OBSERVATION_CONCEPT_ID",
    "c_$1"."CONCEPT_NAME" AS "OBSERVATION_NAME",
    "o_$0"."OBSERVATION_DATE",
    "o_$0"."OBSERVATION_DATETIME",
    "o_$0"."OBSERVATION_TYPE_CONCEPT_ID",
    "t_$2"."CONCEPT_NAME" AS "OBSERVATION_TYPE_NAME",
    "o_$0"."VALUE_AS_NUMBER",
    "o_$0"."VALUE_AS_STRING",
    "o_$0"."VALUE_AS_CONCEPT_ID",
    "v_$3"."CONCEPT_NAME" AS "VALUE_NAME",
    "o_$0"."QUALIFIER_CONCEPT_ID",
    "q_$4"."CONCEPT_NAME" AS "QUALIFIER_NAME",
    "o_$0"."UNIT_CONCEPT_ID",
    "u_$5"."CONCEPT_NAME" AS "UNIT_NAME",
    "o_$0"."PROVIDER_ID",
    "o_$0"."VISIT_OCCURRENCE_ID",
    "o_$0"."OBSERVATION_SOURCE_VALUE",
    "o_$0"."OBSERVATION_SOURCE_CONCEPT_ID",
    "o_$0"."UNIT_SOURCE_VALUE",
    "o_$0"."QUALIFIER_SOURCE_VALUE",
    "c_$1"."CONCEPT_CODE" AS "OBSERVATION_CONCEPT_CODE",
    "t_$2"."CONCEPT_CODE" AS "OBSERVATION_TYPE_CONCEPT_CODE",
    "v_$3"."CONCEPT_CODE" AS "VALUE_AS_CONCEPT_CODE",
    "q_$4"."CONCEPT_CODE" AS "QUALIFIER_CONCEPT_CODE",
    "u_$5"."CONCEPT_CODE" AS "UNIT_CONCEPT_CODE",
    "o_$0"."OBSERVATION_EVENT_ID",
    "o_$0"."OBS_EVENT_FIELD_CONCEPT_ID"
  FROM
    (
      (
        (
          (
            (
              "OBSERVATION" AS "o_$0"
              LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "c_$1" ON (
                "c_$1"."CONCEPT_ID" = "o_$0"."OBSERVATION_CONCEPT_ID"
              )
            )
            LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "t_$2" ON (
              "t_$2"."CONCEPT_ID" = "o_$0"."OBSERVATION_TYPE_CONCEPT_ID"
            )
          )
          LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "v_$3" ON (
            "v_$3"."CONCEPT_ID" = "o_$0"."VALUE_AS_CONCEPT_ID"
          )
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "q_$4" ON (
          "q_$4"."CONCEPT_ID" = "o_$0"."QUALIFIER_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "u_$5" ON ("u_$5"."CONCEPT_ID" = "o_$0"."UNIT_CONCEPT_ID")
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.PROC" (
    "PROCEDURE_OCCURRENCE_ID",
    "PATIENT_ID",
    "PROCEDURE_CONCEPT_ID",
    "PROCEDURE_NAME",
    "PROCEDURE_DATE",
    "PROCEDURE_DATETIME",
    "PROCEDURE_TYPE_CONCEPT_ID",
    "PROCEDURE_TYPE_NAME",
    "MODIFIER_CONCEPT_ID",
    "MODIFIER_NAME",
    "QUANTITY",
    "PROVIDER_ID",
    "VISIT_OCCURRENCE_ID",
    "PROCEDURE_SOURCE_VALUE",
    "PROCEDURE_SOURCE_CONCEPT_ID",
    "MODIFIER_SOURCE_VALUE",
    "PROCEDURE_CONCEPT_CODE",
    "PROCEDURE_TYPE_CONCEPT_CODE",
    "MODIFIER_CONCEPT_CODE"
  ) AS (
  SELECT
    "p_$0"."PROCEDURE_OCCURRENCE_ID",
    "p_$0"."PERSON_ID" AS "PATIENT_ID",
    "p_$0"."PROCEDURE_CONCEPT_ID",
    "c_$1"."CONCEPT_NAME" AS "PROCEDURE_NAME",
    "p_$0"."PROCEDURE_DATE",
    "p_$0"."PROCEDURE_DATETIME",
    "p_$0"."PROCEDURE_TYPE_CONCEPT_ID",
    "t_$2"."CONCEPT_NAME" AS "PROCEDURE_TYPE_NAME",
    "p_$0"."MODIFIER_CONCEPT_ID",
    "m_$3"."CONCEPT_NAME" AS "MODIFIER_NAME",
    "p_$0"."QUANTITY",
    "p_$0"."PROVIDER_ID",
    "p_$0"."VISIT_OCCURRENCE_ID",
    "p_$0"."PROCEDURE_SOURCE_VALUE",
    "p_$0"."PROCEDURE_SOURCE_CONCEPT_ID",
    "p_$0"."MODIFIER_SOURCE_VALUE",
    "c_$1"."CONCEPT_CODE" AS "PROCEDURE_CONCEPT_CODE",
    "t_$2"."CONCEPT_CODE" AS "PROCEDURE_TYPE_CONCEPT_CODE",
    "m_$3"."CONCEPT_CODE" AS "MODIFIER_CONCEPT_CODE"
  FROM
    (
      (
        (
          "PROCEDURE_OCCURRENCE" AS "p_$0"
          LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "c_$1" ON (
            "c_$1"."CONCEPT_ID" = "p_$0"."PROCEDURE_CONCEPT_ID"
          )
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "t_$2" ON (
          "t_$2"."CONCEPT_ID" = "p_$0"."PROCEDURE_TYPE_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "m_$3" ON (
        "m_$3"."CONCEPT_ID" = "p_$0"."MODIFIER_CONCEPT_ID"
      )
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.DRUG_EXP" (
    "DRUG_EXPOSURE_ID",
    "PATIENT_ID",
    "DRUG_CONCEPT_ID",
    "DRUG_NAME",
    "DRUG_EXPOSURE_START_DATE",
    "DRUG_EXPOSURE_END_DATE",
    "DRUG_EXPOSURE_START_DATETIME",
    "DRUG_EXPOSURE_END_DATETIME",
    "VERBATIM_END_DATE",
    "DRUG_TYPE_CONCEPT_ID",
    "DRUG_TYPE_NAME",
    "STOP_REASON",
    "REFILLS",
    "DAYS_SUPPLY",
    "SIG",
    "ROUTE_CONCEPT_ID",
    "ROUTE_NAME",
    "LOT_NUMBER",
    "PROVIDER_ID",
    "VISIT_OCCURRENCE_ID",
    "DRUG_CONCEPT_CODE",
    "DRUG_TYPE_CONCEPT_CODE",
    "ROUTE_CONCEPT_CODE"
  ) AS (
  SELECT
    "de_$0"."DRUG_EXPOSURE_ID",
    "p_$1"."PATIENT_ID",
    "de_$0"."DRUG_CONCEPT_ID",
    "c_$2"."CONCEPT_NAME" AS "DRUG_NAME",
    "de_$0"."DRUG_EXPOSURE_START_DATE",
    "de_$0"."DRUG_EXPOSURE_END_DATE",
    "de_$0"."DRUG_EXPOSURE_START_DATETIME",
    "de_$0"."DRUG_EXPOSURE_END_DATETIME",
    "de_$0"."VERBATIM_END_DATE",
    "de_$0"."DRUG_TYPE_CONCEPT_ID",
    "ct_$3"."CONCEPT_NAME" AS "DRUG_TYPE_NAME",
    "de_$0"."STOP_REASON",
    "de_$0"."REFILLS",
    "de_$0"."DAYS_SUPPLY",
    "de_$0"."SIG",
    "de_$0"."ROUTE_CONCEPT_ID",
    "r_$4"."CONCEPT_NAME" AS "ROUTE_NAME",
    "de_$0"."LOT_NUMBER",
    "de_$0"."PROVIDER_ID",
    "de_$0"."VISIT_OCCURRENCE_ID",
    "c_$2"."CONCEPT_CODE" AS "DRUG_CONCEPT_CODE",
    "ct_$3"."CONCEPT_CODE" AS "DRUG_TYPE_CONCEPT_CODE",
    "r_$4"."CONCEPT_CODE" AS "ROUTE_CONCEPT_CODE"
  FROM
    (
      (
        (
          (
            "DRUG_EXPOSURE" AS "de_$0"
            INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("de_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
          )
          LEFT JOIN "CONCEPT" AS "c_$2" ON (
            "c_$2"."CONCEPT_ID" = "de_$0"."DRUG_CONCEPT_ID"
          )
        )
        LEFT JOIN "CONCEPT" AS "ct_$3" ON (
          "ct_$3"."CONCEPT_ID" = "de_$0"."DRUG_TYPE_CONCEPT_ID"
        )
      )
      LEFT JOIN "CONCEPT" AS "r_$4" ON (
        "r_$4"."CONCEPT_ID" = "de_$0"."ROUTE_CONCEPT_ID"
      )
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.COND" (
    "CONDITION_OCCURRENCE_ID",
    "CONDITION_CONCEPT_ID",
    "CONDITION_NAME",
    "CONDITION_TYPE_NAME",
    "CONDITION_SOURCE_NAME",
    "CONDITION_STATUS_NAME",
    "PATIENT_ID",
    "CONDITION_START_DATE",
    "CONDITION_END_DATE",
    "VISIT_OCCURRENCE_ID",
    "CONDITION_SOURCE_VALUE",
    "CONDITION_CONCEPT_CODE",
    "CONDITION_TYPE_CONCEPT_CODE",
    "CONDITION_SOURCE_CONCEPT_CODE",
    "CONDITION_STATUS_CONCEPT_CODE"
  ) AS (
  SELECT
    "co_$0"."CONDITION_OCCURRENCE_ID",
    "co_$0"."CONDITION_CONCEPT_ID",
    "c_$2"."CONCEPT_NAME" AS "CONDITION_NAME",
    "t_$3"."CONCEPT_NAME" AS "CONDITION_TYPE_NAME",
    "s_$4"."CONCEPT_NAME" AS "CONDITION_SOURCE_NAME",
    "cs_$5"."CONCEPT_NAME" AS "CONDITION_STATUS_NAME",
    "p_$1"."PATIENT_ID",
    "co_$0"."CONDITION_START_DATE",
    "co_$0"."CONDITION_END_DATE",
    "co_$0"."VISIT_OCCURRENCE_ID",
    "co_$0"."CONDITION_SOURCE_VALUE",
    "c_$2"."CONCEPT_CODE" AS "CONDITION_CONCEPT_CODE",
    "t_$3"."CONCEPT_CODE" AS "CONDITION_TYPE_CONCEPT_CODE",
    "s_$4"."CONCEPT_CODE" AS "CONDITION_SOURCE_CONCEPT_CODE",
    "cs_$5"."CONCEPT_CODE" AS "CONDITION_STATUS_CONCEPT_CODE"
  FROM
    (
      (
        (
          (
            (
              "CONDITION_OCCURRENCE" AS "co_$0"
              INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("co_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
            )
            LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
              "c_$2"."CONCEPT_ID" = "co_$0"."CONDITION_CONCEPT_ID"
            )
          )
          LEFT JOIN "VIEW::OMOP.CONCEPT" AS "t_$3" ON (
            "t_$3"."CONCEPT_ID" = "co_$0"."CONDITION_TYPE_CONCEPT_ID"
          )
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "s_$4" ON (
          "s_$4"."CONCEPT_ID" = "co_$0"."CONDITION_SOURCE_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "cs_$5" ON (
        "cs_$5"."CONCEPT_ID" = "co_$0"."CONDITION_STATUS_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.COND_ERA" (
    "CONDITION_ERA_ID",
    "CONDITION_CONCEPT_ID",
    "CONDITION_NAME",
    "PATIENT_ID",
    "CONDITION_ERA_START_DATE",
    "CONDITION_ERA_END_DATE",
    "CONDITION_OCCURRENCE_COUNT",
    "CONDITION_CONCEPT_CODE"
  ) AS (
  SELECT
      "ce_$0"."CONDITION_ERA_ID",
      "ce_$0"."CONDITION_CONCEPT_ID",
      "c_$2"."CONCEPT_NAME" AS "CONDITION_NAME",
      "p_$1"."PATIENT_ID",
      "ce_$0"."CONDITION_ERA_START_DATE",
      "ce_$0"."CONDITION_ERA_END_DATE",
      "ce_$0"."CONDITION_OCCURRENCE_COUNT",
      "c_$2"."CONCEPT_CODE" AS "CONDITION_CONCEPT_CODE"
    FROM
      (
        (
          "CONDITION_ERA" AS "ce_$0"
          INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("ce_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
        )
        LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
          "c_$2"."CONCEPT_ID" = "ce_$0"."CONDITION_CONCEPT_ID"
        )
      ) 
    );

CREATE OR REPLACE VIEW "VIEW::OMOP.OBS_PER" (
    "OBSERVATION_PERIOD_ID",
    "PATIENT_ID",
    "OBSERVATION_PERIOD_START_DATE",
    "OBSERVATION_PERIOD_END_DATE",
    "PERIOD_TYPE_CONCEPT_ID",
    "PERIOD_TYPE_NAME",
    "PERIOD_TYPE_CONCEPT_CODE"
  ) AS (
  SELECT
    "o_$0"."OBSERVATION_PERIOD_ID",
    "o_$0"."PERSON_ID" AS "PATIENT_ID",
    "o_$0"."OBSERVATION_PERIOD_START_DATE",
    "o_$0"."OBSERVATION_PERIOD_END_DATE",
    "o_$0"."PERIOD_TYPE_CONCEPT_ID",
    "t_$1"."CONCEPT_NAME" AS "PERIOD_TYPE_NAME",
    "t_$1"."CONCEPT_CODE" AS "PERIOD_TYPE_CONCEPT_CODE"
  FROM
    (
      "OBSERVATION_PERIOD" AS "o_$0"
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "t_$1" ON (
        "t_$1"."CONCEPT_ID" = "o_$0"."PERIOD_TYPE_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.SPEC" (
    "SPECIMEN_ID",
    "PATIENT_ID",
    "SPECIMEN_CONCEPT_ID",
    "SPECIMEN_NAME",
    "SPECIMEN_TYPE_CONCEPT_ID",
    "SPECIMEN_TYPE_NAME",
    "SPECIMEN_DATE",
    "SPECIMEN_DATETIME",
    "QUANTITY",
    "UNIT_CONCEPT_ID",
    "UNIT_NAME",
    "ANATOMIC_SITE_CONCEPT_ID",
    "ANATOMIC_SITE_NAME",
    "DISEASE_STATUS_CONCEPT_ID",
    "DISEASE_STATUS_NAME",
    "SPECIMEN_SOURCE_ID",
    "SPECIMEN_SOURCE_VALUE",
    "UNIT_SOURCE_VALUE",
    "ANATOMIC_SITE_SOURCE_VALUE",
    "DISEASE_STATUS_SOURCE_VALUE",
    "SPECIMEN_CONCEPT_CODE",
    "SPECIMEN_TYPE_CONCEPT_CODE",
    "UNIT_CONCEPT_CODE",
    "ANATOMIC_SITE_CONCEPT_CODE",
    "DISEASE_STATUS_CONCEPT_CODE"
  ) AS (
  SELECT
    "s_$0"."SPECIMEN_ID",
    "s_$0"."PERSON_ID" AS "PATIENT_ID",
    "s_$0"."SPECIMEN_CONCEPT_ID",
    "c_$1"."CONCEPT_NAME" AS "SPECIMEN_NAME",
    "s_$0"."SPECIMEN_TYPE_CONCEPT_ID",
    "t_$2"."CONCEPT_NAME" AS "SPECIMEN_TYPE_NAME",
    "s_$0"."SPECIMEN_DATE",
    "s_$0"."SPECIMEN_DATETIME",
    "s_$0"."QUANTITY",
    "s_$0"."UNIT_CONCEPT_ID",
    "u_$3"."CONCEPT_NAME" AS "UNIT_NAME",
    "s_$0"."ANATOMIC_SITE_CONCEPT_ID",
    "a_$4"."CONCEPT_NAME" AS "ANATOMIC_SITE_NAME",
    "s_$0"."DISEASE_STATUS_CONCEPT_ID",
    "d_$5"."CONCEPT_NAME" AS "DISEASE_STATUS_NAME",
    "s_$0"."SPECIMEN_SOURCE_ID",
    "s_$0"."SPECIMEN_SOURCE_VALUE",
    "s_$0"."UNIT_SOURCE_VALUE",
    "s_$0"."ANATOMIC_SITE_SOURCE_VALUE",
    "s_$0"."DISEASE_STATUS_SOURCE_VALUE",
    "c_$1"."CONCEPT_CODE" AS "SPECIMEN_CONCEPT_CODE",
    "t_$2"."CONCEPT_CODE" AS "SPECIMEN_TYPE_CONCEPT_CODE",
    "u_$3"."CONCEPT_CODE" AS "UNIT_CONCEPT_CODE",
    "a_$4"."CONCEPT_CODE" AS "ANATOMIC_SITE_CONCEPT_CODE",
    "d_$5"."CONCEPT_CODE" AS "DISEASE_STATUS_CONCEPT_CODE"
  FROM
    (
      (
        (
          (
            (
              "SPECIMEN" AS "s_$0"
              LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "c_$1" ON (
                "c_$1"."CONCEPT_ID" = "s_$0"."SPECIMEN_CONCEPT_ID"
              )
            )
            LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "t_$2" ON (
              "t_$2"."CONCEPT_ID" = "s_$0"."SPECIMEN_TYPE_CONCEPT_ID"
            )
          )
          LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "u_$3" ON ("u_$3"."CONCEPT_ID" = "s_$0"."UNIT_CONCEPT_ID")
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "a_$4" ON (
          "a_$4"."CONCEPT_ID" = "s_$0"."ANATOMIC_SITE_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "d_$5" ON (
        "d_$5"."CONCEPT_ID" = "s_$0"."DISEASE_STATUS_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.VISIT" (
    "VISIT_OCCURRENCE_ID",
    "VISIT_CONCEPT_ID",
    "VISIT_NAME",
    "VISIT_TYPE_NAME",
    "PATIENT_ID",
    "VISIT_START_DATE",
    "PRECEDING_VISIT_OCCURRENCE_ID",
    "VISIT_END_DATE",
    "VISIT_CONCEPT_CODE",
    "VISIT_TYPE_CONCEPT_CODE"
  ) AS (
  SELECT
    "vo_$0"."VISIT_OCCURRENCE_ID",
    "vo_$0"."VISIT_CONCEPT_ID",
    "c_$2"."CONCEPT_NAME" AS "VISIT_NAME",
    "t_$3"."CONCEPT_NAME" AS "VISIT_TYPE_NAME",
    "p_$1"."PATIENT_ID",
    "vo_$0"."VISIT_START_DATE",
    "vo_$0"."PRECEDING_VISIT_OCCURRENCE_ID",
    "vo_$0"."VISIT_END_DATE",
    "c_$2"."CONCEPT_CODE" AS "VISIT_CONCEPT_CODE",
    "t_$3"."CONCEPT_CODE" AS "VISIT_TYPE_CONCEPT_CODE"
  FROM
    (
      (
        (
          "VISIT_OCCURRENCE" AS "vo_$0"
          INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("vo_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
        )
        LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
          "c_$2"."CONCEPT_ID" = "vo_$0"."VISIT_CONCEPT_ID"
        )
      )
      LEFT JOIN "VIEW::OMOP.CONCEPT" AS "t_$3" ON (
        "t_$3"."CONCEPT_ID" = "vo_$0"."VISIT_TYPE_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.MEAS" (
    "MEASUREMENT_ID",
    "MEASUREMENT_CONCEPT_ID",
    "MEASUREMENT_NAME",
    "MEASUREMENT_TYPE_NAME",
    "MEASUREMENT_VALUE_NAME",
    "PATIENT_ID",
    "MEASUREMENT_DATE",
    "VALUE_AS_NUMBER",
    "VISIT_OCCURRENCE_ID",
    "MEASUREMENT_CONCEPT_CODE",
    "MEASUREMENT_TYPE_CONCEPT_CODE",
    "VALUE_AS_CONCEPT_CODE",
    "UNIT_CONCEPT_CODE"
  ) AS (
  SELECT
    "m_$0"."MEASUREMENT_ID",
    "m_$0"."MEASUREMENT_CONCEPT_ID",
    "c_$2"."CONCEPT_NAME" AS "MEASUREMENT_NAME",
    "t_$3"."CONCEPT_NAME" AS "MEASUREMENT_TYPE_NAME",
    "vt_$4"."CONCEPT_NAME" AS "MEASUREMENT_VALUE_NAME",
    "p_$1"."PATIENT_ID",
    "m_$0"."MEASUREMENT_DATE",
    "m_$0"."VALUE_AS_NUMBER",
    "m_$0"."VISIT_OCCURRENCE_ID",
    "c_$2"."CONCEPT_CODE" AS "MEASUREMENT_CONCEPT_CODE",
    "t_$3"."CONCEPT_CODE" AS "MEASUREMENT_TYPE_CONCEPT_CODE",
    "vt_$4"."CONCEPT_CODE" AS "VALUE_AS_CONCEPT_CODE",
    "u_$5"."CONCEPT_CODE" AS "UNIT_CONCEPT_CODE"
  FROM
    (
      (
        (
          (
            (
              "MEASUREMENT" AS "m_$0"
              INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("m_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
            )
            LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
              "c_$2"."CONCEPT_ID" = "m_$0"."MEASUREMENT_CONCEPT_ID"
            )
          )
          LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "t_$3" ON (
            "t_$3"."CONCEPT_ID" = "m_$0"."MEASUREMENT_TYPE_CONCEPT_ID"
          )
        )
        LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "vt_$4" ON (
          "vt_$4"."CONCEPT_ID" = "m_$0"."VALUE_AS_CONCEPT_ID"
        )
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "u_$5" ON ("u_$5"."CONCEPT_ID" = "m_$0"."UNIT_CONCEPT_ID")
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.DRUG_ERA" (
    "DRUG_ERA_ID",
    "PATIENT_ID",
    "DRUG_NAME",
    "DRUG_ERA_START_DATE",
    "DRUG_ERA_END_DATE",
    "DRUG_EXPOSURE_COUNT",
    "GAP_DAYS",
    "DRUG_CONCEPT_CODE"
  ) AS (
  SELECT
    "de_$0"."DRUG_ERA_ID",
    "p_$1"."PATIENT_ID",
    "c_$2"."CONCEPT_NAME" AS "DRUG_NAME",
    "de_$0"."DRUG_ERA_START_DATE",
    "de_$0"."DRUG_ERA_END_DATE",
    "de_$0"."DRUG_EXPOSURE_COUNT",
    "de_$0"."GAP_DAYS",
    "c_$2"."CONCEPT_CODE" AS "DRUG_CONCEPT_CODE"
  FROM
    (
      (
        "DRUG_ERA" AS "de_$0"
        INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("de_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
      )
      LEFT OUTER JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
        "c_$2"."CONCEPT_ID" = "de_$0"."DRUG_CONCEPT_ID"
      )
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.DOSE_ERA" (
    "DOSE_ERA_ID",
    "PATIENT_ID",
    "DRUG_NAME",
    "UNIT_NAME",
    "DOSE_VALUE",
    "DOSE_ERA_START_DATE",
    "DOSE_ERA_END_DATE",
    "DRUG_CONCEPT_CODE",
    "UNIT_CONCEPT_CODE"
  ) AS (
  SELECT
    "de_$0"."DOSE_ERA_ID",
    "p_$1"."PATIENT_ID",
    "c_$2"."CONCEPT_NAME" AS "DRUG_NAME",
    "u_$3"."CONCEPT_NAME" AS "UNIT_NAME",
    "de_$0"."DOSE_VALUE",
    "de_$0"."DOSE_ERA_START_DATE",
    "de_$0"."DOSE_ERA_END_DATE",
    "c_$2"."CONCEPT_CODE" AS "DRUG_CONCEPT_CODE",
    "u_$3"."CONCEPT_CODE" AS "UNIT_CONCEPT_CODE"
  FROM
    (
      (
        (
          "DOSE_ERA" AS "de_$0"
          INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("de_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
        )
        LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
          "c_$2"."CONCEPT_ID" = "de_$0"."DRUG_CONCEPT_ID"
        )
      )
      LEFT JOIN "VIEW::OMOP.CONCEPT" AS "u_$3" ON (
        "u_$3"."CONCEPT_ID" = "de_$0"."UNIT_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.DEVICE_EXPOSURE" (
    "DEVICE_EXPOSURE_ID",
    "PATIENT_ID",
    "DEVICE_NAME",
    "DEVICE_CONCEPT_ID",
    "DEVICE_EXPOSURE_START_DATE",
    "DEVICE_EXPOSURE_END_DATE",
    "DEVICE_TYPE_NAME",
    "VISIT_OCCURRENCE_ID",
    "DEVICE_CONCEPT_CODE",
    "DEVICE_TYPE_CONCEPT_CODE"
  ) AS (
  SELECT
    "de_$0"."DEVICE_EXPOSURE_ID",
    "p_$1"."PATIENT_ID",
    "c_$2"."CONCEPT_NAME" AS "DEVICE_NAME",
    "de_$0"."DEVICE_CONCEPT_ID",
    "de_$0"."DEVICE_EXPOSURE_START_DATE",
    "de_$0"."DEVICE_EXPOSURE_END_DATE",
    "ct_$3"."CONCEPT_NAME" AS "DEVICE_TYPE_NAME",
    "de_$0"."VISIT_OCCURRENCE_ID",
    "c_$2"."CONCEPT_CODE" AS "DEVICE_CONCEPT_CODE",
    "ct_$3"."CONCEPT_CODE" AS "DEVICE_TYPE_CONCEPT_CODE"
  FROM
    (
      (
        (
          "DEVICE_EXPOSURE" AS "de_$0"
          INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("de_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
        )
        LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
          "c_$2"."CONCEPT_ID" = "de_$0"."DEVICE_CONCEPT_ID"
        )
      )
      LEFT JOIN "VIEW::OMOP.CONCEPT" AS "ct_$3" ON (
        "ct_$3"."CONCEPT_ID" = "de_$0"."DEVICE_TYPE_CONCEPT_ID"
      )
    ) 
  );


CREATE OR REPLACE VIEW "VIEW::OMOP.DEATH" (
    "PATIENT_ID",
    "DEATH_TYPE_NAME",
    "DEATH_DATE",
    "DEATH_DATETIME",
    "DEATH_TYPE_CONCEPT_CODE"
  ) AS (
  SELECT
    "p_$1"."PATIENT_ID",
    "c_$2"."CONCEPT_NAME" AS "DEATH_TYPE_NAME",
    "d_$0"."DEATH_DATE",
    "d_$0"."DEATH_DATETIME",
    "c_$2"."CONCEPT_CODE" AS "DEATH_TYPE_CONCEPT_CODE"
  FROM
    (
      (
        "DEATH" AS "d_$0"
        INNER JOIN "VIEW::OMOP.PATIENT" AS "p_$1" ON ("d_$0"."PERSON_ID" = "p_$1"."PATIENT_ID")
      )
      LEFT JOIN "VIEW::OMOP.CONCEPT" AS "c_$2" ON (
        "c_$2"."CONCEPT_ID" = "d_$0"."DEATH_TYPE_CONCEPT_ID"
      )
    ) 
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.COND_ICD10" AS (
  SELECT 
      "CONDITION"."PERSON_ID" AS "PATIENT_ID",
      "CONDITION"."CONDITION_OCCURRENCE_ID",
      "CONDITION"."CONDITION_START_DATE",
      "CONDITION"."CONDITION_END_DATE",
      "CONDITION"."VISIT_OCCURRENCE_ID",
      "CONDITION"."CONDITION_SOURCE_VALUE",
      "CONDITION"."CONDITION_TYPE_CONCEPT_ID",
      "CONDITION"."CONDITION_STATUS_CONCEPT_ID",
      "C1"."CONCEPT_NAME" AS "ICD10_CONDITION_SOURCE_VALUE_CONCEPT_NAME",
      "C2"."CONCEPT_NAME" AS "CONDITION_TYPE_CONCEPT_NAME",
      "C3"."CONCEPT_NAME" AS "CONDITION_STATUS_CONCEPT_NAME",
      "C1"."CONCEPT_CODE" AS "CONDITION_SOURCE_VALUE_CONCEPT_CODE",
      "C2"."CONCEPT_CODE" AS "CONDITION_TYPE_CONCEPT_CODE",
      "C3"."CONCEPT_CODE" AS "CONDITION_STATUS_CONCEPT_CODE"
  FROM "CONDITION_OCCURRENCE" AS "CONDITION"
  LEFT JOIN "VIEW::OMOP.CONCEPT" AS "C1"
  ON "CONDITION"."CONDITION_SOURCE_VALUE" = "C1"."CONCEPT_CODE"
  AND "C1"."DOMAIN_ID" = 'Condition' AND "C1"."VOCABULARY_ID" = 'ICD10'
  LEFT JOIN "VIEW::OMOP.CONCEPT" AS "C2"
  ON "CONDITION"."CONDITION_TYPE_CONCEPT_ID" = "C2"."CONCEPT_ID"
  LEFT JOIN "VIEW::OMOP.CONCEPT" AS "C3"
  ON "CONDITION"."CONDITION_STATUS_CONCEPT_ID" = "C3"."CONCEPT_ID"
  );

CREATE OR REPLACE VIEW "VIEW::OMOP.PP_PER" (
    "PAYER_PLAN_PERIOD_ID",
    "PATIENT_ID",
    "PAYER_PLAN_PERIOD_START_DATE",
    "PAYER_PLAN_PERIOD_END_DATE",
    "PAYER_SOURCE_VALUE",
    "PLAN_SOURCE_VALUE",
    "FAMILY_SOURCE_VALUE"
  ) AS (
  SELECT
    "p_$0"."PAYER_PLAN_PERIOD_ID",
    "p_$0"."PERSON_ID" AS "PATIENT_ID",
    "p_$0"."PAYER_PLAN_PERIOD_START_DATE",
    "p_$0"."PAYER_PLAN_PERIOD_END_DATE",
    "p_$0"."PAYER_SOURCE_VALUE",
    "p_$0"."PLAN_SOURCE_VALUE",
    "p_$0"."FAMILY_SOURCE_VALUE"
  FROM
    "PAYER_PLAN_PERIOD" AS "p_$0"
  );


-- Recreate dependent OMOP GDM view
CREATE OR REPLACE VIEW "VIEW::OMOP.GDM.PATIENT" AS
  (
  SELECT "P".*, "RS".*
  FROM "VIEW::OMOP.PATIENT" AS "P"
  LEFT JOIN "VIEW::GDM.RESEARCH_SUBJECT_BASE" AS "RS"
  ON "P"."PATIENT_ID" = "RS"."PERSON_ID"
  );