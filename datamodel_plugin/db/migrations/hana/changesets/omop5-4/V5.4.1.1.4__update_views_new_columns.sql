--liquibase formatted sql
--changeset alp:V5.4.1.1.4__update_views_new_columns

CREATE OR REPLACE VIEW "VIEW::OMOP.PROC" (
    "PROCEDURE_OCCURRENCE_ID",
    "PATIENT_ID",
    "PROCEDURE_CONCEPT_ID",
    "PROCEDURE_NAME",
    "PROCEDURE_START_DATE",
    "PROCEDURE_START_DATETIME",
    "PROCEDURE_END_DATE",
    "PROCEDURE_END_DATETIME",
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
    "p_$0"."PROCEDURE_DATE" AS "PROCEDURE_START_DATE",
    "p_$0"."PROCEDURE_DATETIME" AS "PROCEDURE_START_DATETIME",
    "p_$0"."PROCEDURE_END_DATE",
    "p_$0"."PROCEDURE_END_DATETIME",
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
  )
  WITH READ ONLY;


CREATE OR REPLACE VIEW "VIEW::OMOP.PP_PER" (
    "PAYER_PLAN_PERIOD_ID",
    "PATIENT_ID",
    "PAYER_PLAN_PERIOD_START_DATE",
    "PAYER_PLAN_PERIOD_END_DATE",
    "PAYER_SOURCE_VALUE",
    "PLAN_SOURCE_VALUE",
    "FAMILY_SOURCE_VALUE",
    "SPONSOR_SOURCE_VALUE"
  ) AS (
  SELECT
    "p_$0"."PAYER_PLAN_PERIOD_ID",
    "p_$0"."PERSON_ID" AS "PATIENT_ID",
    "p_$0"."PAYER_PLAN_PERIOD_START_DATE",
    "p_$0"."PAYER_PLAN_PERIOD_END_DATE",
    "p_$0"."PAYER_SOURCE_VALUE",
    "p_$0"."PLAN_SOURCE_VALUE",
    "p_$0"."FAMILY_SOURCE_VALUE",
    "p_$0"."SPONSOR_SOURCE_VALUE"
  FROM
    "PAYER_PLAN_PERIOD" AS "p_$0"
  )
  WITH READ ONLY;