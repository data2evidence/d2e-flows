--liquibase formatted sql
--changeset alp:V1.0.0.0.6__alter_research_subject_view
CREATE OR REPLACE VIEW "VIEW::GDM.RESEARCH_SUBJECT_BASE" AS
SELECT 
"RS"."ALP_ID",
"RS"."PERSON_ID",
"RS"."STATUS",
"RS"."PERIOD_START",	  
"RS"."PERIOD_END",
"RS"."STUDY_REFERENCE",
"RS"."STUDY_TYPE",
"RS"."STUDY_IDENTIFIER_USE",
"RS"."STUDY_IDENTIFIER_TYPE",
"RS"."STUDY_IDENTIFIER_SYSTEM",
"RS"."STUDY_IDENTIFIER_VALUE",
"RS"."STUDY_IDENTIFIER_PERIOD_START",
"RS"."STUDY_IDENTIFIER_PERIOD_END",
"RS"."STUDY_DISPLAY",
"RS"."INDIVIDUAL_REFERENCE",
"RS"."INDIVIDUAL_TYPE",
"RS"."INDIVIDUAL_IDENTIFIER_USE",
"RS"."INDIVIDUAL_IDENTIFIER_TYPE",
"RS"."INDIVIDUAL_IDENTIFIER_SYSTEM",
"RS"."INDIVIDUAL_IDENTIFIER_VALUE",
"RS"."INDIVIDUAL_IDENTIFIER_PERIOD_START",
"RS"."INDIVIDUAL_IDENTIFIER_PERIOD_END",
"RS"."INDIVIDUAL_DISPLAY",
"RS"."ASSIGNED_ARM",
"RS"."ACTUAL_ARM",
"QR"."MAX_AUTHORED" AS "QUESTIONNAIRE_RESPONSE_MAX_AUTHORED",
TO_DATE('9999-12-31', 'YYYY-MM-DD') AS "DUMMY_DATE_OF_BIRTH"
FROM "GDM.RESEARCH_SUBJECT" AS "RS"
LEFT JOIN (
	SELECT "PERSON_ID", MAX("AUTHORED") AS "MAX_AUTHORED" 
	FROM "GDM.QUESTIONNAIRE_RESPONSE" 
	GROUP BY "PERSON_ID"
) AS "QR"
ON "RS"."PERSON_ID" = "QR"."PERSON_ID"
WITH READ ONLY;

--rollback DROP VIEW "VIEW::GDM.RESEARCH_SUBJECT_BASE";