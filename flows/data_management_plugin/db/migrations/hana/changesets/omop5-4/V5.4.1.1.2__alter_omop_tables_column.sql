--liquibase formatted sql
--changeset alp:V5.4.1.1.2__alter_omop_tables_column.sql


-- Fixes spelling error
RENAME COLUMN "COST"."REVEUE_CODE_SOURCE_VALUE" TO "REVENUE_CODE_SOURCE_VALUE";


-- Add missing column
ALTER TABLE "PAYER_PLAN_PERIOD" ADD (
	"SPONSOR_SOURCE_VALUE" VARCHAR(50) NULL
);


--rollback RENAME COLUMN "COST"."REVENUE_CODE_SOURCE_VALUE" TO "REVEUE_CODE_SOURCE_VALUE";
--rollback ALTER TABLE "PAYER_PLAN_PERIOD" DROP COLUMN "SPONSOR_SOURCE_VALUE";