--liquibase formatted sql
--changeset alp:V5.4.1.2.3__extend_concept_name_col

ALTER TABLE "CONCEPT" DROP SYSTEM VERSIONING;


-- Alter CONCEPT_HISTORY table
DROP FULLTEXT INDEX index_on_concept_name_history;

ALTER TABLE "CONCEPT_HISTORY" ALTER (
	"CONCEPT_NAME" NVARCHAR(2000) NOT NULL
);
CREATE FULLTEXT INDEX index_on_concept_name_history ON CONCEPT_HISTORY(CONCEPT_NAME);

-- Alter CONCEPT table
DROP FULLTEXT INDEX index_on_concept_name;

ALTER TABLE "CONCEPT" ALTER (
	"CONCEPT_NAME" NVARCHAR(2000) NOT NULL
);

CREATE FULLTEXT INDEX index_on_concept_name ON CONCEPT(CONCEPT_NAME);


ALTER TABLE "CONCEPT" ADD SYSTEM VERSIONING HISTORY TABLE "CONCEPT_HISTORY";