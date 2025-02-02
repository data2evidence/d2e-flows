--liquibase formatted sql
--changeset alp:V1.0.0.0.0__create_bi_events_tables
CREATE TABLE "BI.EVENT" (
	"ID" VARCHAR(50) NOT NULL,
	"CREATED_AT" TIMESTAMP NOT NULL,
	"STUDY_ID" VARCHAR(50) NOT NULL,
	"TYPE" VARCHAR(50) NOT NULL,
	"DATA_ACTIVITY_TYPE" VARCHAR(50) NOT NULL,
	"DATA_CONSENT_DOCUMENT_KEY" VARCHAR(50) NULL,
	"DATA_EVENT_SOURCE" VARCHAR(500) NULL,
	"DATA_EVENT_TYPE" VARCHAR(50) NULL,
	"DATA_HOSTNAME" VARCHAR(50) NULL,
	"DATA_SERVICE_NAME" VARCHAR(50) NULL,
	"DATA_SERVICE_VERSION" VARCHAR(50) NULL,
	"DATA_TENANT_ID" VARCHAR(50) NULL,
	"DATA_TIMESTAMP" TIMESTAMP NULL,
	"PERSON_ID" VARCHAR(50) NULL,
	PRIMARY KEY ("ID")
);


CREATE TABLE "BI.EVENT_DATA"(
	"ID" VARCHAR(50) NOT NULL,
	"ATTRIBUTE_GROUP_ID" VARCHAR(50) NOT NULL,
	"ATTRIBUTE" VARCHAR(50) NOT NULL,
	"VALUE" VARCHAR(200) NULL,
	FOREIGN KEY("ID") REFERENCES "BI.EVENT"
);

COMMENT ON COLUMN "BI.EVENT_DATA"."ID" IS 'REFERENCES BI.EVENT TABLE';


--rollback DROP TABLE "BI.EVENT_DATA";
--rollback DROP TABLE "BI.EVENT";