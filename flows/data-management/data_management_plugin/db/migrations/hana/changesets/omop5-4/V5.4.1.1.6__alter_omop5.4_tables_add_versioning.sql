--liquibase formatted sql
--changeset alp:V5.4.1.1.6__alter_omop5.4_tables_add_versioning.sql

-- EPISODE
CREATE COLUMN TABLE "EPISODE_HISTORY" AS 
(
    SELECT * FROM "EPISODE"
);

ALTER TABLE "EPISODE" ADD (
    "SYSTEM_VALID_FROM"                 LONGDATE CS_LONGDATE NOT NULL GENERATED ALWAYS AS ROW START,
    "SYSTEM_VALID_UNTIL"                LONGDATE CS_LONGDATE NOT NULL GENERATED ALWAYS AS ROW END
);
ALTER TABLE "EPISODE_HISTORY" ADD (
    "SYSTEM_VALID_FROM"                 LONGDATE CS_LONGDATE NOT NULL,
    "SYSTEM_VALID_UNTIL"                LONGDATE CS_LONGDATE NOT NULL
);

-- EPISODE_EVENT
CREATE COLUMN TABLE "EPISODE_EVENT_HISTORY" AS 
(
    SELECT * FROM "EPISODE_EVENT"
);

ALTER TABLE "EPISODE_EVENT" ADD (
    "SYSTEM_VALID_FROM"                 LONGDATE CS_LONGDATE NOT NULL GENERATED ALWAYS AS ROW START,
    "SYSTEM_VALID_UNTIL"                LONGDATE CS_LONGDATE NOT NULL GENERATED ALWAYS AS ROW END
);
ALTER TABLE "EPISODE_EVENT_HISTORY" ADD (
    "SYSTEM_VALID_FROM"                 LONGDATE CS_LONGDATE NOT NULL,
    "SYSTEM_VALID_UNTIL"                LONGDATE CS_LONGDATE NOT NULL
);


ALTER TABLE "EPISODE" ADD PERIOD FOR SYSTEM_TIME (SYSTEM_VALID_FROM, SYSTEM_VALID_UNTIL);
ALTER TABLE "EPISODE" ADD SYSTEM VERSIONING HISTORY TABLE "EPISODE_HISTORY";

ALTER TABLE "EPISODE_EVENT" ADD PERIOD FOR SYSTEM_TIME (SYSTEM_VALID_FROM, SYSTEM_VALID_UNTIL);
ALTER TABLE "EPISODE_EVENT" ADD SYSTEM VERSIONING HISTORY TABLE "EPISODE_EVENT_HISTORY";
