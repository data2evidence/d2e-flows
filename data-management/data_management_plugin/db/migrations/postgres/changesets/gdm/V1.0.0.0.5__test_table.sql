--liquibase formatted sql
--changeset alp:V1.0.0.0.5__test_table

create table test_table (
    id integer
    value varchar(20)
)