--liquibase formatted sql
--changeset alp:V1.0.0.0.3_create_upload_metadata_table

CREATE TABLE file_upload_metadata(
    task_run_id varchar(255) not null,
    flow_run_id varchar(255) not null,
    original_filename varchar(255) not null,
    instance_id varchar(255) not null,
    uploaded_filename varchar(255) not null,
    uploaded_datetime timestamp not null,
    image_occurrence_id varchar(255) not null,
    sop_instance_id varchar(255) not null
)

-- rollback drop table file_upload_metadata;

