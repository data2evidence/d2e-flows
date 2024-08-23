from pydantic import BaseModel
from typing import Any, Optional


class CreateDuckdbDatabaseFileType(BaseModel):
    databaseCode: str
    schemaName: str

    # Flag used for cdw-config to create empty duckdb database file for validation
    # When this flag is set to True, it will also save the duckdb database file into a separate volume only for cdw-svc
    createForCdwConfigValidation: Optional[bool] = False


class CreateDuckdbDatabaseFileModules(BaseModel):
    # TODO: TBD disscuss a better way to handle dynamic imports
    utils_types: Any
    dbutils: Any
    dao_DBDao: Any


DUCKDB_FULLTEXT_SEARCH_CONFIG = {
    "concept": {
        "document_identifier": "concept_id",
    },
    "concept_relationship": {
        # primary key does not exist in concept_relationship table
        "document_identifier": "fts_document_identifier_id",
    },
    "relationship": {
        "document_identifier": "relationship_id",
    },
    "vocabulary": {
        "document_identifier": "vocabulary_id",
    },
    "concept_synonym": {
        # primary key does not exist in concept_synonym table,
        "document_identifier": "fts_document_identifier_id",
    },
    "concept_class": {
        "document_identifier": "concept_class_id",
    },
    "domain": {
        "document_identifier": "domain_id",
    },
    "concept_ancestor": {
        # primary key does not exist in concept_ancestor table
        "document_identifier": "fts_document_identifier_id",
    },
    "concept_recommended": {
        # primary key does not exist in concept_ancestor table
        "document_identifier": "fts_document_identifier_id",
    },
}
