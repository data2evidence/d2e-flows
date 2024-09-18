from typing import Optional
from pydantic import BaseModel


class CreateDuckdbDatabaseFileType(BaseModel):
    databaseCode: str
    schemaName: str
    
    # Flag used for cdw-config to create empty duckdb database file for validation
    # When this flag is set to True, it will also save the duckdb database file into a separate volume only for cdw-svc
    createForCdwConfigValidation: Optional[bool] = False

    @property
    def use_cache_db(self) -> str:
        return False


DUCKDB_FULLTEXT_SEARCH_CONFIG = {
    "concept": {
        "document_identifier": "concept_id",
    },
    # TODO: Temp commented out due to OOM error. Discuss if table is required or not.
    # https://github.com/alp-os/internal/issues/1064
    # "concept_relationship": {
    #     # primary key does not exist in concept_relationship table
    #     "document_identifier": "fts_document_identifier_id",
    # },
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
    # TODO: Temp commented out due to OOM error. Discuss if table is required or not.
    # https://github.com/alp-os/internal/issues/1064
    # "concept_ancestor": {
    #     # primary key does not exist in concept_ancestor table
    #     "document_identifier": "fts_document_identifier_id",
    # },
    "concept_recommended": {
        # primary key does not exist in concept_ancestor table
        "document_identifier": "fts_document_identifier_id",
    },
    "note": {
        "document_identifier": "note_id",
    },
}
