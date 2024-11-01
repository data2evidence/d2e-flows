from enum import Enum
from pydantic import BaseModel
from prefect.input import RunInput


class DBCredentialsType(BaseModel):
    adminPassword: str
    adminUser: str
    readPassword: str
    readUser: str
    dialect: str
    databaseName: str
    host: str
    port: int
    encrypt: bool
    validateCertificate: bool
    sslTrustStore: str
    hostnameInCertificate: str
    enableAuditPolicies: bool
    readRole: str


class UserType(str, Enum):
    ADMIN_USER = "admin_user"
    READ_USER = "read_user"


class HANA_TENANT_USERS(str, Enum):
    ADMIN_USER = "TENANT_ADMIN_USER",
    READ_USER = "TENANT_READ_USER",


class PG_TENANT_USERS(str, Enum):
    ADMIN_USER = "postgres_tenant_admin_user",
    READ_USER = "postgres_tenant_read_user",


class SupportedDatabaseDialects(str, Enum):
    HANA = "hana"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"


class RequestType(str, Enum):
    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    
    
class LiquibaseAction(str, Enum):
    UPDATE = "update"  # Create and update schema
    UPDATECOUNT = "updateCount"  # Create schema with count
    STATUS = "status"  # Get Version Info
    ROLLBACK_COUNT = "rollbackCount"  # Rollback on n changesets
    ROLLBACK_TAG = "rollback"  # Rollback on tag
    CHANGELOG_SYNC = "changelog-sync" # mark all changesets in databasechangelog table as executed


class InternalPluginType(str, Enum):
    DATA_MANAGEMENT = "data_management_plugin"
    DATA_CHARACTERIZATION = "data_characterization_plugin"
    DATA_QUALITY = "dqd_plugin"
    COHORT_GENERATOR = "cohort_generator_plugin"
    I2B2 = "i2b2_plugin"
    DUCK_DB = "create_cachedb_file_plugin"
    DATAFLOW_UI = "dataflow_ui_plugin"
    MEILISEARCH = "add_search_index_plugin"
    MEILISEARCH_EMBEDDINGS = "add_search_index_with_embeddings_plugin"
    R_CDM = "r_cdm_plugin"
    DATA_LOAD = "data_load_plugin"

    @staticmethod
    def values():
        return InternalPluginType._value2member_map_

class EntityCountDistributionType(BaseModel):
    OBSERVATION_PERIOD_COUNT: str
    DEATH_COUNT: str
    VISIT_OCCURRENCE_COUNT: str
    VISIT_DETAIL_COUNT: str
    CONDITION_OCCURRENCE_COUNT: str
    DRUG_EXPOSURE_COUNT: str
    PROCEDURE_OCCURRENCE_COUNT: str
    DEVICE_EXPOSURE_COUNT: str
    MEASUREMENT_COUNT: str
    OBSERVATION_COUNT: str
    NOTE_COUNT: str
    EPISODE_COUNT: str
    SPECIMEN_COUNT: str

class AuthToken(RunInput):
    token: str