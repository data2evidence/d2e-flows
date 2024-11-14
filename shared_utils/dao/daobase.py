import re
from typing import Optional
from datetime import datetime
from abc import ABC, abstractmethod
from pydantic import BaseModel
from sqlalchemy import text

from prefect.variables import Variable
from prefect.blocks.system import Secret

from shared_utils.api.OpenIdAPI import OpenIdAPI
from shared_utils.types import SupportedDatabaseDialects, UserType, DBCredentialsType, CacheDBCredentialsType

class DialectDrivers(BaseModel):
    class jdbc:
        postgres: str = "jdbc:postgresql"
        hana: str = "jdbc:sap"
        duckdb: str = "jdbc:duckdb"
        
    class sqlalchemy:
        postgres: str = "postgresql+psycopg2"
        hana: str = "hana+hdbcli"
        duckdb: str = "duckdb"

    class ibis:
        # Used for ibis
        postgres: str = "postgres"
        duckdb: str = "duckdb"
        
    class database_connector:
        postgres: str = "postgresql"
        hana: str = "hana"
        
    class cachedb:
        postgres: str = "postgresql"
        hana: str = "hana"
        duckdb: str = "duckdb"


class DaoBase(ABC):
    path_to_driver = "/app/inst/drivers"
    
    use_cache_db: bool = False
    database_code: str
    schema_name: str
    user_type: Optional[UserType] = UserType.ADMIN_USER
    vocab_schema_name: Optional[str] = ""
    
    def __init__(self, 
                 use_cache_db: bool, 
                 database_code: str,
                 user_type: UserType = UserType.ADMIN_USER,
                 schema_name: str = None,
                 vocab_schema_name: str = None):
        
        secret_block = Secret.load("database-credentials").get()
        if secret_block is None:
           raise ValueError(
               "'DATABASE_CREDENTIALS' secret block is undefined!")

        self.use_cache_db = use_cache_db
        self.database_code = database_code
        self.user_type = user_type
        self.schema_name = schema_name
        self.vocab_schema_name = vocab_schema_name

    # --- Property methods ---
    
    @property
    def dialect(self):
        return self.tenant_configs.dialect
    
    @property
    def read_user(self):
        return self.tenant_configs.readUser
    
    @property
    def read_role(self):
        return self.tenant_configs.readRole
        
    
    @property
    def tenant_configs(self) -> DBCredentialsType | CacheDBCredentialsType:
        database_credentials = self.__extract_database_credentials()
        
        if self.use_cache_db and database_credentials.dialect == SupportedDatabaseDialects.DUCKDB:
            if (self.schema_name is None and self.vocab_name is None):
                raise AttributeError(f"Schema name and vocab name needs to be set if 'use_cache_db' is True!")
            database_credentials.databaseName = self.__create_cachedb_db_name()
            database_credentials.adminUser = database_credentials.readUser = "Bearer " + OpenIdAPI().getClientCredentialToken()
            database_credentials.adminPassword = database_credentials.readPassword = "Qwerty"
            database_credentials.host = Variable.get("cachedb_host")
            database_credentials.port = Variable.get("cachedb_port")
            database_credentials_dict = database_credentials.model_dump()
            return CacheDBCredentialsType(**database_credentials_dict)
    
        return database_credentials


    # --- Create methods ---
    @abstractmethod
    def create_schema(self):
        pass

    @abstractmethod
    def create_table(self, table_name: str, columns: dict):
        pass



    # --- Read methods ---
    @abstractmethod
    def check_schema_exists(schema: str) -> bool:
        pass

    @abstractmethod
    def check_empty_schema(schema: str) -> bool:
        pass
    
    @abstractmethod
    def check_table_exists(self, table: str) -> bool:
        pass
    
    @abstractmethod
    def get_table_names(self, include_views=False) -> list[str]:
        pass

    @abstractmethod
    def get_columns(self, table: str) -> list[str]:
        pass
    
    @abstractmethod
    def get_table_row_count(self, table: str) -> int:
        pass

    @abstractmethod
    def get_distinct_count(self, table_name: str, column_name: str) -> int:
        pass

    @abstractmethod
    def get_value(self, table_name: str, column_name: str) -> str:
        pass

    @abstractmethod
    def get_next_record_id(self, table_name: str, id_column_name: int) -> int:
        pass

    @abstractmethod
    def get_last_executed_changeset(self) -> str:
        pass
    
    @abstractmethod
    def get_datamodel_created_date(self) -> datetime:
        pass
    
    @abstractmethod
    def get_datamodel_updated_date(self) -> datetime:
        pass



    # --- Update methods ---
    @abstractmethod
    def update_cdm_version(self, cdm_version: str):
        pass

    @abstractmethod
    def insert_values_into_table(self, table_name: str, column_value_mapping: list[dict]):
        pass
    
    

    # --- Delete methods ---
    @abstractmethod
    def drop_schema(self, cascade: bool=True):
        pass


    @abstractmethod
    def truncate_table(self, table_name: str):
        pass


    # --- User methods ---
    @abstractmethod
    def check_user_exists(self, user: str) -> bool:
        pass
    
    @abstractmethod
    def check_role_exists(self, role_name: str) -> bool:
        pass
    
    @abstractmethod
    def create_read_role(self, role_name: str):
        pass
    
    @abstractmethod
    def create_user(self, user: str, password: str = None):
        pass
    
    @abstractmethod
    def create_and_assign_role(self, user: str, role_name: str):
        pass
    
    @abstractmethod
    def grant_read_privileges(self, role_name: str):
        pass
    
    @abstractmethod
    def grant_cohort_write_privileges(self, role_name: str):
        pass

    # --- Static methods ---
    @staticmethod
    def validate_schema_name(schema_name: str) -> None:
        if len(schema_name.encode('utf-8')) > 63:
            raise ValueError(f"Schema name '{schema_name}' should not exceed 63 bytes!")

    @staticmethod
    def create_ibis_connection_url(dialect: SupportedDatabaseDialects, 
                                   database_name: str = None,
                                   user: str = None,
                                   password: str = None,
                                   host: str = None,
                                   port: int = None
                                   ) -> str:
        match dialect:
            case SupportedDatabaseDialects.DUCKDB:
                # "duckdb://" will connect to in-memory ephemeral database
                base_url = f"{getattr(DialectDrivers.ibis, dialect)}://{database_name}"
            case _:
                base_url = f"{getattr(DialectDrivers.ibis, dialect)}://{user}:{password}@{host}:{port}/{database_name}"
        return base_url

    @staticmethod
    def create_sqlalchemy_connection_url(dialect: SupportedDatabaseDialects, 
                                         database_name: str = None,
                                         user: str = None,
                                         password: str = None,
                                         host: str = None,
                                         port: int = None) -> str:
        match dialect:
            case SupportedDatabaseDialects.DUCKDB:
                base_url = f"{getattr(DialectDrivers.sqlalchemy, dialect)}://{database_name}"
            case _:
                base_url = f"{getattr(DialectDrivers.sqlalchemy, dialect)}://{user}:{password}@{host}:{port}/{database_name}"

        return base_url

    @staticmethod
    def create_jdbc_connection_url(dialect: SupportedDatabaseDialects, 
                                   database_name: str = None,
                                   user: str = None,
                                   password: str = None,
                                   host: str = None,
                                   port: int = None) -> str:

        match dialect:
            case SupportedDatabaseDialects.DUCKDB:
                base_url = f"{getattr(DialectDrivers.jdbc, dialect)}://{database_name}"
            case SupportedDatabaseDialects.POSTGRES:
                base_url = f"{getattr(DialectDrivers.jdbc, dialect)}://{user}:{password}@{host}:{port}/{database_name}"
            case SupportedDatabaseDialects.HANA:
                base_url = f"{getattr(DialectDrivers.jdbc, dialect)}://{user}:{password}@{host}:{port}?{database_name}"
                
        return base_url


    def get_database_connector_connection_string(
        self,
        user_type: UserType,
        release_date: str = None
    ):
        """
        Used for Database Connector package
        """
        
        database_credentials = self.tenant_configs
        database_connector_dialect = getattr(DialectDrivers.database_connector, database_credentials.dialect)

        match user_type:
            case UserType.ADMIN_USER:
                user = database_credentials.adminUser
                password = database_credentials.adminPassword.get_secret_value()
            case UserType.READ_USER:
                user = database_credentials.readUser
                password = database_credentials.readPassword.get_secret_value()

        dialect = database_credentials.dialect
        host = database_credentials.host
        port = database_credentials.port
        database_name = database_credentials.databaseName

        match dialect:
            case SupportedDatabaseDialects.POSTGRES:
                conn_url = f"{getattr(DialectDrivers.jdbc, dialect)}://{host}:{port}/{database_name}"
            case SupportedDatabaseDialects.HANA:
                conn_url = f"{getattr(DialectDrivers.jdbc, dialect)}://{host}:{port}?{database_name}"
                extra_config = f"&sessionVariable:TEMPORAL_SYSTEM_TIME_AS_OF={release_date}" if release_date else None
                conn_url.append(extra_config)

        connection_string = f"""connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = '{database_connector_dialect}', connectionString = '{conn_url}', user = '{user}', password = '{password}', pathToDriver = '{DaoBase.path_to_driver}')"""

        return connection_string

    @staticmethod
    def set_db_driver_env() -> str:
        """
        Updates path to driver class variable and returns R code
        """
        database_connector_jar_folder = DaoBase.path_to_driver
        set_jar_file_path = f"Sys.setenv(\'DATABASECONNECTOR_JAR_FOLDER\' = '{database_connector_jar_folder}')"
        return set_jar_file_path
    
    @staticmethod
    def compile_sql_with_params(sqlquery: str, bind_params: dict) -> str:
        """
        Compiles an sqlalchemy 
        
        e.g. select * from table where id = :id, {"id": 1}
        """
        if not bind_params:
            return sqlquery
        raw_sql = text(sqlquery).bindparams(**bind_params).compile(compile_kwargs={"literal_binds": True})
        return str(raw_sql)



    # --- Helper methods ---
    def __extract_database_credentials(self) -> DBCredentialsType:
        database_credentials_list = Secret.load("database-credentials").get()
        if not database_credentials_list:
            raise ValueError(f"'DATABASE_CREDENTIALS' secret is empty")
        _db = next(filter(lambda x: x["values"]["code"] == self.database_code and "alp-dataflow-gen" in x["tags"], database_credentials_list), None)
        if not _db:
            raise ValueError(f"Database code '{self.database_code}' not found in database credentials")
        return self.__process_database_credentials(_db)    

    def __process_database_credentials(self, base_database_credentials: dict) -> DBCredentialsType:
        combined = {**base_database_credentials["values"], **base_database_credentials["values"]["credentials"]}
        database_credentials = DBCredentialsType(**combined)
        match database_credentials.dialect:
            case SupportedDatabaseDialects.HANA:
                database_credentials.readRole = "TENANT_READ_ROLE"
            case SupportedDatabaseDialects.POSTGRES:
                database_credentials.readRole = "postgres_tenant_read_role"
            case _:
                dialect_err = f"Dialect {self.values['dialect']} not supported. Unable to find corresponding dialect read role."
                raise ValueError(dialect_err)
        return database_credentials

    def __create_cachedb_db_name(self) -> str:
        match self.user_type:
            case UserType.READ_USER:
                connection_type = "read"
            case UserType.ADMIN_USER:
                connection_type = "write"
        db_name = f"B|{self.dialect}|{connection_type}|{self.database_code}"
        
        if self.dialect == SupportedDatabaseDialects.DUCKDB:
            db_name.append(f"|{self.schema_name}|{self.vocab_schema_name}")
            
        return db_name

    def __sanitize_inputs(self, input: str):
        # Allow only alphanumeric characters, underscores, and periods
        if not all(char.isalnum() or char in ("_", ".") for char in input):
            raise ValueError("Invalid characters in idenitifier")
        return re.sub(r'[^a-zA-Z0-9_.]', '', input)

    def _casefold(self, obj_name: str) -> str:
        if not obj_name.startswith("GDM."):
            return obj_name.casefold()
        else:
            return obj_name