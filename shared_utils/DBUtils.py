import os
import json
from sqlalchemy import create_engine

from prefect.variables import Variable
from prefect.blocks.system import Secret

from shared_utils.types import *
from shared_utils.api.OpenIdAPI import OpenIdAPI


class DBUtils:
    path_to_driver = "/app/inst/drivers"

    def __init__(self, use_cache_db: bool, database_code: str):
        secret_block = Secret.load("database-credentials").get()
        if secret_block is None:
           raise ValueError(
               "'DATABASE_CREDENTIALS' secret block is undefined!")
        self.use_cache_db = use_cache_db
        self.database_code = database_code
        self.db_dialect = self.get_database_dialect()
        
        
    def get_tenant_configs(self, schema_name: str = None):
        if self.schema_name:
            return self.__extract_database_credentials(schema_name=schema_name)
        else:
            return self.__extract_database_credentials()        


    def set_db_driver_env(self) -> str:
        database_connector_jar_folder = DBUtils.path_to_driver
        set_jar_file_path = f"Sys.setenv(\'DATABASECONNECTOR_JAR_FOLDER\' = '{database_connector_jar_folder}')"
        return set_jar_file_path


    def get_database_dialect(self) -> str:
        database_credentials = self.__extract_database_credentials()
        dialect = database_credentials.get("dialect")
        supported_dialects = [dialect.value for dialect in SupportedDatabaseDialects]
        if dialect not in supported_dialects:
            raise ValueError(f"Database dialect '{dialect}' not supported, only '{supported_dialects}'.")
        else:
            return dialect


    def create_database_engine(self, schema_name: str = None, user_type: UserType = None):
        '''
        Used for SQLAlchemy 
        '''
        if self.use_cache_db:
            if not schema_name:
                raise ValueError("schema_name cannot be None")
            connection_string = self.__create_connection_string(schema_name=schema_name, create_engine=True)
        else:
            if not user_type:
                raise ValueError("User Type cannot be None")
            connection_string = self.__create_connection_string(user_type=user_type, create_engine=True)
        engine = create_engine(connection_string)
        return engine


    def get_database_connector_connection_string(self, schema_name: str = None, user_type: UserType = None, release_date: str = None) -> str:
        '''
        Used for Database Connector package
        '''
        if self.use_cache_db:
            if not schema_name:
                raise ValueError("schema_name cannot be None")
            # uses admin user for cachedb
            connection_string = self.__create_connection_string(schema_name=schema_name, create_engine=False)

        else:
            if not user_type:
                raise ValueError(f"User type '{user_type}' not allowed, only '{[user.value for user in UserType]}'.")
            
            dialect = self.get_database_dialect()
            match dialect:
                case SupportedDatabaseDialects.HANA:
                    # Append sessionVariable to database connection string if release_date is defined
                    extra_config = f"&sessionVariable:TEMPORAL_SYSTEM_TIME_AS_OF={release_date}" if release_date else None
                case SupportedDatabaseDialects.POSTGRES:
                    extra_config = None
                case _:
                    raise ValueError(f"Dialect '{dialect}' not supported!")
            connection_string = self.__create_connection_string(user_type=user_type, extra_config=extra_config, create_engine=False)
                
        return connection_string


    def __create_connection_string(self, 
                                   schema_name: str = None, 
                                   user_type: UserType = None, 
                                   extra_config: str = "", 
                                   create_engine: bool = False) -> str:
        '''
        Creates database connection string to be used for SqlAlchemy Engine and Database Connector
        '''
        
        if self.use_cache_db:
            database_credentials = self.__extract_database_credentials(schema_name)
            user = database_credentials.get("adminUser")
            password = database_credentials.get("adminPassword")
        else:
            database_credentials = self.__extract_database_credentials()
            match user_type:
                case UserType.ADMIN_USER:
                    user = database_credentials.get("adminUser")
                    password = database_credentials.get("adminPassword")
                case UserType.READ_USER:
                    user = database_credentials.get("readUser")
                    password = database_credentials.get("readPassword")

        dialect = database_credentials.get("dialect")
        database_name = database_credentials.get("databaseName")
        host = database_credentials.get("host")
        port = database_credentials.get("port")

        if create_engine:  # for sqlalchemy
            match dialect:
                case SupportedDatabaseDialects.HANA:
                    dialect_driver = "hana+hdbcli"
                    encrypt = database_credentials.get("encrypt")
                    validate_certificate = database_credentials.get(
                        "validateCertificate")
                    database_config_string = database_name + \
                        f"?encrypt={encrypt}?sslValidateCertificate={validate_certificate}"

                case SupportedDatabaseDialects.POSTGRES:
                    dialect_driver = "postgresql+psycopg2"
                    database_config_string = database_name
                case _:
                    raise ValueError(f"Dialect '{dialect}' not supported!")

            connection_string = create_base_connection_string(
                dialect_driver, user, password, host, port, database_config_string)
            return connection_string

        match dialect:
            case SupportedDatabaseDialects.HANA:
                connection_dialect = dialect
                base_connection_string = f"jdbc:sap://{host}:{port}?databaseName={database_name}{extra_config}"
            case SupportedDatabaseDialects.POSTGRES:
                connection_dialect = "postgresql"
                base_connection_string = f"jdbc:postgresql://{host}:{port}/{database_name}"
            case _:
                raise ValueError(f"Dialect '{dialect}' not supported!")
        connection_string = f"connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = '{connection_dialect}', connectionString = '{base_connection_string}', user = '{user}', password = '{password}', pathToDriver = '{DBUtils.path_to_driver}')"
        return connection_string


    def __extract_database_credentials(self, schema_name: str = None) -> dict:
        secret_block = Secret.load("database-credentials").get()
        database_credentials_list = json.loads(secret_block)
        
        if database_credentials_list == []:
            raise ValueError(
                f"'DATABASE_CREDENTIALS' environment variable is empty!")
        else:
            _db = next(filter(lambda x: x["values"]
                              ["code"] == self.database_code and "alp-dataflow-gen" in x["tags"], database_credentials_list), None)
            if not _db:
                raise ValueError(
                    f"Database code '{self.database_code}' not found in database credentials")
            else:
                database_credentials = self.__process_database_credentials(_db)

                if schema_name:
                    database_credentials["databaseName"] = f"B|{database_credentials.get('dialect')}|{database_credentials.get('databaseName')}|{schema_name}"
                    database_credentials["adminUser"] = database_credentials["readUser"] = "Bearer " + OpenIdAPI().getClientCredentialToken()
                    database_credentials["adminPassword"] = database_credentials["readPassword"] = "qwerty"
                    database_credentials["host"] = Variable.get("cachedb_host").value
                    database_credentials["port"]  = Variable.get("cachedb_port").value
        
        return database_credentials


    def __process_database_credentials(self, database_credential_json: dict) -> DBCredentialsType:   
        base_values = database_credential_json.get("values")
        database_credential_values = base_values["credentials"]
        database_credential_values["databaseName"] = base_values["databaseName"]
        database_credential_values["dialect"] = base_values["dialect"]
        database_credential_values["host"] = base_values["host"]
        database_credential_values["port"] = base_values["port"]
        database_credential_values["encrypt"] = base_values.get("encrypt", False)
        database_credential_values["validateCertificate"] = base_values.get(
            "validateCertificate", False)
        database_credential_values["sslTrustStore"] = base_values.get(
            "sslTrustStore", "")
        database_credential_values["hostnameInCertificate"] = base_values.get(
            "hostnameInCertificate", "")
        database_credential_values["enableAuditPolicies"] = base_values.get(
            "enableAuditPolicies", False)

        match database_credential_values.get("dialect"):
            case SupportedDatabaseDialects.HANA:
                read_role = "TENANT_READ_ROLE" # Todo: pass as env 
            case SupportedDatabaseDialects.POSTGRES:
                read_role = "postgres_tenant_read_role" # Todo: pass as env
            case _:
                dialect_err = f"Dialect {self.values['dialect']} not supported. Unable to find corresponding dialect read role."
                raise ValueError(dialect_err)
            
        database_credential_values["readRole"] = read_role

        # validate schema
        DBCredentialsType(**database_credential_values)
        return database_credential_values


def create_base_connection_string(dialect_driver: str, user: str, password: str,
                                  host: str, port: int, database_name: str) -> str:
    return f"{dialect_driver}://{user}:{password}@{host}:{port}/{database_name}"