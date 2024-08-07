from functools import partial

from prefect import task, get_run_logger

from data_characterization_plugin.utils.liquibase import Liquibase
from data_characterization_plugin.utils.types import LiquibaseAction, DatabaseDialects


@task(log_prints=True)
def create_schema(dbdao):
    # currently only supports pg dialect
    schema_exists = dbdao.check_schema_exists()
    if schema_exists == False:
        dbdao.create_schema()
    else:
        error_msg = f"Schema {dbdao.schema_name} already exists in database {dbdao.database_code}"
        get_run_logger().error(error_msg)
        raise Exception(error_msg)


@task(log_prints=True)
def run_liquibase_update_task(**kwargs):
    try:
        liquibase = Liquibase(**kwargs)
        liquibase.update_schema()
    except Exception as e:
        get_run_logger().error(e)
        raise e
    
@task(log_prints=True)
def enable_and_create_audit_policies(schema_dao):
    schema_dao.enable_auditing()
    schema_dao.create_system_audit_policy()
    schema_dao.create_schema_audit_policy()


@task(log_prints=True)
def create_and_assign_roles(userdao, tenant_configs, dialect: str):
    logger = get_run_logger()
    # Check if schema read role exists

    match dialect:
        case DatabaseDialects.HANA:
            schema_read_role = f"{userdao.schema_name}_READ_ROLE"
        case DatabaseDialects.POSTGRES:
            schema_read_role = f"{userdao.schema_name}_read_role"

    schema_read_role_exists = userdao.check_role_exists(schema_read_role)
    if schema_read_role_exists:
        logger.info(f"'{schema_read_role}' role already exists")
    else:
        logger.info(f"{schema_read_role} does not exist")
        userdao.create_read_role(schema_read_role)
    # grant schema read role read privileges to schema
    logger.info(f"Granting read privileges to '{schema_read_role}'")
    userdao.grant_read_privileges(schema_read_role)

    # Check if read user exists
    read_user = tenant_configs.get("readUser")

    read_user_exists = userdao.check_user_exists(read_user)
    if read_user_exists:
        logger.info(f"{read_user} user already exists")
    else:
        logger.info(f"{read_user} user does not exist")
        read_password = tenant_configs.get("readPassword")
        logger.info(f"Creating user '{read_user}'")
        userdao.create_user(read_user, read_password)

    # Check if read role exists
    read_role = tenant_configs.get("readRole")

    read_role_exists = userdao.check_role_exists(read_role)
    if read_role_exists:
        logger.info(f"'{read_role}' role already exists")
    else:
        logger.info(f"'{read_role}' role does not exist")
        logger.info(
            f"'Creating '{read_role}' role and assigning to '{read_user}' user")
        userdao.create_and_assign_role(read_user, read_role)

    # Grant read role read privileges
    logger.info(f"'Granting read privileges to '{read_role}' role")
    userdao.grant_read_privileges(read_role)