import os
from functools import partial

from prefect import task
from prefect.logging import get_run_logger
from prefect.logging.loggers import task_run_logger
from prefect.server.schemas.states import StateType

from shared_utils.dao.DBDao import DBDao
from shared_utils.dao.UserDao import UserDao
from shared_utils.liquibase import Liquibase
from shared_utils.types import SupportedDatabaseDialects


def get_plugin_classpath(flow_name: str) -> str:
    return f'{os.getcwd()}/flows/{flow_name}/'


@task(log_prints=True)
def create_schema_task(dbdao: DBDao):
    schema_exists = dbdao.check_schema_exists()
    if schema_exists is False:
        dbdao.create_schema()
    else:
        error_msg = f"Schema '{dbdao.schema_name}' already exists in database '{dbdao.database_code}'"
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
def enable_and_create_audit_policies_task(dbdao: DBDao):
    logger = get_run_logger()
    enable_audit_policies = dbdao.tenant_configs.get("enableAuditPolicies")
    if enable_audit_policies:
        dbdao.enable_auditing()
        dbdao.create_system_audit_policy()
        dbdao.create_schema_audit_policy()
    else:
        logger.info("Skipping Alteration of system configuration")
        logger.info("Skipping creation of Audit policy for system configuration")
        logger.info(f"Skipping creation of new audit policy for {dbdao.schema_name}")


@task(log_prints=True)
def create_and_assign_roles_task(userdao: UserDao):
    logger = get_run_logger()
    
    # Check if schema read role exists
    match userdao.db_dialect:
        case SupportedDatabaseDialects.HANA:
            schema_read_role = f"{userdao.schema_name}_READ_ROLE"
        case SupportedDatabaseDialects.POSTGRES:
            schema_read_role = f"{userdao.schema_name}_read_role"

    schema_read_role_exists = userdao.check_role_exists(schema_read_role)
    if schema_read_role_exists:
        logger.info(f"'{schema_read_role}' role already exists")
    else:
        logger.info(f"'{schema_read_role}' does not exist")
        userdao.create_read_role(schema_read_role)
        
    # grant schema read role read privileges to schema read role
    logger.info(f"Granting read privileges to '{schema_read_role}'")
    userdao.grant_read_privileges(schema_read_role)

    # Check if read user exists
    read_user_exists = userdao.check_user_exists(userdao.read_user)
    if read_user_exists:
        logger.info(f"'{userdao.read_user}' user already exists")
    else:
        logger.info(f"'{userdao.read_user}' user does not exist")
        logger.info(f"Creating user '{userdao.read_user}'..")
        userdao.create_user(userdao.read_user)

    # Check if read role exists
    read_role_exists = userdao.check_role_exists(userdao.read_role)
    if read_role_exists:
        logger.info(f"'{userdao.read_role}' role already exists")
    else:
        logger.info(f"'{userdao.read_role}' role does not exist")
        logger.info(
            f"Creating '{userdao.read_role}' role and assigning to '{userdao.read_user}' user")
        userdao.create_and_assign_read_role(userdao.read_user, userdao.read_role)

    # Grant read role read privileges
    logger.info(f"Granting read privileges to '{userdao.read_role}' role")
    userdao.grant_read_privileges(userdao.read_role)


def drop_schema_hook(task, task_run, state, schema_dao: DBDao):
    logger = task_run_logger(task_run, task)
    logger.info(
        f"Dropping schema '{schema_dao.database_code}.{schema_dao.schema_name}'..")
    try:
        drop_schema = schema_dao.drop_schema()
    except Exception as e:
        logger.info(
            f"Failed to drop schema {schema_dao.database_code}.{schema_dao.schema_name}")
    else:
        logger.info(
            f"Successfully dropped schema '{schema_dao.database_code}.{schema_dao.schema_name}'")
