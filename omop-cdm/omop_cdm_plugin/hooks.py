from prefect.logging.loggers import task_run_logger
from prefect.server.schemas.states import StateType

def drop_schema_hook(task, task_run, state, schema_dao):
    logger = task_run_logger(task_run, task)
    logger.info(
        f"Dropping schema {schema_dao.database_code}.{schema_dao.schema_name}")
    try:
        drop_schema = schema_dao.drop_schema()
        msg = f"Successfully drop schema {schema_dao.database_code}.{schema_dao.schema_name}"
        logger.info(msg)
    except Exception as e:
        logger.error(
            f"Failed to drop schema {schema_dao.database_code}.{schema_dao.schema_name}")
        raise e