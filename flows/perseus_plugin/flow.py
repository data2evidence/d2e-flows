from prefect import flow, task
from prefect.logging import get_run_logger
from flows.perseus_plugin.Perseus import Perseus
from waitress import serve

@flow(log_prints=True)
def perseus_plugin():
    logger = get_run_logger()
    logger.info("triggering perseus flow")
    perseus = Perseus()
    perseus.start()
