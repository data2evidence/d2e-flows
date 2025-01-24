from prefect import flow, task
from prefect.logging import get_run_logger
from flows.white_rabbit_plugin.WhiteRabbit import WhiteRabbit


@flow(log_prints=True)
def white_rabbit_plugin():
    logger = get_run_logger()
    logger.info("white rabbit flow")
    logger.info("white rabbit flow")    
    white_rabbit = WhiteRabbit()
    white_rabbit.start() # should pass this to a task


'''
create own block with postgres credentials and perseus host
start app.jar and inject those env variables 

start flow with the env variables already inside
'''
