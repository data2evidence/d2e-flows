from pydantic import ValidationError
from prefect.variables import Variable
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from prefect_shell import ShellOperation
import requests
import time
import signal
import os
from flows.white_rabbit_plugin.types import ServiceCredentials


class WhiteRabbit:
    def __init__(self):
        self.logger = get_run_logger()

        try:
            service_credentials = ServiceCredentials(
                PG__DB_NAME=Variable.get("pg_db_name"),
                PG__PORT=Variable.get("pg_db_port"),
                PG__HOST=Variable.get("pg_db_host"),
                PERSEUS__FILES_MANAGER_HOST=Variable.get("perseus_host"),
                PG_ADMIN_USER=Secret.load("pg-admin-user").get(),
                PG_ADMIN_PASSWORD=Secret.load("pg-admin-password").get())
        except ValidationError as e:
            self.logger.error(e)
            raise ValidationError(e)

        self.service_credentials = service_credentials

    def start(self):
        try:
            self.logger.info("Running command to start white rabbit...")
            process = ShellOperation(commands=['java -jar /app.jar'],
                                     env=self.service_credentials.model_dump(mode='json')).trigger()
        except Exception as e:
            self.logger.error(f"Failed to start service: {e}")
            raise Exception(e)
        else:
            self.logger.info(
                "Successfully run command to start white rabbit service")

        while not self.health_check():
            time.sleep(3)
        self.logger.info("white rabbit service is ready to accept requests")
        self.process = process
        return process  # maybe not needed here

    def health_check(self):
        try:
            response = requests.get(
                "http://localhost:8000/white-rabbit/api/info")

            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"service is not ready: {e}")
            return False

    def stop(self):
        if self.process.return_code is None:  # returns none if process is still running
            os.kill(os.getpgid(self.process.pid), signal.SIGTERM)
            self.logger.info("White rabbit service is terminated")
# for line in iter(process.stdout.readline, ''):
#             line = line.replace('\r', '').replace('\n', '')
#             logger.info(line)
#             sys.stdout.flush()
