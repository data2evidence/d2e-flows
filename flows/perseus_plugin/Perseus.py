import requests
import time
from pydantic import ValidationError
from prefect.variables import Variable
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from prefect_shell import ShellOperation
from flows.perseus_plugin.types import ServiceCredentials
import os
import sys
from subprocess import Popen, PIPE, STDOUT, CalledProcessError

class Perseus:
    def __init__(self):
        self.logger = get_run_logger()
        self.white_rabbit_endpoint = "http://localhost:5004/backend/"
        try:
            service_credentials = ServiceCredentials(
                PG__DB_NAME=Variable.get("pg_db_name"),
                PG__PORT=Variable.get("pg_db_port"),
                PG__HOST=Variable.get("pg_db_host"),
                PERSEUS__FILES_MANAGER_HOST=Variable.get("perseus_host"),
                PG_ADMIN_USER=Secret.load("pg-admin-user").get(),
                PG_ADMIN_PASSWORD=Secret.load("pg-admin-password").get(),
                PERSEUS_ENV='Local')
        except ValidationError as e:
            self.logger.error(e)
            raise ValidationError(e)

        self.service_credentials = service_credentials

    def start(self):
        try:
            self.logger.info("Running command to start perseus...")
            process = ShellOperation(commands=['/usr/src/app/bin/python3 /app/main.py'],
                                     env=self.service_credentials.model_dump(mode='json')).trigger()
               
        except Exception as e:
            self.logger.error(f"Failed to start service: {e}")
            raise Exception(e)
        else:
            self.logger.info(
                "Successfully run command to start perseus service")

        while not self.health_check():
            time.sleep(10)
        self.logger.info("perseus service is ready to accept requests")
        self.process = process

    def health_check(self):
        try:
            response = requests.get(f"{self.white_rabbit_endpoint}api/info")

            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"perseus service is not ready: {e}")
            return False
