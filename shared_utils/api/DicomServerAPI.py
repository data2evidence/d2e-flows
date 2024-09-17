import os
import json
import requests
from typing import Dict
from orthanc_api_client import OrthancApiClient

from prefect.variables import Variable
from prefect.blocks.system import Secret



class DicomServerAPI:
    def __init__(self):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")
        
        # Parse SERVICE_ROUTES and get dicomServer
        self.url = json.loads(service_routes)["dicomServer"]
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert

    def get_uploaded_file_name(self, instance_id: str) -> str:
        url = f"{self.url}instances/{instance_id}"
        response = requests.get(
            url,
            verify=self.verifySsl,
        )
        if ((response.status_code >= 400) and (response.status_code < 600)):
            raise Exception(
                f"DicomServerAPI failed to return instance information, {response.content}")
        else:
            file_name = response.json()['FileUuid']
            return str(file_name) + ".dcm"
