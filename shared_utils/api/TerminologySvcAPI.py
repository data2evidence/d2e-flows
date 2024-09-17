import os
import json
import requests

from prefect.variables import Variable
from prefect.blocks.system import Secret


class TerminologySvcAPI:
    def __init__(self, token):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")
        
        self.url = json.loads(service_routes)["terminology"]
        self.token = token
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert


    def getOptions(self):
        return {
            "Authorization": self.token
        }

    def get_hybridSearchConfig(self):
        url = f"{self.url}hybrid-search-config"
        headers = self.getOptions()
        result = requests.get(
            url,
            headers=headers,
            verify=self.verifySsl
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"TerminologySvcAPI Failed to get hybrid search config, {result.content}")
        else:
            hybridSearchConfig = result.json()
            return hybridSearchConfig