import os
import json
import requests

from prefect.variables import Variable
from prefect.blocks.system import Secret


class AnalyticsSvcAPI:
    def __init__(self, token):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")
        
        self.url = json.loads(service_routes)["analytics"]
        self.token = token
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert

    def getOptions(self):
        return {
            "Authorization": self.token
        }

    def create_cohort_definition(self, datasetId: str,
                                 description: str,
                                 owner: str,
                                 syntax: str,
                                 name: str) -> int:
        url = f"{self.url}api/services/cohort-definition"
        headers = self.getOptions()
        data = {
            "studyId": datasetId,
            "name": name,
            "description": description,
            "owner": owner,
            "syntax": syntax
        }
        result = requests.post(
            url,
            headers=headers,
            json=data,
            verify=self.verifySsl
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"AnalyticsSvcAPI Failed to create create_cohort_definition schema, {result.content}")
        else:
            cohortDefinitionId = json.loads(result.content)['data']
            return cohortDefinitionId
