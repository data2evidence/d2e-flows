import os
import json
import requests

from prefect.variables import Variable
from prefect.blocks.system import Secret


class PortalServerAPI:
    def __init__(self, token):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")
        
        self.datasets_url = json.loads(service_routes)["portalServer"] + 'dataset/list?role=systemAdmin'
        self.dataset_attributes_url = json.loads(service_routes)["portalServer"] + 'dataset/attribute'
        self.token = token
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert

    def getOptions(self):
        return {
            "Authorization": self.token
        }

    def get_datasets_from_portal(self):
        headers = self.getOptions()
        result = requests.get(
            self.datasets_url,
            headers=headers,
            verify=self.verifySsl
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"[{result.status_code}] PortalServerAPI Failed to retrieve datasets from portal")
        else:
            datasets_list = result.json()
            return datasets_list

    def update_dataset_attributes_table(self, study_id: str, attribute_id: str, attribute_value: str):
        headers = self.getOptions()
        result = requests.put(
            self.dataset_attributes_url,
            headers=headers,
            verify=self.verifySsl,
            json={"studyId": str(
                study_id), "attributeId": attribute_id, "value": attribute_value}
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"[{result.status_code}] PortalServerAPI - Failed to update dataset attribute '{attribute_id}' for study '{study_id}'")
        else:
            return True
