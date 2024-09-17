import os
import json
import requests
from typing import Union
from shared_utils.api.OpenIdAPI import OpenIdAPI

from prefect.variables import Variable
from prefect.blocks.system import Secret

class MeilisearchSvcAPI:
    def __init__(self):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")
        
        self.url = json.loads(service_routes)["meilisearch"]
        self.openIdAPI = OpenIdAPI()
        self.token = None
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert

    def getOptions(self):
        # Get new client credential token if it is empty or has expired
        if self.openIdAPI.isTokenExpiredOrEmpty(self.token):
            self.token = self.openIdAPI.getClientCredentialToken()

        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }

    def create_index(self, index_name: str, primary_key: str):
        url = f"{self.url}indexes"

        headers = self.getOptions()
        result = requests.post(
            url,
            headers=headers,
            verify=self.verifySsl,
            json={"uid": index_name, "primaryKey": primary_key}
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"MeilisearchAPI Failed to create index:{index_name}, {result.content}")

        return result

    def update_index_settings(self, index_name: str, settings: dict):
        url = f"{self.url}indexes/{index_name}/settings"

        headers = self.getOptions()
        result = requests.patch(
            url,
            headers=headers,
            verify=self.verifySsl,
            json=settings
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"MeilisearchAPI Failed to update settings for:{index_name}, {result.content}")

        return result

    def add_documents_to_index(self, index_name: str, primary_key: Union[str, int], documents: list):
        url = f"{self.url}indexes/{index_name}/documents?primaryKey={primary_key}"

        headers = self.getOptions()
        result = requests.put(
            url,
            headers=headers,
            verify=self.verifySsl,
            json=documents
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"MeilisearchAPI Failed add document to index:{index_name}, {result.content}")

        return result

    def update_synonym_index(self, index_name: str, documents):
        url = f"{self.url}indexes/{index_name}/settings/synonyms"

        headers = self.getOptions()
        result = requests.put(
            url,
            headers=headers,
            verify=self.verifySsl,
            json=documents
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"MeilisearchAPI Failed to update concept table index:{index_name} with synonyms. {result.content}")
