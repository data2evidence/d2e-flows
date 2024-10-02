
import requests
from typing import Union
from shared_utils.api.OpenIdAPI import OpenIdAPI

from shared_utils.api.BaseAPI import BaseAPI


class MeilisearchSvcAPI(BaseAPI):
    def __init__(self):
        super().__init__()
        self.url = self.get_service_route("meilisearch")        
        self.openIdAPI = OpenIdAPI()
        self.token = None

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
            verify=self.get_verify_value(),
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
            verify=self.get_verify_value(),
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
            verify=self.get_verify_value(),
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
            verify=self.get_verify_value(),
            json=documents
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"MeilisearchAPI Failed to update concept table index:{index_name} with synonyms. {result.content}")
