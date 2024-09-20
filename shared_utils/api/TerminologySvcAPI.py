import requests

from shared_utils.api.BaseAPI import BaseAPI


class TerminologySvcAPI(BaseAPI):
    def __init__(self, token):
        super().__init__()
        self.url = self.get_service_route("terminology_service_route")
        self.token = token

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
            verify=self.get_verify_value()
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"TerminologySvcAPI Failed to get hybrid search config, {result.content}")
        else:
            hybridSearchConfig = result.json()
            return hybridSearchConfig