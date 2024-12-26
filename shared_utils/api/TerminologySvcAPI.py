import requests

from shared_utils.types import AuthToken
from shared_utils.api.BaseAPI import BaseAPI
from shared_utils.api.PrefectAPI import get_auth_token_from_input


class TerminologySvcAPI(BaseAPI):
    def __init__(self):
        super().__init__()
        self.url = self.get_service_route("terminology")


    def get_hybrid_search_config(self):
        url = f"{self.url}hybrid-search-config"
        headers = self.get_options()
        result = requests.get(
            url,
            headers=headers,
            verify=self.get_verify_value()
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"TerminologySvcAPI Failed to get hybrid search config, {result.content}")
        else:
            hybrid_search_config = result.json()
            return hybrid_search_config