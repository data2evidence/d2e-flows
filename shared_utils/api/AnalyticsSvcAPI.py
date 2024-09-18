import json
import requests

from shared_utils.api.BaseAPI import BaseAPI


class AnalyticsSvcAPI(BaseAPI):
    def __init__(self, token):
        super().__init__()
        self.url = self.get_service_route("analytics_service_route")
        self.token = token

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
            verify=self.get_verify_value()
        )
        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"AnalyticsSvcAPI Failed to create create_cohort_definition schema, {result.content}")
        else:
            cohortDefinitionId = json.loads(result.content)['data']
            return cohortDefinitionId
