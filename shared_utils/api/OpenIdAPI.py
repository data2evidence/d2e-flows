import os
import jwt
import json
import requests

from prefect.variables import Variable
from prefect.blocks.system import Secret

class OpenIdAPI:
    def __init__(self):
        service_routes = Variable.get("service_routes").value
        
        if service_routes is None:
            raise ValueError("'service_routes' prefect variable is undefined")

        logto_alp_data_client_id = Secret.load("logto-alp-data-client-id").get()
        logto_alp_data_client_secret = Secret.load("logto-alp-data-client-secret").get()
        idp_scope = Variable.get("idp_scope").value

        if logto_alp_data_client_id is None:
            raise ValueError("'logto_alp_data_client_id' secret block is undefined")

        if logto_alp_data_client_secret is None:
            raise ValueError("'logto_alp_data_client_secret' secret block is undefined")

        if idp_scope is None:
            raise ValueError("'idp_scope' prefect variable is undefined")
        
        python_verify_ssl = Variable.get("python_verify_ssl").value
        tls_internal_ca_cert = Secret.load("tls-internal-ca-cert").get()
        
        if python_verify_ssl == 'true' and tls_internal_ca_cert is None:
            raise ValueError("'tls-internal-ca-cert' prefect variable is undefined")

        # Parse SERVICE_ROUTES and get idIssuerUrl
        self.url = json.loads(service_routes)["idIssuerUrl"]
        self.clientId = logto_alp_data_client_id
        self.clientSecret = logto_alp_data_client_secret
        self.scope = idp_scope
        self.verifySsl = False if python_verify_ssl == 'false' else tls_internal_ca_cert


    def getOptions(self):
        return {
            "Content-Type": "application/json",
        }

    def getSigningKey(self, token):
        jwks_client = jwt.PyJWKClient(f"{self.url}/jwks")
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        return signing_key.key

    def getClientCredentialToken(self) -> str:
        params = {
            'grant_type': "client_credentials",
            'client_id': self.clientId,
            'client_secret': self.clientSecret,
        }

        result = requests.post(
            f"{self.url}/token",
            headers=self.getOptions(),
            verify=self.verifySsl,
            json=params
        )

        if ((result.status_code >= 400) and (result.status_code < 600)):
            raise Exception(
                f"OpenIdAPI Failed to get client credential token, {result.content}")
        else:
            return result.json()['access_token']

    def isTokenExpiredOrEmpty(self, token: str | None):
        if (not token):
            return True

        signing_key = self.getSigningKey(token)
        try:
            jwt.decode(token, signing_key, audience=self.scope,
                       algorithms=["ES384"])
            # Token can be successfully decoded and is still valid.
            return False
        except jwt.ExpiredSignatureError:
            # Token has expired
            return True
