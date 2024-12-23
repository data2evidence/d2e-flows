from prefect import task

from shared_utils.types import AuthToken


@task(log_prints=True)
def get_auth_token_from_input() -> AuthToken:
    iter = AuthToken.receive(key_prefix="authtoken", timeout=300, poll_interval=3)
    return iter.next()


def get_token_value(auth_token: AuthToken) -> str:
    return auth_token.token.get_secret_value().replace("Bearer ", "")
