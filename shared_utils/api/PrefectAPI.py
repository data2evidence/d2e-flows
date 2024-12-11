from shared_utils.types import AuthToken

async def get_auth_token_from_input():
    iter = AuthToken.receive(key_prefix="authtoken", timeout=30, poll_interval=3)
    return await iter.next()