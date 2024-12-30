import json
import os
from pathlib import Path

import msal


def acquire_token_func():

    client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
    scopes = ["Files.ReadWrite.All"]

    app = msal.PublicClientApplication(client_id=client_id, authority="https://login.microsoftonline.com/common")

    refresh_token = os.environ["MSGRAPH_TOKEN"]

    return app.acquire_token_by_refresh_token(refresh_token, scopes)


# For acquiring new refresh token
def _store_new_refresh_token():

    client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
    scopes = ["Files.ReadWrite.All"]

    token_file = Path(__file__).parent / "ms_graph_token.json"

    app = msal.PublicClientApplication(client_id=client_id, authority="https://login.microsoftonline.com/common")

    # Try to load existing refresh token
    if os.path.exists(token_file):
        with open(token_file) as f:
            stored_token = json.load(f)
            result = app.acquire_token_by_refresh_token(stored_token["refresh_token"], scopes)
            if result:
                return result

    # Get new tokens if refresh fails or no stored token
    flow = app.initiate_device_flow(scopes)
    print(flow["message"])
    result = app.acquire_token_by_device_flow(flow)

    # Store the refresh token
    with open(token_file, "w") as f:
        json.dump(result, f)

    return result
