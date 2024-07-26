from utils._constants import (
    PAYLOAD,
    DOMAIN,
    API_VERSION,
)
from utils.access_token import AccessToken

import requests
import json


if __name__ == "__main__":
    # Auth Setup
    auth = AccessToken(domain=DOMAIN, payload=PAYLOAD)
    auth.generate_access_token()
    auth_header = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth.access_token}",
    }

    api_endpoint = DOMAIN + f"/services/data/v{API_VERSION}/query/?q="
    query = input("Enter your query:\n")
    query = query.replace("\n", "").replace(" ", "+").strip()

    response = requests.get(url=api_endpoint + query, headers=auth_header)
    data = response.json()

    print(json.dumps(data, indent=4))
