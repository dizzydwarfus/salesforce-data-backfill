import requests


class AccessToken:
    def __init__(
        self,
        domain: str,
        payload: dict,
        access_token: dict = None,
    ):
        self.domain = domain
        self.payload = payload
        self.access_token = access_token

    def generate_access_token(
        self,
    ) -> dict:
        response = requests.post(
            f"{self.domain}/services/oauth2/token", data=self.payload
        )
        response.raise_for_status()
        self.access_token = response.json()["access_token"]
        return response.json()

    def upsert_access_token(self, access_token: str):
        with open(".env", "r") as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if line.startswith("ACCESS_TOKEN"):
                    lines[i] = f"ACCESS_TOKEN = {access_token}\n"
                    break
        with open(".env", "w") as file:
            file.writelines(lines)

        return 0

    def __str__(self):
        return f"Access token: {self.access_token}"
