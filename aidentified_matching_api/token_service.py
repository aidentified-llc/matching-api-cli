import os
import time
import requests
import urllib.parse

import aidentified_matching_api.constants as constants


class TokenService:
    __slots__ = ["expires_at", "token"]

    def __init__(self):
        self.expires_at = 0
        self.token = ""

    def get_token(self, args) -> str:
        # For local debugging.
        env_token = os.environ.get('AID_TOKEN')
        if env_token is not None:
            return env_token

        if time.monotonic() < self.expires_at:
            return self.token

        login_payload = {
            "email": args.email,
            "password": args.password,
        }

        resp = requests.post(f"{constants.AIDENTIFIED_URL}/login", json=login_payload)
        resp.raise_for_status()
        resp_payload = resp.json()

        self.expires_at = resp_payload["expires_in"] + time.monotonic()
        self.token = resp_payload["bearer_token"]

        return self.token

    def get_auth_headers(self, args) -> dict:
        return {"Authorization": f"Bearer {self.get_token(args)}"}

    def api_call(self, args, fn, url, **kwargs):
        auth_headers = self.get_auth_headers(args)

        if "headers" in kwargs:
            kwargs["headers"].update(auth_headers)
        else:
            kwargs["headers"] = auth_headers

        try:
            resp = fn(f"{constants.AIDENTIFIED_URL}{url}", **kwargs)
            resp_obj = resp.json()
        except requests.RequestException:
            raise Exception("Unable to make API call")

        try:
            resp.raise_for_status()
        except requests.RequestException:
            raise Exception(f"Unable to make API call: {resp_obj}")

        return resp_obj

    def paginated_api_call(self, args, fn, url, **kwargs):
        resp = []
        fetch_url = url
        parsed_orig_url = urllib.parse.urlparse(url)

        while True:
            paged = self.api_call(args, fn, fetch_url, **kwargs)
            resp.extend(paged["results"])
            if paged["next"] is None:
                break

            parsed_page_url = urllib.parse.urlparse(paged["next"])
            parsed_orig_url = parsed_orig_url._replace(query=parsed_page_url.query)
            fetch_url = parsed_orig_url.geturl()

        return resp



token_service = TokenService()
