# -*- coding: utf-8 -*-
import datetime
import logging
import os
import pickle
import urllib.parse

import appdirs
import requests

import aidentified_matching_api.constants as constants


logger = logging.getLogger("api")


class TokenService:
    __slots__ = ["expires_at", "token", "cache_file"]

    def __init__(self):
        self.expires_at = 0
        self.token = ""
        dirs = appdirs.AppDirs(
            appname="aidentified_match", appauthor="Aidentified", version="1.0"
        )
        os.makedirs(dirs.user_cache_dir, exist_ok=True)
        self.cache_file = os.path.join(dirs.user_cache_dir, "token_cache")

    def _read_token_cache(self):
        try:
            with open(self.cache_file, "rb") as fd:
                token_cache = pickle.load(fd)
                self.token = token_cache.get("token", "")
                self.expires_at = token_cache.get("expires_at", 0)
        except FileNotFoundError:
            pass

    def _write_token_cache(self):
        cache_value = {"token": self.token, "expires_at": self.expires_at}
        with open(self.cache_file, "wb") as fd:
            pickle.dump(cache_value, fd, protocol=pickle.HIGHEST_PROTOCOL)

    def get_token(self, args) -> str:
        self._read_token_cache()

        if datetime.datetime.utcnow().timestamp() < self.expires_at:
            return self.token

        # N.B. these are read from envvars AID_EMAIL and
        # AID_PASSWORD by default
        login_payload = {
            "email": args.email,
            "password": args.password,
        }

        logger.info("get_token /login")
        try:
            resp = requests.post(
                f"{constants.AIDENTIFIED_URL}/login", json=login_payload
            )
        except requests.exceptions.RequestException as e:
            raise Exception(f"Unable to connect to API: {e}") from None

        try:
            resp_payload = resp.json()
        except ValueError:
            raise Exception(f"Unable to parse API response: {resp.content}") from None

        try:
            resp.raise_for_status()
        except requests.exceptions.RequestException:
            raise Exception(
                f"Bad response from API: {resp.status_code} {resp_payload}"
            ) from None

        expires_at_dt = (
            datetime.timedelta(seconds=resp_payload["expires_in"])
            + datetime.datetime.utcnow()
        )

        self.expires_at = expires_at_dt.timestamp()
        self.token = resp_payload["bearer_token"]

        self._write_token_cache()

        return self.token

    def get_auth_headers(self, args) -> dict:
        return {"Authorization": f"Bearer {self.get_token(args)}"}

    def api_call(self, args, fn, url, **kwargs):
        auth_headers = self.get_auth_headers(args)

        if "headers" in kwargs:
            kwargs["headers"].update(auth_headers)
        else:
            kwargs["headers"] = auth_headers

        logger.info(f"{fn.__name__} {url}")
        try:
            resp: requests.Response = fn(f"{constants.AIDENTIFIED_URL}{url}", **kwargs)
        except requests.RequestException as e:
            raise Exception(f"Unable to make API call: {e}") from None

        if not resp.content:
            resp_obj = {}
        else:
            try:
                resp_obj = resp.json()
            except ValueError:
                raise Exception(
                    f"Unable to make API call: invalid response {resp.content}"
                ) from None

        try:
            resp.raise_for_status()
        except requests.RequestException:
            raise Exception(f"Unable to make API call: {resp_obj}") from None

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


def get_token(args):
    if args.clear_cache:
        try:
            os.remove(token_service.cache_file)
        except FileNotFoundError:
            pass

    print(token_service.get_token(args))


token_service = TokenService()
