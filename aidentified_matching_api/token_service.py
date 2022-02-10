# -*- coding: utf-8 -*-
# Copyright 2022 Aidentified LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import logging
import os
import pickle
import urllib.parse
import zlib

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
        endpoint_hash = hex(zlib.crc32(constants.AIDENTIFIED_URL.encode("utf-8")))[2:]
        self.cache_file = os.path.join(
            dirs.user_cache_dir, f"token_cache_{endpoint_hash}"
        )

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
            raise Exception(
                f"Unable to make API call: {resp.status_code} {resp_obj}"
            ) from None

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
