import logging
import os as _os
import re as _re
import string as _string
import sys as _sys
import time
import uuid as _uuid
from typing import Dict, List
import requests
import urllib
from six import moves as _six_moves
from six import text_type as _text_type

from flytekit.common.exceptions.user import FlyteUserException as _FlyteUserException
from flytekit.configuration import aws as _aws_config
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import common as _common_data

if _sys.version_info >= (3,):
    from shutil import which as _which
else:
    from distutils.spawn import find_executable as _which


class AwsS3Proxy(_common_data.DataProxy):
    _AWS_CLI = "aws"
    _SHARD_CHARACTERS = [_text_type(x) for x in _six_moves.range(10)] + list(_string.ascii_lowercase)

    def __init__(self, raw_output_data_prefix_override: str = None):
        """
        :param raw_output_data_prefix_override: Instead of relying on the AWS or GCS configuration (see
            S3_SHARD_FORMATTER for AWS and GCS_PREFIX for GCP) setting when computing the shard
            path (_get_shard_path), use this prefix instead as a base. This code assumes that the
            path passed in is correct. That is, an S3 path won't be passed in when running on GCP.
        """
        self._raw_output_data_prefix_override = raw_output_data_prefix_override
        self._latch_endpoint = _os.environ.get("LATCH_AUTHENTICATION_ENDPOINT")

    @property
    def raw_output_data_prefix_override(self) -> str:
        return self._raw_output_data_prefix_override

    @staticmethod
    def _split_s3_path_to_bucket_and_key(path):
        """
        :param Text path:
        :rtype: (Text, Text)
        """
        path = path[len("s3://") :]
        first_slash = path.index("/")
        return path[:first_slash], path[first_slash + 1 :]

    def exists(self, remote_path):
        """
        :param Text remote_path: remote s3:// path
        :rtype bool: whether the s3 file exists or not
        """

        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        r = requests.post(self._latch_endpoint + "/api/object-exists-at-url", json={"object_url": remote_path, "project_name_claim": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
        if r.status_code != 200:
            raise _FlyteUserException("failed to check if object exists at url `{}`".format(remote_path))
        
        return r.json()["exists"]

    def download_directory(self, remote_path, local_path):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """

        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")
        
        __, dir_key = self._split_s3_path_to_bucket_and_key(remote_path)
        if dir_key[-1] != "/":
            dir_key += "/"

        r = requests.post(self._latch_endpoint + "/api/get-presigned-urls-for-dir", json={"object_url": remote_path, "project_name_claim": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
        if r.status_code != 200:
            raise _FlyteUserException("failed to download `{}`".format(remote_path))
        
        key_to_url_map = r.json()["key_to_url_map"]
        for key, url in key_to_url_map.items():
            local_file_path = _os.path.join(local_path, key.replace(dir_key, ""))
            _os.makedirs(local_file_path, exist_ok=True)
            urllib.urlretrieve(url, local_file_path)

    def download(self, remote_path, local_path):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """
        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        r = requests.post(self._latch_endpoint + "/api/get-presigned-url", json={"object_url": remote_path, "project_name_claim": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
        if r.status_code != 200:
            raise _FlyteUserException("failed to download `{}`".format(remote_path))
        
        url = r.json()["url"]
        urllib.urlretrieve(url, local_path)

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """

        r = requests.post(self._latch_endpoint + "/api/get-upload-url", json={"object_url": to_path, "project_name_claim": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
        if r.status_code != 200:
            raise _FlyteUserException("failed to get presigned upload url for `{}`".format(to_path))

        data = r.json()
        files = { "file": open(file_path, "rb")}
        r = requests.post(data["url"], data=data["fields"], files=files)
        if r.status_code != 200:
            raise _FlyteUserException("failed to upload `{}` to `{}`".format(file_path, data["url"]))

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """

        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        r = requests.post(self._latch_endpoint + "/api/get-upload-url-for-dir", json={"object_url": remote_path, "project_name_claim": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
        if r.status_code != 200:
            raise _FlyteUserException("failed to get presigned upload url for `{}`".format(remote_path))
        
        url = r.json()["url"]

        files_to_upload = [_os.path.join(dp, f) for dp, __, filenames in _os.walk(local_path) for f in filenames]

        for file_to_upload in files_to_upload:
            fields = r.json()["fields"]
            fields["key"] = file_to_upload.replace(local_path, "")
            r = requests.post(url, data=fields, files={ "file": open(file_to_upload, "rb")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to upload `{}` to `{}`".format(file_to_upload, url))


    def get_random_path(self):
        """
        :rtype: Text
        """
        # Create a 128-bit random hash because the birthday attack principle shows that there is about a 50% chance of a
        # collision between objects when 2^(n/2) objects are created (where n is the number of bits in the hash).
        # Assuming Flyte eventually creates 1 trillion pieces of data (~2 ^ 40), the likelihood
        # of a collision is 10^-15 with 128-bit...or basically 0.
        key = _uuid.UUID(int=_flyte_random.random.getrandbits(128)).hex
        return _os.path.join(self._get_shard_path(), key)

    def get_random_directory(self):
        """
        :rtype: Text
        """
        return self.get_random_path() + "/"

    def _get_shard_path(self) -> str:
        """
        If this object was created with a raw output data prefix, usually set by Propeller/Plugins at execution time
        and piped all the way here, it will be used instead of referencing the S3 shard configuration.
        """
        if self.raw_output_data_prefix_override:
            return self.raw_output_data_prefix_override

        shard = ""
        for _ in _six_moves.range(_aws_config.S3_SHARD_STRING_LENGTH.get()):
            shard += _flyte_random.random.choice(self._SHARD_CHARACTERS)
        return _aws_config.S3_SHARD_FORMATTER.get().format(shard)
