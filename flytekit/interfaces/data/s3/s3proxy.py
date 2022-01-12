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
from flytekit.tools import subprocess as _subprocess


if _sys.version_info >= (3,):
    from shutil import which as _which
else:
    from distutils.spawn import find_executable as _which

def _update_cmd_config_and_execute(cmd: List[str]):
    env = _os.environ.copy()

    if _aws_config.ENABLE_DEBUG.get():
        cmd.insert(1, "--debug")

    if _aws_config.S3_ENDPOINT.get() is not None:
        cmd.insert(1, _aws_config.S3_ENDPOINT.get())
        cmd.insert(1, _aws_config.S3_ENDPOINT_ARG_NAME)

    if _aws_config.S3_ACCESS_KEY_ID.get() is not None:
        env[_aws_config.S3_ACCESS_KEY_ID_ENV_NAME] = _aws_config.S3_ACCESS_KEY_ID.get()

    if _aws_config.S3_SECRET_ACCESS_KEY.get() is not None:
        env[_aws_config.S3_SECRET_ACCESS_KEY_ENV_NAME] = _aws_config.S3_SECRET_ACCESS_KEY.get()

    retry = 0
    while True:
        try:
            return _subprocess.check_call(cmd, env=env)
        except Exception as e:
            logging.error(f"Exception when trying to execute {cmd}, reason: {str(e)}")
            retry += 1
            if retry > _aws_config.RETRIES.get():
                raise
            secs = _aws_config.BACKOFF_SECONDS.get()
            logging.info(f"Sleeping before retrying again, after {secs} seconds")
            time.sleep(secs)
            logging.info("Retrying again")

def _extra_args(extra_args: Dict[str, str]) -> List[str]:
    cmd = []
    if "ContentType" in extra_args:
        cmd += ["--content-type", extra_args["ContentType"]]
    if "ContentEncoding" in extra_args:
        cmd += ["--content-encoding", extra_args["ContentEncoding"]]
    if "ACL" in extra_args:
        cmd += ["--acl", extra_args["ACL"]]
    return cmd

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
        self._latch_endpoint = _aws_config.S3_LATCH_AUTHENTICATION_ENDPOINT.get()

    @property
    def raw_output_data_prefix_override(self) -> str:
        return self._raw_output_data_prefix_override

    @staticmethod
    def _check_binary():
        """
        Make sure that the AWS cli is present
        """
        if not _which(AwsS3Proxy._AWS_CLI):
            raise _FlyteUserException("AWS CLI not found at Please install.")

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

        bucket, file_path = self._split_s3_path_to_bucket_and_key(remote_path)
        if "ldata-managed" in bucket:
            r = requests.post(self._latch_endpoint + "/api/object-exists-at-url", json={"object_url": remote_path, "project_name": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to check if object exists at url `{}`".format(remote_path))
            
            return r.json()["exists"]
        else:
            AwsS3Proxy._check_binary()
            cmd = [
                AwsS3Proxy._AWS_CLI,
                "s3api",
                "head-object",
                "--bucket",
                bucket,
                "--key",
                file_path,
            ]
            try:
                _update_cmd_config_and_execute(cmd)
                return True
            except Exception as ex:
                # The s3api command returns an error if the object does not exist. The error message contains
                # the http status code: "An error occurred (404) when calling the HeadObject operation: Not Found"
                #  This is a best effort for returning if the object does not exist by searching
                # for existence of (404) in the error message. This should not be needed when we get off the cli and use lib
                if _re.search("(404)", _text_type(ex)):
                    return False
                else:
                    raise ex

    def download_directory(self, remote_path, local_path):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """
        print(remote_path)
        print(local_path)
        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")
        
        bucket, dir_key = self._split_s3_path_to_bucket_and_key(remote_path)
        if "ldata-managed" in bucket:
            print("downloading dir from ldata-managed")
            if dir_key[-1] != "/":
                dir_key += "/"

            r = requests.post(self._latch_endpoint + "/api/get-presigned-urls-for-dir", json={"object_url": remote_path, "project_name": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to download `{}`".format(remote_path))
            
            key_to_url_map = r.json()["key_to_url_map"]
            for key, url in key_to_url_map.items():
                local_file_path = _os.path.join(local_path, key.replace(dir_key, ""))
                print(local_file_path)
                print(key)
                _os.makedirs(local_file_path, exist_ok=True)
                urllib.request.urlretrieve(url, local_file_path)
                assert _os.path.exists(local_file_path)
            return True
        else:
            AwsS3Proxy._check_binary()
            cmd = [AwsS3Proxy._AWS_CLI, "s3", "cp", "--recursive", remote_path, local_path]
            return _update_cmd_config_and_execute(cmd)

    def download(self, remote_path, local_path):
        """
        :param Text remote_path: remote s3:// path
        :param Text local_path: directory to copy to
        """

        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        bucket, __ = self._split_s3_path_to_bucket_and_key(remote_path)

        if "ldata-managed" in bucket:
            print("downloading file from ldata-managed")
            r = requests.post(self._latch_endpoint + "/api/get-presigned-url", json={"object_url": remote_path, "project_name": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to get presigned url for `{}`".format(remote_path))
            
            url = r.json()["url"]
            urllib.request.urlretrieve(url, local_path)
            print(local_path)
            print(_os.path.exists(local_path))
            return _os.path.exists(local_path)
        else:
            AwsS3Proxy._check_binary()
            cmd = [AwsS3Proxy._AWS_CLI, "s3", "cp", remote_path, local_path]
            return _update_cmd_config_and_execute(cmd)

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """

        bucket, __ = self._split_s3_path_to_bucket_and_key(to_path)

        if "ldata-managed" in bucket:
            print("uploading file to ldata-managed")
            r = requests.post(self._latch_endpoint + "/api/get-upload-url", json={"object_url": to_path, "project_name": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to get presigned upload url for `{}`".format(to_path))

            data = r.json()["res"]
            files = { "file": open(file_path, "rb")}
            r = requests.post(data["url"], data=data["fields"], files=files)
            if r.status_code != 200:
                raise _FlyteUserException("failed to upload `{}` to `{}`".format(file_path, data["url"]))
            return True
        else:
            AwsS3Proxy._check_binary()

            extra_args = {
                "ACL": "bucket-owner-full-control",
            }

            cmd = [AwsS3Proxy._AWS_CLI, "s3", "cp"]
            cmd.extend(_extra_args(extra_args))
            cmd += [file_path, to_path]

            return _update_cmd_config_and_execute(cmd)

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        if not remote_path.startswith("s3://"):
            raise ValueError("Not an S3 ARN. Please use FQN (S3 ARN) of the format s3://...")

        if "ldata-managed" in remote_path:
            print("uploading directory to ldata-managed")
            r = requests.post(self._latch_endpoint + "/api/get-upload-url-for-dir", json={"object_url": remote_path, "project_name": _os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")})
            if r.status_code != 200:
                raise _FlyteUserException("failed to get presigned upload url for `{}`".format(remote_path))
            
            data = r.json()["res"]

            files_to_upload = [_os.path.join(dp, f) for dp, __, filenames in _os.walk(local_path) for f in filenames]

            for file_to_upload in files_to_upload:
                fields = data["fields"]
                fields["key"] = file_to_upload.replace(local_path, "")
                r = requests.post(data["url"], data=fields, files={ "file": open(file_to_upload, "rb")})
                if r.status_code != 200:
                    raise _FlyteUserException("failed to upload `{}` to `{}`".format(file_to_upload, data["url"]))
            return True
        else:
            extra_args = {
                "ACL": "bucket-owner-full-control",
            }

            AwsS3Proxy._check_binary()
            cmd = [AwsS3Proxy._AWS_CLI, "s3", "cp", "--recursive"]
            cmd.extend(_extra_args(extra_args))
            cmd += [local_path, remote_path]
            return _update_cmd_config_and_execute(cmd)


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
