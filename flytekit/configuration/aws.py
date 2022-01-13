from flytekit.configuration import common as _config_common

S3_SHARD_FORMATTER = _config_common.FlyteRequiredStringConfigurationEntry("aws", "s3_shard_formatter")

S3_SHARD_STRING_LENGTH = _config_common.FlyteIntegerConfigurationEntry("aws", "s3_shard_string_length", default=2)

S3_ENDPOINT = _config_common.FlyteStringConfigurationEntry("aws", "endpoint", default=None)

S3_ACCESS_KEY_ID = _config_common.FlyteStringConfigurationEntry("aws", "access_key_id", default=None)

S3_SECRET_ACCESS_KEY = _config_common.FlyteStringConfigurationEntry("aws", "secret_access_key", default=None)

S3_ACCESS_KEY_ID_ENV_NAME = "AWS_ACCESS_KEY_ID"

S3_SECRET_ACCESS_KEY_ENV_NAME = "AWS_SECRET_ACCESS_KEY"

S3_ENDPOINT_ARG_NAME = "--endpoint-url"

S3_LATCH_AUTHENTICATION_ENDPOINT = _config_common.FlyteStringConfigurationEntry("aws", "latch_authentication_endpoint", default="https://nucleus.latch.bio")

S3_FLYTE_BUCKET = _config_common.FlyteStringConfigurationEntry("aws", "flyte_bucket", default="prod-borg-prod")

S3_UPLOAD_CHUNK_SIZE_BYTES = _config_common.FlyteIntegerConfigurationEntry("aws", "upload_chunk_size_bytes", default=10000000)

ENABLE_DEBUG = _config_common.FlyteBoolConfigurationEntry("aws", "enable_debug", default=False)

RETRIES = _config_common.FlyteIntegerConfigurationEntry("aws", "retries", default=3)

BACKOFF_SECONDS = _config_common.FlyteIntegerConfigurationEntry("aws", "backoff_seconds", default=5)
