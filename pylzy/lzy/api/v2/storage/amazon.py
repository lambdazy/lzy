from pathlib import Path
from urllib import urlparse


class AmazonClient:
    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self._client = boto3.client(
            "s3",
            aws_access_key_id=credentials.access_token,
            aws_secret_access_key=credentials.secret_token,
            endpoint_url=credentials.endpoint,
        )
        self.__logger = logging.getLogger(self.__class__.__name__)

    async def read(self, url: str, dest: IO) -> Any:
        assert urlparse(url).scheme == "s3"

        bucket, key = bucket_from_url(url)
        self._client.download_fileobj(bucket, key, dest)

    async def write(self, bucket: str, key: str, data: IO) -> str:
        self._client.upload_fileobj(data, bucket, key)
        return self.generate_uri(bucket, key)

    async def blob_exists(self, container: str, blob: str) -> bool:
        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            self._client.head_object(Bucket=container, Key=blob)
            return True
        except ClientError:
            return False

    def generate_uri(self, bucket: str, key: str) -> str:
        path = Path(bucket) / key
        return f"s3:/{path}"
