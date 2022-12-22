import aioboto3


async def create_bucket(endpoint: str) -> None:
    async with aioboto3.Session().client(
            "s3",
            aws_access_key_id="aaa",
            aws_secret_access_key="aaa",
            endpoint_url=endpoint,
            region_name='us-east-1'
    ) as s3:
        await s3.create_bucket(Bucket="bucket")
