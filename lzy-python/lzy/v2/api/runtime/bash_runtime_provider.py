from lzy.v2.api.runtime.runtime import Runtime
from lzy.v2.api.runtime.runtime_impl import RuntimeImpl
from lzy.v2.api.runtime.runtime_provider import RuntimeProvider
from lzy.v2.serialization.serializer import Serializer
from lzy.v2.servant.bash_servant_client import BashServantClient
from lzy.v2.servant.servant_client import CredentialsTypes
from lzy.v2.servant.v2.channel_manager import ServantChannelManager
from lzy.v2.storage.storage_client import from_credentials, StorageClient


class BashRuntimeProvider(RuntimeProvider):
    def get(self, lzy_mount: str, serializer: Serializer) -> Runtime:
        bash_servant_client = BashServantClient().instance(lzy_mount)
        bash_channel_manager = ServantChannelManager()
        bucket = bash_servant_client.get_bucket()
        credentials = bash_servant_client.get_credentials(CredentialsTypes.S3, bucket)
        storage_client: StorageClient = from_credentials(credentials)
        return RuntimeImpl(lzy_mount, serializer, bash_channel_manager, bash_servant_client, storage_client)