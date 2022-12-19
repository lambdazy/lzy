from serialzy.registry import DefaultSerializerRegistry

from lzy.serialization.file import FileSerializer


class LzySerializerRegistry(DefaultSerializerRegistry):
    def __init__(self):
        super().__init__()
        self.register_serializer(FileSerializer())
