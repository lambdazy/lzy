import json
import logging
from json.decoder import JSONDecodeError

from lzy.api._proxy import proxy
from lzy.api.whiteboard.api import *
from lzy.api.whiteboard.credentials import StorageCredentials
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.whiteboard_storage import WhiteboardStorage


class SnapshotBashApi(SnapshotApi):
    def __init__(self, mount_point: str) -> None:
        super().__init__()
        self.__mount = mount_point
        self._log = logging.getLogger(str(self.__class__))

    def create(self) -> SnapshotDescription:
        self._log.info("Creating snapshot")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot",
                                           "create")
        try:
            res = json.loads(out)
            return SnapshotDescription(res['snapshotId'])
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}") from e

    def finalize(self, snapshot_id: str):
        self._log.info(f"Finalizing snapshot {snapshot_id}")
        BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot",
                                     "finalize", snapshot_id)


T = TypeVar('T')


class WhiteboardBashApi(WhiteboardApi):
    def __init__(self, mount_point: str, client: ServantClient) -> None:
        super().__init__()
        self.__mount = mount_point
        self.__client = client
        self._log = logging.getLogger(str(self.__class__))
        self.__credentials: Optional[StorageCredentials] = None
        self.__whiteboard_storage: Optional[WhiteboardStorage] = None

    @property
    def _whiteboard_storage(self) -> WhiteboardStorage:
        if not self.__credentials:
            self.__credentials = self.__client.get_credentials(ServantClient.CredentialsTypes.S3)
        if not self.__whiteboard_storage:
            self.__whiteboard_storage = WhiteboardStorage.create(self.__credentials)
        return self.__whiteboard_storage

    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        self._log.info(f"Resolving field by url {field_url} to type {field_type}")

        return proxy(lambda: self._whiteboard_storage.read(field_url), field_type) # type: ignore[no-any-return]
    
    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        logging.info(f"Creating whiteboard for snapshot {snapshot_id} with fields {fields}")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "create", snapshot_id, "-l", ",".join(fields))
        try:
            res = json.loads(out)
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}") from e

    def link(self, wb_id: str, field_name: str, entry_id: str):
        self._log.info(
            f"Linking field {field_name} of whiteboard {wb_id} to entry {entry_id}")
        BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "link",
                                     wb_id, "-e", entry_id, "-f", field_name)

    def get(self, wb_id: str) -> WhiteboardDescription:
        self._log.info(f"Getting whiteboard {wb_id}")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard",
                                           "get", wb_id)
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboard {wb_id}: {res}")
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}") from e

    def getAll(self) -> List[WhiteboardInfo]:
        self._log.info("Getting all whiteboards")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "getAll")
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboards info: {res}")
            return self._parse_wb_info_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}") from e

    @staticmethod
    def _parse_wb_json(res: Dict[str, Any]) -> WhiteboardDescription:
        fields = [
            WhiteboardFieldDescription(field['fieldName'],
                                       field.get('storageUri'),
                                       field.get('dependentFieldNames'))
            for field in res.get('fields', [])
        ]

        snapshot = None
        if res.get('snapshot') is not None:
            snapshot = SnapshotDescription(res['snapshot']['snapshotId'])

        status = None
        if res.get('status') is not None:
            status = WhiteboardStatus(res['status'])

        return WhiteboardDescription(res['id'], fields, snapshot, status)

    @staticmethod
    def _parse_wb_info_json(res: Dict[str, Any]) -> List[WhiteboardInfo]:
        return [WhiteboardInfo(field['id'], field.get('whiteboardStatus')) for field in res.get('whiteboards', [])]
