import base64
import json
import logging
import tempfile
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, cast

# noinspection PyProtectedMember
import cloudpickle
from lzy.api.v1.servant.model.slot import DataSchema

from lzy._proxy import proxy_optional
from lzy.api.v1.servant.bash_servant_client import exec_bash
from lzy.api.v1.servant.servant_client import CredentialsTypes, ServantClient
from lzy.api.v1.servant.whiteboard_storage import WhiteboardStorage
from lzy.api.v1.utils import infer_real_type
from lzy.api.v1.whiteboard.model import (
    SnapshotApi,
    SnapshotDescription,
    WhiteboardApi,
    WhiteboardDescription,
    WhiteboardFieldDescription,
    WhiteboardFieldStatus,
    WhiteboardStatus,
    get_bucket_from_url,
)
from lzy.serialization.serializer import FileSerializer
from lzy.storage.credentials import StorageCredentials


class SnapshotBashApi(SnapshotApi):
    def __init__(self, mount_point: str) -> None:
        super().__init__()
        self.__mount = mount_point
        self._log = logging.getLogger(str(self.__class__))

    def create(self, workflow_name: str) -> SnapshotDescription:
        self._log.info("Creating snapshot")
        out = exec_bash(f"{self.__mount}/sbin/snapshot", "create", workflow_name)
        try:
            res = json.loads(out)
            return SnapshotDescription(res["snapshotId"])
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    def finalize(self, snapshot_id: str):
        self._log.info(f"Finalizing snapshot {snapshot_id}")
        exec_bash(f"{self.__mount}/sbin/snapshot", "finalize", snapshot_id)

    def last(self, workflow_name: str) -> Optional[SnapshotDescription]:
        ret = exec_bash(f"{self.__mount}/sbin/snapshot", "last", workflow_name)
        data: Optional[str] = json.loads(ret).get("snapshotId")
        return SnapshotDescription(data) if data else None


T = TypeVar("T")  # pylint: disable=invalid-name


class WhiteboardBashApi(WhiteboardApi):
    def __init__(
        self, mount_point: str, client: ServantClient, serializer: FileSerializer
    ) -> None:
        super().__init__()
        self._mount = mount_point
        self._client = client
        self._log = logging.getLogger(str(self.__class__))
        self._credentials: Dict[str, StorageCredentials] = {}
        self._whiteboard_storage_by_bucket: Dict[str, WhiteboardStorage] = {}
        self._serializer = serializer

    def _whiteboard_storage(self, bucket: str) -> WhiteboardStorage:
        if bucket not in self._credentials:
            self._credentials[bucket] = self._client.get_credentials(
                CredentialsTypes.S3, bucket
            )
        if bucket not in self._whiteboard_storage_by_bucket:
            self._whiteboard_storage_by_bucket[bucket] = WhiteboardStorage.create(
                self._credentials[bucket]
            )
        return self._whiteboard_storage_by_bucket[bucket]

    def resolve(self, field_url: str, field_type: Type[T]) -> T:
        self._log.info(f"Resolving field by url {field_url} to type {field_type}")

        real_type: Type[T] = infer_real_type(field_type)
        bucket = get_bucket_from_url(field_url)
        with tempfile.TemporaryFile() as file:
            # TODO(aleksZubakov): do we need retry here?
            self._whiteboard_storage(bucket).read(field_url, file)
            file.seek(0)
            obj = self._serializer.deserialize_from_file(file, real_type)
        return obj

    def create(
        self,
        fields: List[Tuple[str, type]],
        snapshot_id: str,
        namespace: str,
        tags: List[str],
    ) -> WhiteboardDescription:
        self._log.info(
            f"Creating whiteboard for snapshot {snapshot_id} with fields {fields}, namespace {namespace}, tags {tags}"
        )
        # fmt: off
        command = [
            f"{self._mount}/sbin/whiteboard",
            "create", snapshot_id,
            "-l", ",".join(name for name, _ in fields),
        ]
        # fmt: on
        if tags:
            command.extend(["-t", ",".join(tags)])
        if namespace:
            command.extend(["-n", namespace])

        out = exec_bash(*command)
        try:
            res = json.loads(out)
            return self._parse_wb_json(res, self._log)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    def link(self, wb_id: str, field_name: str, entry_id: str):
        self._log.info(
            f"Linking field {field_name} of whiteboard {wb_id} to entry {entry_id}"
        )
        # fmt: off
        exec_bash(
            f"{self._mount}/sbin/whiteboard",
            "link", wb_id,
            "-e", entry_id,
            "-f", field_name,
        )
        # fmt: on

    def get(self, wb_id: str) -> WhiteboardDescription:
        self._log.info(f"Getting whiteboard {wb_id}")
        out = exec_bash(f"{self._mount}/sbin/whiteboard", "get", wb_id)
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboard {wb_id}: {res}")
            return self._parse_wb_json(res, self._log)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    @staticmethod
    def _parse_wb_json(res: Dict[str, Any], log) -> WhiteboardDescription:
        fields = []
        for field in res.get("fields", []):
            data_schema = None
            uri = None
            if "scheme" in field:
                data_schema = DataSchema(field["scheme"]["type"], field["scheme"].get("schemeType", "plain"))
                uri = field["storageUri"]

            fields.append(
                WhiteboardFieldDescription(
                    field["fieldName"],
                    WhiteboardFieldStatus(field["status"]),
                    field.get("dependentFieldNames"),
                    uri,
                    data_schema,
                )
            )
        snapshot = SnapshotDescription(res["snapshot"]["snapshotId"])
        status = WhiteboardStatus(res["status"])
        wb_desc = WhiteboardDescription(res["id"], fields, snapshot, status)
        log.info(f"Built wb: {wb_desc}")
        return wb_desc

    @staticmethod
    def _parse_wb_json_list(res: Dict[str, Any], log) -> List[WhiteboardDescription]:
        return [
            WhiteboardBashApi._parse_wb_json(whiteboard, log)
            for whiteboard in res.get("whiteboards", [])
        ]

    def list(
        self,
        namespace: str,
        tags: List[str],
        from_date: datetime = None,
        to_date: datetime = None,
    ) -> List[WhiteboardDescription]:
        self._log.info(f"Getting whiteboards in namespace {namespace} with tags {tags}")
        command = [f"{self._mount}/sbin/whiteboard", "list"]
        if tags:
            command.extend(["-t", ",".join(tags)])
        if namespace:
            command.extend(["-n", namespace])
        if from_date:
            command.extend(["-from", str(int(from_date.timestamp()))])
        if to_date:
            command.extend(["-to", str(int(to_date.timestamp()))])

        out = exec_bash(*command)
        try:
            res = json.loads(out)
            self._log.info(
                f"Received whiteboards in namespace {namespace} with tags {tags}: {res}"
            )
            return self._parse_wb_json_list(res, self._log)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err
