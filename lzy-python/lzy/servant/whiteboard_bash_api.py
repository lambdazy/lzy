import json
import logging
from json.decoder import JSONDecodeError
from typing import Any, Type, Dict, List, TypeVar

# noinspection PyProtectedMember
from lzy.api._proxy import proxy
from lzy.api.whiteboard.credentials import StorageCredentials
from lzy.api.whiteboard.model import (
    SnapshotApi,
    SnapshotDescription,
    WhiteboardApi,
    WhiteboardDescription,
    WhiteboardStatus,
    WhiteboardFieldDescription, get_bucket_from_url
)
from lzy.servant.bash_servant_client import exec_bash
from lzy.servant.servant_client import ServantClient, CredentialsTypes
from lzy.servant.whiteboard_storage import WhiteboardStorage


class SnapshotBashApi(SnapshotApi):
    def __init__(self, mount_point: str) -> None:
        super().__init__()
        self.__mount = mount_point
        self._log = logging.getLogger(str(self.__class__))

    def create(self) -> SnapshotDescription:
        self._log.info("Creating snapshot")
        out = exec_bash(f"{self.__mount}/sbin/snapshot", "create")
        try:
            res = json.loads(out)
            return SnapshotDescription(res["snapshotId"])
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    def finalize(self, snapshot_id: str):
        self._log.info(f"Finalizing snapshot {snapshot_id}")
        exec_bash(
            f"{self.__mount}/sbin/snapshot", "finalize", snapshot_id
        )


T = TypeVar("T")  # pylint: disable=invalid-name


class WhiteboardBashApi(WhiteboardApi):
    def __init__(self, mount_point: str, client: ServantClient) -> None:
        super().__init__()
        self.__mount = mount_point
        self.__client = client
        self._log = logging.getLogger(str(self.__class__))
        self.__credentials: Dict[str, StorageCredentials] = {}
        self.__whiteboard_storage_by_bucket: Dict[str, WhiteboardStorage] = {}

    def _whiteboard_storage(self, bucket: str) -> WhiteboardStorage:
        if bucket not in self.__credentials:
            self.__credentials[bucket] = self.__client.get_credentials(
                CredentialsTypes.S3,
                bucket
            )
        if bucket not in self.__whiteboard_storage_by_bucket:
            self.__whiteboard_storage_by_bucket[bucket] = WhiteboardStorage.create(self.__credentials[bucket])
        return self.__whiteboard_storage_by_bucket[bucket]

    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        self._log.info(f"Resolving field by url {field_url} to type {field_type}")

        bucket = get_bucket_from_url(field_url)
        return proxy(
            lambda: self._whiteboard_storage(bucket).read(field_url), field_type
        )  # type: ignore[no-any-return]

    def create(self, fields: List[str], snapshot_id: str, namespace: str, tags: List[str]) -> WhiteboardDescription:
        logging.info(
            f"Creating whiteboard for snapshot {snapshot_id} with fields {fields}, namespace {namespace}, tags {tags}"
        )
        command = " ".join([f"{self.__mount}/sbin/whiteboard", "create", snapshot_id, "-l", ",".join(fields)])
        if len(tags) > 0:
            command = " ".join([command, "-t", ",".join(tags)])
        if namespace:
            command = " ".join([command, "-n", namespace])
        out = exec_bash(command)
        try:
            res = json.loads(out)
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    def link(self, wb_id: str, field_name: str, entry_id: str):
        self._log.info(
            f"Linking field {field_name} of whiteboard {wb_id} to entry {entry_id}"
        )
        exec_bash(
            f"{self.__mount}/sbin/whiteboard",
            "link",
            wb_id,
            "-e",
            entry_id,
            "-f",
            field_name,
        )

    def get(self, wb_id: str) -> WhiteboardDescription:
        self._log.info(f"Getting whiteboard {wb_id}")
        out = exec_bash(
            f"{self.__mount}/sbin/whiteboard", "get", wb_id
        )
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboard {wb_id}: {res}")
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err

    @staticmethod
    def _parse_wb_json(res: Dict[str, Any]) -> WhiteboardDescription:
        fields = [
            WhiteboardFieldDescription(
                field["fieldName"],
                field["status"],
                field.get("dependentFieldNames"),
                field.get("storageUri")
            )
            for field in res.get("fields", [])
        ]
        snapshot = SnapshotDescription(res["snapshot"]["snapshotId"])
        status = WhiteboardStatus(res["status"])
        return WhiteboardDescription(res["id"], fields, snapshot, status)

    @staticmethod
    def _parse_wb_json_list(res: Dict[str, Any]) -> List[WhiteboardDescription]:
        return [WhiteboardBashApi._parse_wb_json(whiteboard) for whiteboard in res.get("whiteboards", [])]

    def list(self, namespace: str, tags: List[str]) -> List[WhiteboardDescription]:
        self._log.info(f"Getting whiteboards in namespace {namespace} with tags {tags}")
        command = " ".join([f"{self.__mount}/sbin/whiteboard", "list"])
        if tags:
            command = " ".join([command, "-t", ",".join(tags)])
        if namespace:
            command = " ".join([command, "-n", namespace])
        out = exec_bash(command)
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboards in namespace {namespace} with tags {tags}: {res}")
            return self._parse_wb_json_list(res)
        except (JSONDecodeError, KeyError) as err:
            raise RuntimeError(f"Wrong command output format: {out}") from err
