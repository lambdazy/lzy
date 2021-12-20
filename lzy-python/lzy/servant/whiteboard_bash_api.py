import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import TypeVar
from urllib import parse

import cloudpickle
import s3fs

from lzy.api._proxy import proxy
from lzy.api.whiteboard.api import *
from lzy.servant.bash_servant_client import BashServantClient


class SnapshotBashApi(SnapshotApi):
    def __init__(self, mount_point: str) -> None:
        super().__init__()
        self.__mount = mount_point
        self._log = logging.getLogger(str(self.__class__))
    
    def create(self) -> SnapshotDescription:
        self._log.info("Creating snapshot")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot", "create")
        try:
            res = json.loads(out)
            return SnapshotDescription(res['snapshotId'])
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}")
    
    def finalize(self, snapshot_id: str):
        self._log.info(f"Finalizing snapshot {snapshot_id}")
        BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot", "finalize", snapshot_id)


T = TypeVar('T')


class WhiteboardBashApi(WhiteboardApi):
    def __init__(self, mount_point: str) -> None:
        super().__init__()
        self.__mount = mount_point
        self._log = logging.getLogger(str(self.__class__))

    def _get_from_s3(self, url: str) -> Any:
        access_token = os.getenv("S3_ACCESS_TOKEN", None)
        secret_token = os.getenv("S3_SECRET_TOKEN", None)
        if not access_token or not secret_token:
            out = BashServantClient._exec_bash(f"{self.__mount}/sbin/credentials", "s3")
            self._log.info(f"Resolved creds {out}")
            res = json.loads(out)
            access_token = res['accessToken']
            secret_token = res['secretToken']
            os.environ['S3_ACCESS_TOKEN'] = access_token  # type: ignore
            os.environ['S3_SECRET_TOKEN'] = secret_token  # type: ignore
        uri = parse.urlparse(url)
        fs = s3fs.S3FileSystem(key=access_token, secret=secret_token, client_kwargs={'endpoint_url': f"http://{uri.netloc}"})
        with fs.open(uri.path) as f:
            return cloudpickle.load(f)

    def resolve(self, field_url: str, field_type: Type[Any]) -> Any:
        self._log.info(f"Resolving field by url {field_url} to type {field_type}")
        return proxy(lambda: self._get_from_s3(field_url), field_type)
    
    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        logging.info(f"Creating whiteboard for snapshot {snapshot_id} with fields {fields}")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "create", snapshot_id, "-l", ",".join(fields))
        try:
            res = json.loads(out)
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}")
    
    def link(self, wb_id: str, field_name: str, entry_id: str):
        self._log.info(f"Linking field {field_name} of whiteboard {wb_id} to entry {entry_id}")
        BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "link", wb_id, "-e", entry_id, "-f", field_name)
    
    def get(self, wb_id: str) -> WhiteboardDescription:
        self._log.info(f"Getting whiteboard {wb_id}")
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "get", wb_id)
        try:
            res = json.loads(out)
            self._log.info(f"Received whiteboard {wb_id}: {res}")
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {out}")
    
    @staticmethod
    def _parse_wb_json(res: Dict[str, Any]) -> WhiteboardDescription:
        return WhiteboardDescription(
                res['id'],
                [WhiteboardFieldDescription(field['fieldName'], field.get('storageUri'), field.get('dependentFieldNames')) for field in res.get('fields', [])],
                SnapshotDescription(res['snapshot']['snapshotId']) if res.get('snapshot') else None,
                WhiteboardStatus(res['status']) if res.get('status') else None
            )
