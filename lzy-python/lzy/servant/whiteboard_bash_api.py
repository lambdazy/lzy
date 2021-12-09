from json.decoder import JSONDecodeError
from typing import TypeVar

import cloudpickle

from lzy.api.whiteboard.api import *
from lzy.servant.bash_servant_client import BashServantClient, Singleton
from urllib import request
from lzy.api._proxy import proxy

import json


class SnapshotBashApi(SnapshotApi):
    def __init__(self, mount_point: str) -> None:
        self.__mount = mount_point
    
    def create(self) -> SnapshotDescription:
        try:
            out = BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot", "create")
            res = json.loads(out)
            return SnapshotDescription(res['snapshotId'])
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {e}")
    
    def finalize(self, snapshot_id: str):
        BashServantClient._exec_bash(f"{self.__mount}/sbin/snapshot", "finalize", snapshot_id)


T = TypeVar('T')


class WhiteboardBashApi(WhiteboardApi):
    @staticmethod
    def _get_from_s3(url: str) -> Any:
        with request.urlopen(url) as req:
            return cloudpickle.load(req)

    def resolve(self, wb_id: str, field_name: str, typ: Type[T]) -> T:
        out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "resolve", wb_id, "-f", field_name)
        res = json.loads(out)
        return proxy(lambda: WhiteboardBashApi._get_from_s3(res['url']), typ)

    def __init__(self, mount_point: str) -> None:
        self.__mount = mount_point
    
    def create(self, fields: List[str], snapshot_id: str) -> WhiteboardDescription:
        try:
            out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "create", "-l", ",".join(fields), snapshot_id)
            res = json.loads(out)
            
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {e}")
    
    def link(self, wb_id: str, field_name: str, entry_id: str):
        BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "link", "-e", entry_id, "-f", field_name, wb_id)
    
    def get(self, wb_id: str) -> WhiteboardDescription:
        try:
            out = BashServantClient._exec_bash(f"{self.__mount}/sbin/whiteboard", "get", wb_id)
            res = json.loads(out)
            
            return self._parse_wb_json(res)
        except (JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Wrong command output format: {e}")
    
    @staticmethod
    def _parse_wb_json(res: dict[str, Any]) -> WhiteboardDescription:
        return WhiteboardDescription(
                res['id'],
                [WhiteboardFieldDescription(field['fieldName'], field.get('storageUri'), field.get('dependentFieldNames'), field['empty'], WhiteboardFieldStatus(field['satus'])) for field in res['fields']],
                SnapshotDescription(res['snapshot']['snapshotId']),
                WhiteboardStatus(res['status'])
            )
