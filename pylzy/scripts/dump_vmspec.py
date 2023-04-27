import json
import sys
import time
import typing
import google.protobuf.json_format

from dataclasses import dataclass
from pathlib import Path
from lzy.api.v1 import op, Lzy, GpuType, Provisioning
from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec


class Stop(Exception):
    pass


@dataclass(frozen=True)
class DumpProvisioning(Provisioning):
    dump_path: Path = None

    def resolve_pool(self, pool_specs: typing.Sequence[VmPoolSpec]) -> VmPoolSpec:
        result = [google.protobuf.json_format.MessageToDict(spec) for spec in pool_specs]

        text = json.dumps(result, indent=4)

        self.dump_path.write_text(text)

        raise Stop()


@op()
def func() -> int:
    return 1


def main():
    path = Path(sys.argv[1])

    lzy = Lzy()

    try:
        with lzy.workflow(
            "dump_vmspec",
            provisioning=DumpProvisioning(dump_path=path),
            interactive=False,
        ):
            func()
    except Stop:
        pass


if __name__ == '__main__':
    main()
