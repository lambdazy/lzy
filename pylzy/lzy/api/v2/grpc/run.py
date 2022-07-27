from typing import Tuple

import dataclass
from grpclib.client import Channel

from ai.lzy.v1.auth_pb2 import Auth
from ai.lzy.v1.task_pb2 import TaskSpec, TaskProgress, SlotAssignment
from ai.lzy.v1.zygote_pb2 import Zygote
from ai.lzy.v1.fs_grpc import LzyFsStub
from ai.lzy.v1.server_grpc import LzyServerStub


@dataclass.dataclass
class Measurement:
    time_ns: int = 0


def creds():
    raise NotImplementedError("not implemented yet")
    return "localhost", "7777"


class Run:
    def __init__(self):
        self._chan = Channel(*creds())
        self.auth = Auth("", "")  # TODO
        self.server = LzyServerStub(self._chan)
        self.servant_fs = LzyFsStub(self._chan)
        self.pid = -1

    async def exec(self, zygote: Zygote) -> Measurement:
        raise NotImplementedError("not implemented yet")

        # preparing
        zygote.name += f"_{self.pid}"
        assignments = [
            SlotAssignment(
                slot=slot,
                # TODO[ottergottaott]: normal resolve call
                binding=None,  # self.resolve_slot(slot),
            )
            for slot in zygote.slots
        ]
        task_spec = TaskSpec(
            auth=self.auth,
            zygote=zygote,
            assignments=assignments,
        )

        # run
        # TODO[ottergottaott]: measure time
        rc, description = await self.start(task_spec)

        #  MetricEventLogger.log(
        #      new MetricEvent(
        #          "time from Task start to Task finish",
        #          Map.of("metric_type", "task_metric"),
        #          finishTimeMillis - startTimeMillis
        #      )
        #  );
        #  LOG.info("Run:: Task finished RC = {}, Description = {}", rc, description);
        #  if (rc != 0) {
        #      System.err.print(description);
        #  } else {
        #      communicationLatch.await(); // waiting for slots to finish communication
        #  }
        #  channels.forEach(this::destroyChannel);
        #  return rc;
        # }

        return Measurement()

    async def start(self, task_spec: TaskSpec) -> Tuple[int, str]:
        exit: int = 0
        description: str = ""

        with self.server.Start.open() as stream:
            await stream.send_request()
            await stream.send_message(task_spec)

            async for task_progress in stream:
                task_progress: TaskProgress = task_progress
                if (
                    task_progress.status is TaskProgress.ERROR
                    or task_progress.status is TaskProgress.SUCCESS
                ):
                    # TODO[ottergottaott]: pretty print here
                    print(task_progress)
                    rc_ = task_progress.rc
                    description = task_progress.description

            return rc_, description

    def close(self):
        self._chan.close()
