from grpclib.client import Channel

from lzy.proto.ai.lzy.priv.v2.lzy_fs_grpc import LzyFsStub
from lzy.proto.ai.lzy.priv.v2.lzy_server_grpc import LzyServerStub
from lzy.proto.priv.v2 import (
    Auth,
    ChannelCommand,
    ChannelCreate,
    ChannelDestroy,
    ChannelStatus,
    CreateSlotCommand,
    DataScheme,
    DirectChannelSpec,
    Slot,
    SlotAssignment,
    SlotCommand,
    SlotCommandStatus,
    SlotMedia,
    SnapshotChannelSpec,
    TaskProgress,
    TaskSpec,
    TaskStatus,
    Zygote,
)


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

    async def start(self, zygote: Zygote) -> TaskProgress:
        raise NotImplementedError("not implemented yet")

        zygote.name += f"_{self.pid}"
        assignments = [
            SlotAssignment(
                slot=slot,
                binding=self.resolve_slot(slot),
            )
            for slot in zygote.slots
        ]
        task_spec = TaskSpec(
            auth=self.auth,
            zygote=zygote,
            assignments=assignments,
        )

        return await self.server.Start(task_spec)

        #  final long startTimeMillis = System.currentTimeMillis();
        #  final Iterator<Tasks.TaskProgress> executionProgress = server.start(taskSpec.build());
        #  final int[] exit = new int[] {-1};
        #  final String[] descriptionArr = new String[] {"Got no exit code"};
        #  executionProgress.forEachRemaining(progress -> {
        #      try {
        #          LOG.info(JsonFormat.printer().print(progress));
        #          if (progress.getStatus() == Tasks.TaskProgress.Status.ERROR
        #              || progress.getStatus() == Tasks.TaskProgress.Status.SUCCESS) {
        #              exit[0] = progress.getRc();
        #              descriptionArr[0] = progress.getDescription();
        #              System.in.close();
        #          }
        #      } catch (InvalidProtocolBufferException e) {
        #          LOG.warn("Unable to parse execution progress", e);
        #      } catch (IOException e) {
        #          LOG.error("Unable to close stdin", e);
        #      }
        #  });
        #  final int rc = exit[0];
        #  final String description = descriptionArr[0];
        #  final long finishTimeMillis = System.currentTimeMillis();
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

    def close(self):
        self._chan.close()
