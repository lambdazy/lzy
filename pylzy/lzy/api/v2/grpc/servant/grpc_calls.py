from grpclib.client import Channel

from lzy.proto.ai.lzy.priv.v2.lzy_fs_grpc import LzyFsStub
from lzy.proto.ai.lzy.priv.v2.lzy_server_grpc import LzyServerStub
from lzy.proto.bet.ai.lzy.priv.v2 import (
    Auth,
    ChannelCommand,
    ChannelCreate,
    ChannelDestroy,
    ChannelStatus,
    CreateSlotCommand,
    Slot,
    SlotAssignment,
    SlotCommand,
    SlotCommandStatus,
    SlotMedia,
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

    async def create_channel(self, slot: Slot, channel_name: str) -> ChannelStatus:
        # LOG.info("Create channel `{}` for slot `{}`.", channelName, slot.name());
        create = ChannelCreate(content_type=slot.content_type, direct=slot.direction)
        channel_cmd = ChannelCommand(
            auth=self.auth,
            channel_name=channel_name,
            create=create,
        )
        result = await self.server.Channel(channel_cmd)
        return result

    async def destroy_channel(self, channel_name: str):
        destroy = ChannelDestroy()
        channel_cmd = ChannelCommand(
            auth=self.auth,
            channel_name=channel_name,
            destroy=destroy,
        )
        # TODO[aleksZubakov]: try/catch here?
        result = await self.server.Channel(channel_cmd)
        return result

    async def create_slot(
        self, slot: Slot, pid: int, name: str, pipe: bool, channel_id: str
    ) -> SlotCommandStatus:
        create_cmd = CreateSlotCommand(
            slot=slot,
            channel_id=channel_id,
            is_pipe=pipe,
        )
        # LOG.info("Create {}slot `{}` ({}) for channel `{}` with taskId {}.",
        #         pipe ? "pipe " : "", slotName, name, channelId, pid);
        slot_cmd = SlotCommand(
            tid=str(pid),
            slot=name,
            create=create_cmd,
        )
        result = await self.servant_fs.ConfigureSlot(slot_cmd)
        return result

    async def start(self, zygote: Zygote) -> TaskProgress:
        zygote.name += f"_{self.pid}"

        assignments = []
        for slot in zygote.slots:
            # LOG.info("Resolving slot " + slot.name());
            #
            # final String binding;
            # if (slot.media() == Slot.Media.ARG) {
            #     binding = String.join(" ", command.getArgList().subList(1, command.getArgList().size()));
            # } else if (bindings.containsKey(slot.name())) {
            #     binding = "channel:" + bindings.get(slot.name());
            # } else {
            #     binding = "channel:" + resolveChannel(slot);
            # }
            # LOG.info("Slot " + slot.name() + " resolved to " + binding);

            raise NotImplementedError("not implemented yet")
            binding = "aaa"
            assignments.append(SlotAssignment(slot, binding=binding))

        task_spec = TaskSpec(auth=self.auth, zygote=zygote, assignments=assignments)

        result = await self.server.Start(task_spec)
        return result

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
