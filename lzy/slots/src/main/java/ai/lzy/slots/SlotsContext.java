package ai.lzy.slots;

import ai.lzy.slots.transfers.TransferFactory;
import ai.lzy.v1.channel.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;

public record SlotsContext(
    LzyChannelManagerBlockingStub channelManager,
    TransferFactory transferFactory,
    String apiUrl,
    SlotsService slotsService,
    String executeRequestId,
    String executionId,
    String taskId,
    SlotsExecutionContext executionContext
) {}
