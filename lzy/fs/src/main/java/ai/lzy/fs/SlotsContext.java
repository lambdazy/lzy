package ai.lzy.fs;

import ai.lzy.fs.transfers.TransferFactory;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;

public record SlotsContext(
    LzyChannelManagerBlockingStub channelManager,
    TransferFactory transferFactory,
    String apiUrl,
    SlotsService slotsService,
    String executionId,
    SlotsExecutionContext executionContext
) {}
