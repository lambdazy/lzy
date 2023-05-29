package ai.lzy.channelmanager.v2;

import ai.lzy.v1.common.LMD;
import jakarta.annotation.Nullable;

public record Channel(
    String userId,
    String executionId,
    String id,
    LMD.DataScheme dataScheme,
    String workflowName,
    @Nullable String storageProducerUri,
    @Nullable String storageConsumerUri
) { }
