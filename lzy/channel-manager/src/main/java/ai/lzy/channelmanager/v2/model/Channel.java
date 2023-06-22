package ai.lzy.channelmanager.v2.model;

import ai.lzy.v1.common.LMD;
import jakarta.annotation.Nullable;

public record Channel(
    String id,
    String userId,
    String workflowName,
    String executionId,
    LMD.DataScheme dataScheme,
    @Nullable String storageProducerUri,
    @Nullable String storageConsumerUri
) { }
