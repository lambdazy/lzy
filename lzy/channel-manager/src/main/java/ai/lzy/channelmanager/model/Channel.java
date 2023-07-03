package ai.lzy.channelmanager.model;

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
