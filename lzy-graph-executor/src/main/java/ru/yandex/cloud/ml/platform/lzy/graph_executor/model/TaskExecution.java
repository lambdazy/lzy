package ru.yandex.cloud.ml.platform.lzy.graph_executor.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskExecution(
        String workflowId,
        String graphExecutionId,
        String id,
        TaskDescription description
) {}
