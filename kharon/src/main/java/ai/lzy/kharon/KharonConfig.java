package ai.lzy.kharon;

import io.micronaut.context.annotation.ConfigurationProperties;
import ai.lzy.kharon.workflow.configs.WorkflowServiceConfig;

import javax.annotation.Nullable;


@ConfigurationProperties("kharon")
public record KharonConfig(
    String address,
    @Nullable
    String externalHost,
    String iamAddress,
    String serverAddress,
    String whiteboardAddress,
    String snapshotAddress,
    int servantProxyPort,
    int servantFsProxyPort,
    @Nullable
    WorkflowServiceConfig workflow
) {}
