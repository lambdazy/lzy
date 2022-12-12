package ai.lzy.channelmanager.v2.operation.state;

public record UnbindActionState(
    String executionId,
    String channelId,
    String endpointUri
) { }
