package ai.lzy.channelmanager.operation.state;

public record UnbindActionState(
    String executionId,
    String channelId,
    String endpointUri
) { }
