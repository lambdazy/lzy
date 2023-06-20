package ai.lzy.channelmanager.operation.state;

public record UnbindActionState(
    String wfName,
    String executionId,
    String channelId,
    String endpointUri
) { }
