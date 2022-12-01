package ai.lzy.channelmanager.v2.operation.state;

public record UnbindActionState(
    String channelId,
    String endpointUri
) { }
