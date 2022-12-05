package ai.lzy.channelmanager.v2.operation;

import java.time.Instant;

public record ChannelOperation(
    String id,
    Instant startedAt,
    Instant deadline,
    Type type,
    String stateJson
) {
    public enum Type {
        BIND,
        UNBIND,
        DESTROY
    }
}