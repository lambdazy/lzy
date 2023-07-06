package ai.lzy.channelmanager.model;

import ai.lzy.v1.common.LC;

public record Peer(
    String id,
    String channelId,
    Role role,
    LC.PeerDescription description
) {
    public enum Role {
        PRODUCER,
        CONSUMER
    }
}
