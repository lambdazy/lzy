package ai.lzy.channelmanager.v2;

import ai.lzy.v1.common.LC;

public record Peer(
    String id,
    String channelId,
    Role role,
    LC.PeerDescription peerDescription
) {

    public enum Role {
        PRODUCER,
        CONSUMER
    }
}
