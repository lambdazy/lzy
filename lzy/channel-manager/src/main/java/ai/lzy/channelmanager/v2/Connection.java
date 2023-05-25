package ai.lzy.channelmanager.v2;

public record Connection(
    String channelId,
    Peer producer,
    Peer consumer,
    State state
) {

    public enum State {
        CONNECTING,
        CONNECTED
    }
}
