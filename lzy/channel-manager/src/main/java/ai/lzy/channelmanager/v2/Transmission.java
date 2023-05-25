package ai.lzy.channelmanager.v2;

public record Transmission(
    String channelId,
    Peer loader,
    Peer target,
    State state
) {

    public enum State {
        BEFORE_START,  // If StartTransmission on slots api not called yet.
        STARTED,
        SUCCEEDED,
        FAILED
    }
}
