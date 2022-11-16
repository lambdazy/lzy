package ai.lzy.channelmanager.v2.exceptions;

public class ChannelInternalException extends RuntimeException {

    private final String channelId;

    public ChannelInternalException(String channelId, String message) {
        super("channelId=" + channelId + ", " + message);
        this.channelId = channelId;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public String getChannelId() {
        return channelId;
    }

}
