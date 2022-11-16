package ai.lzy.channelmanager.v2.exceptions;

public abstract class ChannelGraphStateException extends Exception {

    private final String channelId;

    public ChannelGraphStateException(String channelId, String message) {
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
