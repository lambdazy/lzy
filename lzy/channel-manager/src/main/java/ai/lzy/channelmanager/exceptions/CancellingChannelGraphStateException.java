package ai.lzy.channelmanager.exceptions;

public class CancellingChannelGraphStateException extends ChannelGraphStateException {

    public CancellingChannelGraphStateException(String channelId, String message) {
        super(channelId, message);
    }

}
