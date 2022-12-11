package ai.lzy.channelmanager.v2.exceptions;

public class CancellingChannelGraphStateException extends ChannelGraphStateException {

    public CancellingChannelGraphStateException(String channelId, String message) {
        super(channelId, message);
    }

}
