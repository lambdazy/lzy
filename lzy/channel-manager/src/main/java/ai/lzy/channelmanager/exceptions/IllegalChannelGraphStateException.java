package ai.lzy.channelmanager.exceptions;

public class IllegalChannelGraphStateException extends ChannelGraphStateException {

    public IllegalChannelGraphStateException(String channelId, String message) {
        super(channelId, message);
    }

}
