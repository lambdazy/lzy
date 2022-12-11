package ai.lzy.channelmanager.v2.exceptions;

public class IllegalChannelGraphStateException extends ChannelGraphStateException {

    public IllegalChannelGraphStateException(String channelId, String message) {
        super(channelId, message);
    }

}
