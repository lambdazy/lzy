package ai.lzy.channelmanager.v2.exceptions;

public class SkippingChannelGraphStateException extends IllegalChannelGraphStateException {

    public SkippingChannelGraphStateException(String channelId, String message) {
        super(channelId, message);
    }

}
