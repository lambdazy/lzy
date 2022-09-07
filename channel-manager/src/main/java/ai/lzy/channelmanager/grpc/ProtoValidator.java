package ai.lzy.channelmanager.grpc;

import ai.lzy.v1.Channels;
import ai.lzy.v1.LzyFsApi;

public class ProtoValidator {

    public static boolean isValid(Channels.ChannelSpec channelSpec) {
        try {
            boolean isValid = true;
            isValid = isValid && !channelSpec.getChannelName().isBlank();
            isValid = isValid && channelSpec.getTypeCase().getNumber() != 0;
            isValid = isValid && !channelSpec.getContentType().getType().isBlank();
            isValid = isValid && channelSpec.getContentType().getSchemeType().getNumber() != 0;
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LzyFsApi.SlotInstance slotInstance) {
        try {
            boolean isValid = true;
            isValid = isValid && !slotInstance.getTaskId().isBlank();
            isValid = isValid && !slotInstance.getSlotUri().isBlank();
            isValid = isValid && !slotInstance.getChannelId().isBlank();
            isValid = isValid && !slotInstance.getSlot().getName().isBlank();
            isValid = isValid && !slotInstance.getSlot().getContentType().getType().isBlank();
            isValid = isValid && slotInstance.getSlot().getContentType().getSchemeType().getNumber() != 0;
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

}
