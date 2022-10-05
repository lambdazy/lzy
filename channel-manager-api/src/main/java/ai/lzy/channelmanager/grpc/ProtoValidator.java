package ai.lzy.channelmanager.grpc;

import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.common.LMS;

public class ProtoValidator {

    public static boolean isValid(LCM.ChannelSpec channelSpec) {
        try {
            boolean isValid = true;
            isValid = isValid && !channelSpec.getChannelName().isBlank();
            isValid = isValid && channelSpec.getTypeCase().getNumber() != 0;
            isValid = isValid && !channelSpec.getContentType().getDataFormat().isBlank();
            isValid = isValid && !channelSpec.getContentType().getSchemeContent().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LMS.SlotInstance slotInstance) {
        try {
            boolean isValid = true;
            isValid = isValid && !slotInstance.getTaskId().isBlank();
            isValid = isValid && !slotInstance.getSlotUri().isBlank();
            isValid = isValid && !slotInstance.getChannelId().isBlank();
            isValid = isValid && !slotInstance.getSlot().getName().isBlank();
            isValid = isValid && !slotInstance.getSlot().getContentType().getDataFormat().isBlank();
            isValid = isValid && !slotInstance.getSlot().getContentType().getSchemeContent().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

}
