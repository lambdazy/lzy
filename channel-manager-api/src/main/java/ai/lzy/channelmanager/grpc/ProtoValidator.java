package ai.lzy.channelmanager.grpc;

import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.common.LMS;

public class ProtoValidator {

    public static boolean isValid(LCM.ChannelSpec channelSpec) {
        try {
            boolean isValid = true;
            //noinspection ConstantConditions
            isValid = isValid && !channelSpec.getChannelName().isBlank();
            isValid = isValid && channelSpec.getTypeCase().getNumber() != 0;
            isValid = isValid && !channelSpec.getContentType().getDataFormat().isBlank();
            isValid = isValid && !channelSpec.getContentType().getSchemeFormat().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isValid(LMS.SlotInstance slotInstance) {
        try {
            boolean isValid = true;
            //noinspection ConstantConditions
            isValid = isValid && !slotInstance.getTaskId().isBlank();
            isValid = isValid && !slotInstance.getSlotUri().isBlank();
            isValid = isValid && !slotInstance.getChannelId().isBlank();
            isValid = isValid && !slotInstance.getSlot().getName().isBlank();
            isValid = isValid && !slotInstance.getSlot().getContentType().getDataFormat().isBlank();
            isValid = isValid && !slotInstance.getSlot().getContentType().getSchemeFormat().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

}
