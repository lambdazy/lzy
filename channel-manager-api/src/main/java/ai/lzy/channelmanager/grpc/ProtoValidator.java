package ai.lzy.channelmanager.grpc;

import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.common.LMS;

public class ProtoValidator {

    public static boolean isValid(LCMPS.ChannelCreateRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getExecutionId().isBlank();
            isValid = isValid && isValid(request.getChannelSpec());
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LCMPS.ChannelDestroyRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getChannelId().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LCMPS.ChannelDestroyAllRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getExecutionId().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LCMPS.ChannelStatusRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getChannelId().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LCMPS.ChannelStatusAllRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getExecutionId().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }


    public static boolean isValid(ai.lzy.v1.channel.LCM.ChannelSpec channelSpec) {
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

    public static boolean isValid(LCM.ChannelSpec channelSpec) {
        try {
            boolean isValid = true;
            isValid = isValid && !channelSpec.getChannelName().isBlank();
            isValid = isValid && !channelSpec.getScheme().getDataFormat().isBlank();
            isValid = isValid && !channelSpec.getScheme().getSchemeContent().isBlank();
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
