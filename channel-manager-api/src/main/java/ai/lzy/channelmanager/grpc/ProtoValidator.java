package ai.lzy.channelmanager.grpc;

import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;

public class ProtoValidator {

    public static ValidationVerdict validate(LCMPS.ChannelCreateRequest request) {
        try {
            if (request.getExecutionId().isBlank()) {
                return ValidationVerdict.fail("executionId is blank");
            }
            return validate(request.getChannelSpec());
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMPS.ChannelDestroyRequest request) {
        try {
            if (request.getChannelId().isBlank()) {
                return ValidationVerdict.fail("channelId is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMPS.ChannelDestroyAllRequest request) {
        try {
            if (request.getExecutionId().isBlank()) {
                return ValidationVerdict.fail("executionId is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMPS.ChannelStatusRequest request) {
        try {
            if (request.getChannelId().isBlank()) {
                return ValidationVerdict.fail("channelId is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMPS.ChannelStatusAllRequest request) {
        try {
            if (request.getExecutionId().isBlank()) {
                return ValidationVerdict.fail("executionId is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMS.BindRequest request) {
        try {
            if (request.getSlotOwner().getNumber() <= 0) {
                return ValidationVerdict.fail("slotOwner is " + request.getSlotOwner().name());
            }
            return validate(request.getSlotInstance());
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCMS.UnbindRequest request) {
        try {
            if (request.getSlotUri().isBlank()) {
                return ValidationVerdict.fail("executionId is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LCM.ChannelSpec channelSpec) {
        try {
            if (channelSpec.getChannelName().isBlank()) {
                return ValidationVerdict.fail("channelSpec.chanelName is blank");
            }
            return validate(channelSpec.getScheme());
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LMS.SlotInstance slotInstance) {
        try {
            if (slotInstance.getChannelId().isBlank()) {
                return ValidationVerdict.fail("slotInstance.channelId is blank");
            }
            if (slotInstance.getTaskId().isBlank()) {
                return ValidationVerdict.fail("slotInstance.taskId is blank");
            }
            if (slotInstance.getSlotUri().isBlank()) {
                return ValidationVerdict.fail("slotInstance.slotUri is blank");
            }
            if (slotInstance.getSlot().getName().isBlank()) {
                return ValidationVerdict.fail("slotInstance.slotName is blank");
            }
            if (slotInstance.getSlot().getDirection().getNumber() <= 0) {
                String direction = slotInstance.getSlot().getDirection().name();
                return ValidationVerdict.fail("slotInstance.slotDirection is " + direction);
            }
            return validate(slotInstance.getSlot().getContentType());
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public static ValidationVerdict validate(LMD.DataScheme dataScheme) {
        try {
            if (dataScheme.getDataFormat().isBlank()) {
                return ValidationVerdict.fail("dataScheme.dataFormat is blank");
            }
            if (dataScheme.getSchemeFormat().isBlank()) {
                return ValidationVerdict.fail("dataScheme.schemeFormat is blank");
            }
            return ValidationVerdict.ok();
        } catch (NullPointerException e) {
            return ValidationVerdict.fail(e.getMessage());
        }
    }

    public record ValidationVerdict(boolean isOk, String description) {

        public static ValidationVerdict ok() {
            return new ValidationVerdict(true, "ok");
        }

        public static ValidationVerdict fail(String errorMessage) {
            return new ValidationVerdict(false, errorMessage);
        }

    }

}
