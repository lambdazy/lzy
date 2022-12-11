package ai.lzy.whiteboard.grpc;

import ai.lzy.v1.common.LMD;
import ai.lzy.v1.whiteboard.LWB;

public class ProtoValidator {

    public static boolean isValid(LWB.WhiteboardFieldInfo fieldInfo) {
        boolean isValid = true;
        try {
            //noinspection ConstantConditions
            isValid = isValid && !fieldInfo.getName().isBlank();
            switch (fieldInfo.getStateCase()) {
                case NONESTATE -> {
                }
                case LINKEDSTATE -> {
                    isValid = isValid && !fieldInfo.getLinkedState().getStorageUri().isBlank();
                    isValid = isValid && isValid(fieldInfo.getLinkedState().getScheme());
                }
                default -> isValid = false;
            }
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LMD.DataScheme dataScheme) {
        boolean isValid = true;
        try {
            //noinspection ConstantConditions
            isValid = isValid && !dataScheme.getDataFormat().isBlank();
            isValid = isValid && !dataScheme.getSchemeFormat().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }
}
