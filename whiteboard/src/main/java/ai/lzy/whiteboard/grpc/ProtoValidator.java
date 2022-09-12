package ai.lzy.whiteboard.grpc;

import ai.lzy.v1.common.LMB;
import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBPS;

public class ProtoValidator {

    public static boolean isValid(LWB.WhiteboardFieldInfo fieldInfo) {
        boolean isValid = true;
        try {
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

    public static boolean isValid(LMB.DataScheme dataScheme) {
        boolean isValid = true;
        try {
            isValid = isValid && !dataScheme.getType().isBlank();
            isValid = isValid && !dataScheme.getSchemeType().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LWBPS.CreateWhiteboardRequest request) {
        boolean isValid = true;
        try {
            isValid = isValid && !request.getUserId().isBlank();
            isValid = isValid && !request.getWhiteboardName().isBlank();
            isValid = isValid && request.getFieldsCount() != 0;
            isValid = isValid && request.getFieldsList().stream().allMatch(ProtoValidator::isValid);
            isValid = isValid && !request.getNamespace().isBlank();
            isValid = isValid && !request.getStorage().getName().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    public static boolean isValid(LWBPS.FinalizeFieldRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getWhiteboardId().isBlank();
            isValid = isValid && !request.getFieldName().isBlank();
            isValid = isValid && !request.getStorageUri().isBlank();
            isValid = isValid && isValid(request.getScheme());
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

}
