package ai.lzy.whiteboard.grpc;

import ai.lzy.v1.common.LMD;
import ai.lzy.v1.whiteboard.LWB;

public class ProtoValidator {

    public static boolean isValid(LWB.WhiteboardField fieldInfo) {
        boolean isValid = true;
        try {
            //noinspection ConstantConditions
            isValid = isValid && !fieldInfo.getName().isBlank();
            isValid = isValid && isValid(fieldInfo.getScheme());
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
