package ai.lzy.util.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

public class JsonUtils {
    public static String printRequest(MessageOrBuilder request) {
        try {
            return JsonFormat.printer().print(request);
        } catch (InvalidProtocolBufferException e) {
            return "Unable to parse request; cause " + e;
        }
    }

    public static String printSingleLine(MessageOrBuilder request) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(request);
        } catch (InvalidProtocolBufferException e) {
            return "Unable to parse request; cause " + e;
        }
    }
}
