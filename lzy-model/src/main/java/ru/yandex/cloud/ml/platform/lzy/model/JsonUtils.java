package ru.yandex.cloud.ml.platform.lzy.model;

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
}
