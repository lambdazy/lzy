package ai.lzy.util.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public static <T> String printAsArray(Collection<T> collection) {
        return collection.stream().map(Objects::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    public static <T> String printAsTuple(Collection<T> collection) {
        return printAsTuple(collection, Objects::toString);
    }

    public static <T> String printAsTuple(Collection<T> collection, Function<T, String> toString) {
        return collection.stream().map(toString).collect(Collectors.joining(", ", "(", ")"));
    }
}
