package ai.lzy.util.grpc;

import com.google.protobuf.Timestamp;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@SuppressWarnings("OverloadMethodsDeclarationOrder")
public enum ProtoConverter {
    ;

    public static Instant fromProto(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds()).plus(timestamp.getNanos(), ChronoUnit.NANOS);
    }

    public static Timestamp toProto(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }

    public static Duration fromProto(com.google.protobuf.Duration duration) {
        return Duration.ofSeconds(duration.getSeconds()).plus(duration.getNanos(), ChronoUnit.NANOS);
    }

    public static com.google.protobuf.Duration toProto(Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
            .setSeconds(duration.getSeconds())
            .setNanos(duration.getNano())
            .build();
    }

}
