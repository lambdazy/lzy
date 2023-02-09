package ai.lzy.test;

import ai.lzy.model.DataScheme;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortal;
import io.grpc.stub.StreamObserver;
import lombok.Lombok;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GrpcUtils {
    private static final AtomicInteger reqidCounter = new AtomicInteger(1);

    public static String generateRequestId() {
        return "reqid-" + reqidCounter.getAndIncrement();
    }

    public static io.grpc.Context newGrpcContext() {
        return GrpcHeaders.createContext(Map.of(GrpcHeaders.X_REQUEST_ID, generateRequestId()));
    }

    public static <T> T withGrpcContext(Supplier<T> fn) {
        var ctx = newGrpcContext();
        try {
            return ctx.wrap(fn::get).call();
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    public static LMS.Slot makeInputFileSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.FILE)
            .setDirection(LMS.Slot.Direction.INPUT)
            .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    public static LMS.Slot makeOutputFileSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.FILE)
            .setDirection(LMS.Slot.Direction.OUTPUT)
            .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    public static LMS.Slot makeInputPipeSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.PIPE)
            .setDirection(LMS.Slot.Direction.INPUT)
            .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    public static LMS.Slot makeOutputPipeSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.PIPE)
            .setDirection(LMS.Slot.Direction.OUTPUT)
            .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    public static LzyPortal.PortalSlotDesc.Snapshot makeAmazonSnapshot(String key, String bucket, String endpoint) {
        return LzyPortal.PortalSlotDesc.Snapshot.newBuilder()
            .setStorageConfig(LMST.StorageConfig.newBuilder()
                .setUri("s3://" + bucket + "/" + key)
                .setS3(LMST.S3Credentials.newBuilder()
                    .setAccessToken("")
                    .setSecretToken("")
                    .setEndpoint(endpoint)
                    .build()))
            .build();
    }

    public static LzyPortal.PortalSlotDesc.StdOut makeStdoutStorage(String taskId) {
        return LzyPortal.PortalSlotDesc.StdOut.newBuilder()
            .setTaskId(taskId)
            .build();
    }

    public static LzyPortal.PortalSlotDesc.StdErr makeStderrStorage(String taskId) {
        return LzyPortal.PortalSlotDesc.StdErr.newBuilder()
            .setTaskId(taskId)
            .build();
    }

    public static int rollPort() {
        return FreePortFinder.find(10000, 20000);
    }

    public abstract static class SuccessStreamObserver<T> implements StreamObserver<T> {

        @Override
        public void onError(Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.getMessage());
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage) {
            return new SuccessStreamObserver<>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                }
            };
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage, Runnable onFinish) {
            return new SuccessStreamObserver<>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                    onFinish.run();
                }
            };
        }
    }
}
