package ai.lzy.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.util.grpc.GrpcHeaders;
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
