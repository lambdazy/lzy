package ai.lzy.service;

import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public interface ValidationTests<T extends Message> {
    ThrowingRunnable action(T request);

    default void doAssert(T request) {
        var sre = assertThrows(StatusRuntimeException.class, action(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }
}
