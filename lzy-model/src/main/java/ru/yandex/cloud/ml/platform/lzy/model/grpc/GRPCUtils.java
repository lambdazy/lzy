package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

public class GRPCUtils {
    private static final int MAX_RETRIES = 5;

    public static <T> T callWithRetry(Supplier<T> action) {
        try {
            int retriesCount = 0;
            while (true) {
                try {
                    return safeGet(action);
                } catch (RetryableClientException e) {
                    if (retriesCount++ >= MAX_RETRIES) {
                        throw e;
                    }
                    if (isFatal(e)) {
                        throw e;
                    }
                    Thread.sleep(e.getRetryDuration(retriesCount).toMillis());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isFatal(RetryableClientException e) {
        return false;
    }

    public static <T> T safeGet(Supplier<T> supplier) throws ClientException {
        try {
            return supplier.get();
        } catch (StatusRuntimeException e) {
            throw forStatusException(e);
        }
    }

    private static ClientException forStatusException(StatusRuntimeException e) {
        if (isInterrupted(e)) {
            Thread.currentThread().interrupt();
            return new ClientException(e);
        } else {
            Status status = e.getStatus();
            return LzyRetryStrategy.isRetryable(status) ?
                    new RetryableClientException(e) :
                    new ClientException(e);
        }
    }

    private static boolean isInterrupted(StatusRuntimeException e) {
        return e.getStatus().getCode() == Status.Code.CANCELLED &&
                (Thread.currentThread().isInterrupted() || isInterruptException(e));
    }

    private static boolean isInterruptException(Throwable t) {
        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable next;
        for (Throwable current = t; current != null; current = next) {
            if (current instanceof InterruptedException ||
                    current instanceof InterruptedIOException) {
                return true;
            }

            dejaVu.add(current);
            next = current.getCause();
            if (dejaVu.contains(next)) {
                return false;
            }
        }

        return false;
    }
}
