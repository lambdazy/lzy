package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.Status;

import java.util.Set;

import static io.grpc.Status.Code.*;

public class LzyRetryStrategy {
    private static final Set<Status.Code> NON_RETRYABLE_CODES = Set.of(
            UNIMPLEMENTED,
            // Not an error
            OK,
            // Handled by higher-level code
            NOT_FOUND, ALREADY_EXISTS,
            // Hard error. Reported to the user.
            INVALID_ARGUMENT
    );

    public static boolean isRetryable(Status status) {
        return !NON_RETRYABLE_CODES.contains(status.getCode());
    }


}
