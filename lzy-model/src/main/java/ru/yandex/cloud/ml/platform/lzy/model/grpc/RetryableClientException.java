package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.StatusRuntimeException;

import java.time.Duration;

public class RetryableClientException extends ClientException {

    private final long max = 60000; // 1 min to millis
    private final long initial = 5000; // 5 sec to millis
    private final long step = 10000; // 10 sec to millis

    public RetryableClientException(StatusRuntimeException e) {
        super(e);
    }

    public Duration getRetryDuration(int retriesCount) {
        if (retriesCount == 0) {
            return Duration.ofMillis(initial);
        } else {
            return Duration.ofMillis(Math.min(max, initial + step * retriesCount));
        }
    }
}
