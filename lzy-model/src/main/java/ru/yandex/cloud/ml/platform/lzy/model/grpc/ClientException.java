package ru.yandex.cloud.ml.platform.lzy.model.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class ClientException extends RuntimeException {
    private final Status.Code code;

    public ClientException(StatusRuntimeException e) {
        this(e.getStatus(), e);
    }

    public ClientException(Status status, Throwable cause) {
        this(String.format("[%s] %s", status.getCode(), status.getDescription()), cause, status.getCode());
    }

    public ClientException(String message, Throwable cause, Status.Code code) {
        super(message, cause);
        this.code = code;
    }

    public Status.Code getCode() {
        return this.code;
    }
}
