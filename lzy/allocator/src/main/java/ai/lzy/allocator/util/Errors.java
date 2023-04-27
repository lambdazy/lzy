package ai.lzy.allocator.util;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Errors {
    private final List<Error> errors = new ArrayList<>();

    public static Errors create() {
        return new Errors();
    }

    public void add(Error error) {
        errors.add(error);
    }

    public void add(String message) {
        errors.add(new Error(message));
    }

    public void add(String message, Throwable cause) {
        errors.add(new Error(message, cause));
    }

    public void add(Throwable cause) {
        errors.add(new Error(cause));
    }

    public void add(Errors other) {
        errors.addAll(other.errors);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public StatusRuntimeException toStatusRuntimeException(Status.Code statusCode) {
        return Status.fromCode(statusCode)
            .withDescription(errors.stream()
                .map(Error::getMessage)
                .map(m -> '"' + m + '"')
                .collect(Collectors.joining(", ", "Errors: ", "")))
            .asRuntimeException();
    }

    public record Error(
        String message,
        @Nullable Throwable cause
    ) {
        public Error(String message) {
            this(message, null);
        }

        public Error(Throwable cause) {
            this(cause.getMessage(), cause);
        }

        public String getMessage() {
            return cause == null ? message : message + ". details: " + cause.getMessage();
        }
    }
}
