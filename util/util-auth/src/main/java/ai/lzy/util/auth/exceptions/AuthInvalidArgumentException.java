package ai.lzy.util.auth.exceptions;

import io.grpc.Status;

public class AuthInvalidArgumentException extends AuthException {
    public AuthInvalidArgumentException(String details) {
        super(details);
    }

    public AuthInvalidArgumentException(Throwable cause) {
        super(cause);
    }

    public AuthInvalidArgumentException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.INVALID_ARGUMENT;
    }
}
