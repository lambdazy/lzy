package ai.lzy.util.auth.exceptions;

import io.grpc.Status;

public class AuthBadRequestException extends AuthException {

    public AuthBadRequestException(String details) {
        super(details);
    }

    public AuthBadRequestException(Throwable cause) {
        super(cause);
    }

    public AuthBadRequestException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.INVALID_ARGUMENT;
    }
}
