package ai.lzy.util.auth.exceptions;

import io.grpc.Status;

public class AuthUniqueViolationException extends AuthException {

    public AuthUniqueViolationException(String details) {
        super(details);
    }

    public AuthUniqueViolationException(Throwable cause) {
        super(cause);
    }

    public AuthUniqueViolationException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.ALREADY_EXISTS;
    }
}
