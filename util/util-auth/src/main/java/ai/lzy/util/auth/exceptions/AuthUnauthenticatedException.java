package ai.lzy.util.auth.exceptions;

import io.grpc.Status;

public class AuthUnauthenticatedException extends AuthException {

    public AuthUnauthenticatedException(String details) {
        super(details);
    }

    public AuthUnauthenticatedException(Throwable cause) {
        super(cause);
    }

    public AuthUnauthenticatedException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.UNAUTHENTICATED;
    }
}
