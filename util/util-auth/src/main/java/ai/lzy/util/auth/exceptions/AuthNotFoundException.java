package ai.lzy.util.auth.exceptions;

import io.grpc.Status;

public class AuthNotFoundException extends AuthException {

    public AuthNotFoundException(String details) {
        super(details);
    }

    public AuthNotFoundException(Throwable cause) {
        super(cause);
    }

    public AuthNotFoundException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.NOT_FOUND;
    }
}
