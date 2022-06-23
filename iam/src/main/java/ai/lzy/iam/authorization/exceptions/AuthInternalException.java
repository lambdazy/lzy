package ai.lzy.iam.authorization.exceptions;

import io.grpc.Status;

public class AuthInternalException extends AuthException {

    public AuthInternalException(String details) {
        super(details);
    }

    public AuthInternalException(Throwable cause) {
        super(cause);
    }

    public AuthInternalException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.INTERNAL;
    }
}
