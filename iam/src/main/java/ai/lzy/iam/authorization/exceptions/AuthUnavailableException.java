package ai.lzy.iam.authorization.exceptions;

import io.grpc.Status;

public class AuthUnavailableException extends AuthException {

    public AuthUnavailableException(String details) {
        super(details);
    }

    public AuthUnavailableException(Throwable cause) {
        super(cause);
    }

    public AuthUnavailableException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.UNAVAILABLE;
    }
}
