package ai.lzy.model.db.exceptions;

import java.sql.SQLException;

public class InvalidStateException extends SQLException {
    public InvalidStateException() {}

    public InvalidStateException(String message) {
        super(message);
    }

    public InvalidStateException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
