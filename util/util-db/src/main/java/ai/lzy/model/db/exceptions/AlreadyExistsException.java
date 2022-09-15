package ai.lzy.model.db.exceptions;


import java.sql.SQLException;

public class AlreadyExistsException extends SQLException {
    public AlreadyExistsException() {}

    public AlreadyExistsException(String message) {
        super(message);
    }

    public AlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
