package ai.lzy.model.db.exceptions;

public class DaoException extends Exception {
    public DaoException(Throwable cause) {
        super(cause);
    }

    public DaoException(String message) {
        super(message);
    }
}
