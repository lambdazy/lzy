package ai.lzy.model.db.exceptions;

import java.sql.SQLException;

public class ConcurrentModificationException extends SQLException {
    private final Object existing;

    public ConcurrentModificationException(String reason, Object existing) {
        super(reason);
        this.existing = existing;
    }

    public Object existing() {
        return existing;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
