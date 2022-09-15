package ai.lzy.model.db.exceptions;

import java.sql.SQLException;

public class NotFoundException extends SQLException {

    public NotFoundException(String e) {
        super(e);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
