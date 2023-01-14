package ai.lzy.longrunning.dao;

import java.sql.SQLException;

public final class OperationCompletedException extends SQLException {
    public OperationCompletedException(String operationId) {
        super("Operation %s already completed".formatted(operationId));
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
