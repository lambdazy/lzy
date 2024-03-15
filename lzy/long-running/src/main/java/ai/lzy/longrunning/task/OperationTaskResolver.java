package ai.lzy.longrunning.task;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface OperationTaskResolver {
    Result resolve(OperationTask operationTask, @Nullable TransactionHandle tx) throws SQLException;

    enum Status {
        SUCCESS,
        STALE,
        BAD_STATE,
        UNKNOWN_TASK,
        RESOLVE_ERROR,
    }

    record Result(
        @Nullable OpTaskAwareAction action,
        Status status,
        @Nullable Exception exception
    ) {
        public static final Result STALE = new Result(null, Status.STALE, null);
        public static final Result BAD_STATE = new Result(null, Status.BAD_STATE, null);
        public static final Result UNKNOWN_TASK = new Result(null, Status.UNKNOWN_TASK, null);

        public static Result success(OpTaskAwareAction action) {
            return new Result(action, Status.SUCCESS, null);
        }

        public static Result resolveError(Exception e) {
            return new Result(null, Status.RESOLVE_ERROR, e);
        }
    }
}
