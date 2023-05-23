package ai.lzy.longrunning;

import ai.lzy.longrunning.dao.OperationDao;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.model.db.DbHelper.withRetries;

public enum OperationUtils {
    ;

    @Nullable
    public static Operation awaitOperationDone(OperationDao operationDao, String opId, Duration loadAttemptDelay,
                                               Duration timeout, Logger log)
    {
        long deadline = System.nanoTime() + timeout.toNanos();

        Operation op = null;
        try {
            op = withRetries(log, () -> operationDao.get(opId, null));
        } catch (Exception e) {
            log.error("Error while loading operation by id {}: {}", opId, e.getMessage(), e);
            return op;
        }

        while (!op.done() && deadline - System.nanoTime() > 0L) {
            LockSupport.parkNanos(loadAttemptDelay.toNanos());
            try {
                op = withRetries(log, () -> operationDao.get(opId, null));
            } catch (Exception e) {
                log.error("Error while loading operation by id {}: {}", opId, e.getMessage(), e);
                return op;
            }
        }

        return op;
    }
}
