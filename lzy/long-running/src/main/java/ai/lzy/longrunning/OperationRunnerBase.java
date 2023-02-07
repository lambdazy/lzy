package ai.lzy.longrunning;

import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import com.google.protobuf.Any;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public abstract class OperationRunnerBase implements Runnable {
    private final String logPrefix;
    private final Logger log = LogManager.getLogger(getClass());
    private final String id;
    private final Storage storage;
    private final OperationDao operationsDao;
    private final OperationsExecutor executor;
    private Operation op;

    protected OperationRunnerBase(String id, String descr, Storage storage, OperationDao operationsDao,
                                  OperationsExecutor executor)
    {
        this.logPrefix = "[Op %s (%s)]".formatted(id, descr);
        this.id = id;
        this.storage = storage;
        this.operationsDao = operationsDao;
        this.executor = executor;
    }

    @Override
    public final void run() {
        try {
            if (!loadOperation()) {
                return;
            }

            if (expireOperation()) {
                return;
            }

            for (var step : steps()) {
                final var stepResult = step.get();
                switch (stepResult.code()) {
                    case ALREADY_DONE -> { }
                    case CONTINUE -> {
                        var update = updateOperationProgress();
                        switch (update.code()) {
                            case ALREADY_DONE, CONTINUE -> { }
                            case RESTART -> {
                                executor.schedule(this, update.delay().toMillis(), TimeUnit.MILLISECONDS);
                                return;
                            }
                            case FINISH -> {
                                notifyFinished();
                                return;
                            }
                        }
                    }
                    case RESTART -> {
                        executor.schedule(this, stepResult.delay().toMillis(), TimeUnit.MILLISECONDS);
                        return;
                    }
                    case FINISH -> {
                        notifyFinished();
                        return;
                    }
                }
            }
        } catch (Throwable e) {
            notifyFinished();
            if (e instanceof Error err && isInjectedError(err)) {
                log.error("{} Terminated by InjectedFailure exception: {}", logPrefix, e.getMessage());
            } else {
                log.error("{} Terminated by exception: {}", logPrefix, e.getMessage(), e);
                throw e;
            }
        }
    }

    private boolean loadOperation() {
        try {
            op = withRetries(log, () -> operationsDao.get(id, null));
        } catch (Exception e) {
            op = null;
            log.error("{} Cannot load operation: {}. Retry later...", logPrefix, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }

        if (op == null) {
            log.error("{} Not found", logPrefix);
            if (!handleNotFound()) {
                return false;
            }
            notifyFinished();
            return false;
        }

        if (op.done()) {
            if (op.response() != null) {
                log.warn("{} Already successfully completed", logPrefix);
            } else {
                log.warn("{} Already completed with error: {}", logPrefix, op.error());
            }

            if (!handleCompletedOutside()) {
                return false;
            }

            notifyFinished();
            return false;
        }

        return true;
    }

    private boolean handleNotFound() {
        try {
            return withRetries(log, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    onNotFound(tx);
                    tx.commit();
                    return true;
                }
            });
        } catch (Exception e) {
            log.error("{} DB error: {}. Retry later...", logPrefix, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }
    }

    private boolean handleCompletedOutside() {
        try {
            return withRetries(log, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    if (!op.done()) {
                        op = operationsDao.get(id, tx);
                    }
                    onCompletedOutside(op, tx);
                    tx.commit();
                    return true;
                }
            });
        } catch (Exception e) {
            log.error("{} DB error: {}. Retry later...", logPrefix, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }
    }

    private boolean expireOperation() {
        var deadline = op.deadline();
        if (deadline == null || deadline.isAfter(Instant.now())) {
            return false;
        }

        log.warn("{} Expired", logPrefix);
        try {
            op = withRetries(log, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var operation = operationsDao.fail(id, toProto(Status.DEADLINE_EXCEEDED), tx);
                    onExpired(tx);
                    tx.commit();
                    return operation;
                }
            });
            notifyExpired();
            notifyFinished();
        } catch (OperationCompletedException ex) {
            log.error("{} Cannot fail operation: already completed", logPrefix);
            if (!handleCompletedOutside()) {
                return true;
            }
            notifyFinished();
        } catch (NotFoundException e) {
            log.error("{} Cannot fail operation: not found", logPrefix);
            op = null;
            if (!handleNotFound()) {
                return true;
            }
            notifyFinished();
        } catch (Exception e) {
            log.error("{} Cannot fail operation: {}. Retry later...", logPrefix, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
        }
        return true;
    }

    private StepResult updateOperationProgress() {
        try {
            withRetries(log(), () -> operationsDao.update(id, null));
            log().debug("{} Progress updated", logPrefix);
            return StepResult.CONTINUE;
        } catch (OperationCompletedException e) {
            log().error("{} Cannot update operation: already completed", logPrefix);
            if (!handleCompletedOutside()) {
                return StepResult.RESTART;
            }
            return StepResult.FINISH;
        } catch (NotFoundException e) {
            if (!handleNotFound()) {
                return StepResult.RESTART;
            }
            log().error("{} Cannot update operation: not found", logPrefix);
            return StepResult.FINISH;
        } catch (Exception e) {
            log().error("{} Cannot update operation: {}", logPrefix, e.getMessage());
            return StepResult.RESTART.after(Duration.ofSeconds(1));
        }
    }

    protected boolean isInjectedError(Error e) {
        return false;
    }

    protected abstract List<Supplier<StepResult>> steps();

    protected final Logger log() {
        return log;
    }

    protected final String logPrefix() {
        return logPrefix;
    }

    public final String id() {
        return id;
    }

    protected final Operation op() {
        return Objects.requireNonNull(op);
    }

    protected void notifyExpired() {
    }

    protected void onExpired(TransactionHandle tx) throws SQLException {
    }

    protected void onNotFound(TransactionHandle tx) throws SQLException {
    }

    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
    }

    protected void notifyFinished() {
    }

    protected final void failOperation(Status status, TransactionHandle tx) throws SQLException {
        operationsDao.fail(id, toProto(status), tx);
    }

    protected final void completeOperation(@Nullable Any meta, Any response, TransactionHandle tx) throws SQLException {
        operationsDao.complete(id, meta, response, tx);
    }

    protected final OperationDao operationsDao() {
        return operationsDao;
    }

    public record StepResult(
        StepResult.Code code,
        Duration delay
    ) {
        public enum Code {
            ALREADY_DONE,
            CONTINUE,
            RESTART,
            FINISH
        }

        public static final StepResult ALREADY_DONE = new StepResult(Code.ALREADY_DONE, null);
        public static final StepResult CONTINUE = new StepResult(StepResult.Code.CONTINUE, null);
        public static final StepResult RESTART = new StepResult(StepResult.Code.RESTART, Duration.ofSeconds(1));
        public static final StepResult FINISH = new StepResult(StepResult.Code.FINISH, null);

        public StepResult after(Duration delay) {
            assert code == StepResult.Code.RESTART;
            return new StepResult(code, delay);
        }

        @Override
        public Duration delay() {
            assert code == StepResult.Code.RESTART;
            return delay;
        }
    }
}
