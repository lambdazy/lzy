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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public abstract class OperationRunnerBase implements Runnable {
    private final Logger log = LogManager.getLogger(getClass());
    private final String id;
    private final String descr;
    private final Storage storage;
    private final OperationDao operationsDao;
    private final ScheduledExecutorService executor;
    private Operation op = null;

    protected OperationRunnerBase(String id, String descr, Storage storage, OperationDao operationsDao,
                                  ScheduledExecutorService executor)
    {
        this.id = id;
        this.descr = descr;
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
        } catch (Error e) {
            notifyFinished();
            if (isInjectedError(e)) {
                log.error("Terminate action by InjectedFailure exception: {}", e.getMessage());
            } else {
                throw e;
            }
        }
    }

    private boolean loadOperation() {
        try {
            op = withRetries(log, () -> operationsDao.get(id, null));
        } catch (Exception e) {
            op = null;
            log.error("Cannot load operation {} ({}): {}. Retry later...", id, descr, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }

        if (op == null) {
            log.error("Operation {} ({}) not found", id, descr);
            notifyFinished();
            return false;
        }

        if (op.done()) {
            if (op.response() != null) {
                log.warn("Operation {} ({}) already successfully completed", id, descr);
            } else {
                log.warn("Operation {} ({}) already completed with error: {}", id, descr, op.error());
            }

            notifyFinished();
            return false;
        }

        return true;
    }

    private boolean expireOperation() {
        var deadline = op.deadline();
        if (deadline == null || deadline.isAfter(Instant.now())) {
            return false;
        }

        log.warn("Allocation operation {} ({}) is expired", id, descr);
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
        } catch (OperationCompletedException ex) {
            log.error("Cannot fail operation {} ({}): already completed", id, descr);
            notifyFinished();
        } catch (NotFoundException e) {
            log.error("Cannot fail operation {} ({}): not found", id, descr);
            op = null;
            notifyFinished();
        } catch (Exception e) {
            log.error("Cannot fail operation {} ({}): {}. Retry later...", id, descr, e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
        }
        return true;
    }

    private StepResult updateOperationProgress() {
        try {
            withRetries(log(), () -> operationsDao.update(id, null));
            return StepResult.CONTINUE;
        } catch (OperationCompletedException e) {
            log().error("Cannot update operation {} (VM {}): already completed", id, descr);
            return StepResult.FINISH;
        } catch (NotFoundException e) {
            log().error("Cannot update operation {} (VM {}): not found", id, descr);
            return StepResult.FINISH;
        } catch (Exception e) {
            log().error("Cannot update operation {} (VM {}): {}", id, descr, e.getMessage());
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

    protected final String descr() {
        return descr;
    }

    protected void notifyExpired() {
    }

    protected void onExpired(TransactionHandle tx) throws SQLException {
    }

    protected void notifyFinished() {
    }

    protected final void failOperation(Status status, TransactionHandle tx) throws SQLException {
        operationsDao.fail(id, toProto(status), tx);
    }

    protected final void completeOperation(@Nullable Any meta, Any response, TransactionHandle tx) throws SQLException {
        operationsDao.complete(id, meta, response, tx);
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
