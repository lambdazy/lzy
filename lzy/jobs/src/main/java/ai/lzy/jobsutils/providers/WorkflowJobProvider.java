package ai.lzy.jobsutils.providers;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.JobSerializer.SerializationException;
import ai.lzy.longrunning.dao.OperationDao;
import com.google.rpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

public abstract class WorkflowJobProvider<T> extends JobProviderBase<WorkflowJobProvider.WorkflowJobArg> {
    private static final List<Code> RETRYABLE_CODES = List.of(
        Code.UNAVAILABLE,
        Code.UNKNOWN
    );

    protected final Logger logger;
    private final JobSerializerBase<T> serializer;
    private final OperationDao dao;
    private final @Nullable Class<? extends WorkflowJobProvider<T>> next;
    private final @Nullable Class<? extends WorkflowJobProvider<T>> prev;
    private final ApplicationContext context;

    protected WorkflowJobProvider(JobService jobService, JobSerializerBase<T> serializer, JobsOperationDao dao,
                                  @Nullable Class<? extends WorkflowJobProvider<T>> prev,
                                  @Nullable Class<? extends WorkflowJobProvider<T>> next,
                                  ApplicationContext context)
    {
        super(new WorkflowJobSerializer(), jobService, WorkflowJobArg.class);
        this.serializer = serializer;
        this.dao = dao;
        this.next = next;
        this.prev = prev;
        this.context = context;
        this.logger = LogManager.getLogger(this.getClass());
    }

    protected abstract T exec(T state, String operationId) throws JobProviderException;
    protected abstract T clear(T state, String operationId);

    @Override
    protected void executeJob(WorkflowJobArg arg) {
        try {
            final T prevState = serializer.deserializeArg(arg.serializedState);

            final T state;
            final boolean failed;

            if (!validateOp(arg)) {
                failed = true;
                state = clear(prevState, arg.operationId());
            } else if (arg.deadline != null && Instant.now().isAfter(arg.deadline)) {
                failed = true;

                failOp(arg, Status.newBuilder()
                        .setCode(Code.DEADLINE_EXCEEDED.value())
                        .setMessage("Operation deadline exceeded")
                        .build());

                state = clear(prevState, arg.operationId());

            } else {
                failed = false;

                state = exec(prevState, arg.operationId());
            }

            final String serializedState = serializer.serializeArg(state);

            final WorkflowJobArg nextJobArg = new WorkflowJobArg(
                    arg.operationId,
                    serializedState,
                    failed ? null : arg.deadline
            );

            if (failed && prev != null) {

                var provider = context.getBean(prev);
                provider.schedule(nextJobArg, null);

            } else if (!failed && next != null) {

                var provider = context.getBean(next);
                provider.schedule(nextJobArg, null);

            }
        } catch (SerializationException e) {
            logger.error("Cannot serialize or deserialize job argument in op {}: ", arg.operationId, e);
            try {
                failOp(arg, Status.newBuilder()
                    .setCode(Code.INTERNAL.value())
                    .setMessage("Error while executing operation")
                    .build()
                );
            } catch (Exception ex) {
                logger.error("Cannot fail operation, rescheduling... ");
                reschedule(arg, Duration.ofSeconds(1));
            }
        } catch (JobProviderException e) {
            logger.error("Rescheduling job for op {}", arg.operationId);
            if (e.status() != null) {
                try {
                    failOp(arg, e.status);
                } catch (Exception ex) {
                    logger.error("Cannot fail operation, rescheduling... ");
                }
            }

            reschedule(arg, e.after);
        } catch (StatusRuntimeException e) {
            logger.error("Grpc exception while executing operation {}: ", arg.operationId, e);
            if (!RETRYABLE_CODES.contains(e.getStatus().getCode())) {
                try {
                    failOp(arg, Status.newBuilder()
                        .setCode(Code.INTERNAL.value())
                        .setMessage("Internal exception")
                        .build());
                } catch (Exception ex) {
                    logger.error("Cannot fail operation, rescheduling... ");
                }
            }
            reschedule(arg, Duration.ofSeconds(1));
        } catch (Exception e) {
            logger.error("Unexpected error while executing op {}. ", arg.operationId, e);
            try {
                failOp(arg, Status.newBuilder()
                    .setCode(Code.INTERNAL.value())
                    .setMessage("Internal exception")
                    .build());
            } catch (Exception ex) {
                logger.error("Cannot fail operation, rescheduling... ");
            }
            reschedule(arg, Duration.ofSeconds(1));
        }

    }

    public record WorkflowJobArg(
        String operationId,
        String serializedState,
        @Nullable Instant deadline
    ) {}

    @Singleton
    public static class WorkflowJobSerializer extends JsonJobSerializer<WorkflowJobArg> {
        protected WorkflowJobSerializer() {
            super(WorkflowJobArg.class);
        }
    }

    private boolean validateOp(WorkflowJobArg arg) throws Exception {
        var op = withRetries(logger, () -> dao.get(arg.operationId, null));
        if (op == null) {
            logger.error("Operation {} not found", arg.operationId);
            return false;
        }
        if (op.done()) {
            if (op.response() != null) {
                logger.warn("Operation {} already successfully completed", arg.operationId);
            } else {
                logger.warn("Operation {} already completed with error: {}",
                        arg.operationId, op.error());
            }
            return false;
        }
        return true;
    }

    private void failOp(WorkflowJobArg arg, Status error) throws Exception {
        withRetries(logger, () -> dao.failOperation(arg.operationId, error, null, logger));
    }

    private void reschedule(WorkflowJobArg arg, @Nullable Duration startAfter) {
        try {
            this.schedule(arg, startAfter);
        } catch (SerializationException e) {
            logger.error("Error while resheduling job for op {}. Trying to fail op.", arg.operationId, e);
            try {
                failOp(arg, Status.newBuilder()
                    .setCode(Code.INTERNAL.value())
                    .setMessage("Internal error while executing op")
                    .build());
            } catch (Exception ex) {
                logger.error("Cannot fail op", ex);
            }
        }
    }

    protected void reschedule(@Nullable Duration after) throws JobProviderException {
        throw new JobProviderException(null, after);
    }

    public void schedule(String opId, T input, @Nullable Duration startAfter,
                         @Nullable Instant deadline) throws SerializationException
    {
        schedule(new WorkflowJobArg(
            opId,
            serializer.serializeArg(input),
            deadline
        ), startAfter);
    }

    /**
     * Reschedules job.
     * If status is not null, also fails op
     */
    public static class JobProviderException extends Exception {

        private final @Nullable Status status;
        private final @Nullable Duration after;

        JobProviderException(@Nullable Status status, @Nullable Duration after) {
            this.status = status;
            this.after = after;
        }

        @Nullable
        Status status() {
            return status;
        }

        @Nullable
        public Duration after() {
            return after;
        }
    }

    protected void fail(Status status) throws JobProviderException {
        throw new JobProviderException(status, null);
    }
}
