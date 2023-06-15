package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.operations.ExecutionOperationRunner;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.Status;

import java.sql.SQLException;

import static ai.lzy.model.db.DbHelper.withRetries;

public abstract class StopExecution extends ExecutionOperationRunner {
    private final KafkaLogsListeners kafkaLogsListeners;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;
    private final Status finishStatus;
    private final StopExecutionState state;

    protected StopExecution(StopExecutionBuilder<?> builder) {
        super(builder);
        this.kafkaLogsListeners = builder.kafkaLogsListeners;
        this.allocOpClient = builder.allocOpClient;
        this.channelsClient = builder.channelsClient;
        this.finishStatus = builder.finishStatus;
        this.state = builder.state;
    }

    protected KafkaLogsListeners kafkaLogsListeners() {
        return kafkaLogsListeners;
    }

    protected LongRunningServiceBlockingStub allocOpClient() {
        return allocOpClient;
    }

    protected LzyChannelManagerPrivateBlockingStub channelsClient() {
        return channelsClient;
    }

    protected Status finishStatus() {
        return finishStatus;
    }

    protected StopExecutionState state() {
        return state;
    }

    protected abstract Message response();

    protected StepResult complete() {
        var response = Any.pack(response());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    execDao().setFinishStatus(execId(), finishStatus(), tx);
                    execOpsDao().deleteOp(id(), tx);
                    completeOperation(null, response, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful {} op: {}.{}", logPrefix(), response.getClass().getSimpleName(),
                e.getMessage(), (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    @Override
    protected boolean fail(Status status) {
        log().error("{} Fail {} operation: {}", logPrefix(), response().getClass().getSimpleName(),
            status.getDescription());

        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    execOpsDao().deleteOp(id(), tx);
                    failOperation(status, tx);
                    tx.commit();
                }
            });
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
        } catch (Exception ex) {
            log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
            return false;
        }

        return true;
    }

    protected abstract static class StopExecutionBuilder<T extends StopExecutionBuilder<T>>
        extends ExecutionOperationRunnerBuilder<T>
    {
        private KafkaLogsListeners kafkaLogsListeners;
        private LongRunningServiceBlockingStub allocOpClient;
        private LzyChannelManagerPrivateBlockingStub channelsClient;
        private Status finishStatus;
        private StopExecutionState state;

        public T setAllocOpClient(LongRunningServiceBlockingStub allocOpClient) {
            this.allocOpClient = allocOpClient;
            return self();
        }

        public T setKafkaLogsListeners(KafkaLogsListeners kafkaLogsListeners) {
            this.kafkaLogsListeners = kafkaLogsListeners;
            return self();
        }

        public T setChannelsClient(LzyChannelManagerPrivateBlockingStub channelsClient) {
            this.channelsClient = channelsClient;
            return self();
        }

        public T setFinishStatus(Status finishStatus) {
            this.finishStatus = finishStatus;
            return self();
        }

        public T setState(StopExecutionState state) {
            this.state = state;
            return self();
        }
    }
}
