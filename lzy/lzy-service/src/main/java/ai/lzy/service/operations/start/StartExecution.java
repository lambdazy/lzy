package ai.lzy.service.operations.start;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.config.AllocatorSessionSpec;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.service.operations.ExecutionOperationRunner;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Any;
import io.grpc.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class StartExecution extends ExecutionOperationRunner {
    private final LMST.StorageConfig storageConfig;
    private final AllocatorSessionSpec allocatorSessionSpec;
    private final StartExecutionState state;

    private final List<Supplier<StepResult>> steps;

    private StartExecution(StartExecutionBuilder builder) {
        super(builder);
        this.storageConfig = builder.storageConfig;
        this.state = builder.state;
        this.allocatorSessionSpec = builder.allocatorSessionSpec;
        this.steps = List.of(createKafkaTopic(), createAllocatorSession(), StartExecution.this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private Supplier<StepResult> createKafkaTopic() {
        return new CreateKafkaTopic(stepCtx(), state, storageConfig, kafkaClient(), s3SinkClient());
    }

    private Supplier<StepResult> createAllocatorSession() {
        return new CreateAllocatorSession(stepCtx(), state, allocatorSessionSpec, allocClient());
    }

    private StepResult complete() {
        InjectedFailures.failStartExecutionCompletion();

        var response = Any.pack(LWFS.StartWorkflowResponse.newBuilder().setExecutionId(execId()).build());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    execOpsDao().deleteOp(id(), tx);
                    completeOperation(null, response, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful StartWorkflow operation: {}.{}", logPrefix(), e.getMessage(),
                (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    @Override
    protected boolean fail(Status status) {
        log().error("{} Fail StartWorkflow operation: {}", logPrefix(), status.getDescription());

        boolean[] success = {false};
        var abortOp = Operation.create(userId(), "Abort execution: execId='%s'".formatted(execId()),
            serviceCfg().getOperations().getAbortWorkflowTimeout(), null, null);
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    success[0] = Objects.equals(wfDao().getExecutionId(userId(), wfName(), tx), execId());
                    if (success[0]) {
                        wfDao().cleanActiveExecution(userId(), wfName(), tx);
                        operationsDao().create(abortOp, tx);
                        execOpsDao().createAbortOp(abortOp.id(), serviceCfg().getInstanceId(), execId(), tx);
                        execDao().setFinishStatus(execId(), Status.INTERNAL.withDescription("error on start"), tx);
                    }
                    execOpsDao().deleteOp(id(), tx);
                    failOperation(status, tx);
                    tx.commit();
                }
            });
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
            return true;
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
            return true;
        } catch (Exception ex) {
            log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
            return false;
        }

        if (success[0]) {
            try {
                log().debug("{} Schedule action to abort execution that not started properly: { execId: {} }",
                    logPrefix(), execId());
                var opRunner = opRunnersFactory().createAbortExecOpRunner(abortOp.id(), abortOp.description(), null,
                    userId(), wfName(), execId());
                opsExecutor().startNew(opRunner);
            } catch (Exception e) {
                log().warn("{} Cannot schedule action to abort execution that not started properly: { execId: {}, " +
                    "error: {} }", logPrefix(), execId(), e.getMessage(), e);
            }
        }

        return true;
    }

    public static StartExecutionBuilder builder() {
        return new StartExecutionBuilder();
    }

    public static final class StartExecutionBuilder extends ExecutionOperationRunnerBuilder<StartExecutionBuilder> {
        private LMST.StorageConfig storageConfig;
        private AllocatorSessionSpec allocatorSessionSpec;
        private StartExecutionState state;

        public StartExecutionBuilder setStorageCfg(LMST.StorageConfig storageConfig) {
            this.storageConfig = storageConfig;
            return this;
        }

        public StartExecutionBuilder setState(StartExecutionState state) {
            this.state = state;
            return this;
        }

        public StartExecutionBuilder setAllocatorSessionSpec(AllocatorSessionSpec allocatorSessionSpec) {
            this.allocatorSessionSpec = allocatorSessionSpec;
            return this;
        }

        @Override
        public StartExecution build() {
            return new StartExecution(this);
        }
    }
}

