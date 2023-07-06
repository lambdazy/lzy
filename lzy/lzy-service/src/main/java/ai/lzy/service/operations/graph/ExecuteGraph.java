package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.operations.ExecutionOperationRunner;
import ai.lzy.storage.StorageClient;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Any;
import io.grpc.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class ExecuteGraph extends ExecutionOperationRunner {
    private final KafkaConfig kafkaConfig;
    private final ExecutionDao.KafkaTopicDesc kafkaTopicDesc;
    private final VmPoolServiceBlockingStub vmPoolClient;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;
    private final GraphExecutorBlockingStub graphsClient;
    private final StorageClient storageClient;
    private final ExecuteGraphState state;

    private final List<Supplier<StepResult>> steps;

    private ExecuteGraph(ExecuteGraphBuilder builder) {
        super(builder);
        this.kafkaConfig = builder.kafkaConfig;
        this.kafkaTopicDesc = builder.kafkaTopicDesc;
        this.vmPoolClient = builder.vmPoolClient;
        this.channelsClient = builder.channelsClient;
        this.graphsClient = builder.graphsClient;
        this.storageClient = builder.storageClient;
        this.state = builder.state;
        this.steps = List.of(checkCache(), findZone(), buildDataflowGraph(), createChannels(),
            buildTasks(), executeGraph(), this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private Supplier<StepResult> checkCache() {
        return new CheckCache(stepCtx(), state, storageClient, this::complete);
    }

    private Supplier<StepResult> findZone() {
        return new FindVmPoolZone(stepCtx(), state, vmPoolClient);
    }

    private Supplier<StepResult> buildDataflowGraph() {
        return new BuildDataFlowGraph(stepCtx(), state);
    }

    private Supplier<StepResult> createChannels() {
        return new CreateChannels(stepCtx(), state, channelsClient);
    }

    private Supplier<StepResult> buildTasks() {
        return new BuildTasks(stepCtx(), state, kafkaConfig, kafkaTopicDesc);
    }

    private Supplier<StepResult> executeGraph() {
        return new StartGraphExecution(stepCtx(), state, graphsClient);
    }

    private StepResult complete() {
        try {
            if (state.graphId != null) {
                var graphDesc = new GraphDao.GraphDescription(state.graphId, execId());
                var response = Any.pack(LWFS.ExecuteGraphResponse.newBuilder().setGraphId(state.graphId).build());
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        execOpsDao().deleteOp(id(), tx);
                        graphDao().put(graphDesc, tx);
                        completeOperation(null, response, tx);
                        tx.commit();
                    }
                });
            } else {
                var response = Any.pack(LWFS.ExecuteGraphResponse.getDefaultInstance());
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        execOpsDao().deleteOp(id(), tx);
                        completeOperation(null, response, tx);
                        tx.commit();
                    }
                });
            }
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful ExecuteGraph op: {}.{}", logPrefix(), e.getMessage(),
                (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    @Override
    protected boolean fail(Status status) {
        log().error("{} Fail ExecuteGraph operation: {}", logPrefix(), status.getDescription());

        boolean[] success = {false};
        var abortOp = Operation.create(userId(), "Abort execution: execId='%s'".formatted(execId()),
            serviceCfg().getOperations().getAbortWorkflowTimeout(), null, null);
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    success[0] = Objects.equals(wfDao().getExecutionId(userId(), wfName(), tx), execId());
                    if (success[0]) {
                        wfDao().setActiveExecutionId(userId(), wfName(), null, tx);
                        operationsDao().create(abortOp, tx);
                        execOpsDao().createAbortOp(abortOp.id(), serviceCfg().getInstanceId(), execId(), tx);
                        execDao().setFinishStatus(execId(), Status.INTERNAL.withDescription("error on execute graph"),
                            tx);
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
                log().debug("{} Schedule action to abort execution that has graph not executed properly: " +
                    "{ execId: {} }", logPrefix(), execId());
                var opRunner = opRunnersFactory().createAbortExecOpRunner(abortOp.id(), abortOp.description(), null,
                    userId(), wfName(), execId());
                opsExecutor().startNew(opRunner);
            } catch (Exception e) {
                log().warn("{} Cannot schedule action to abort execution that has graph not executed properly: " +
                    "{ execId: {}, error: {} }", logPrefix(), execId(), e.getMessage(), e);
            }
        }

        return true;
    }

    public static ExecuteGraphBuilder builder() {
        return new ExecuteGraphBuilder();
    }

    public static final class ExecuteGraphBuilder extends ExecutionOperationRunnerBuilder<ExecuteGraphBuilder> {
        private KafkaConfig kafkaConfig;
        private ExecutionDao.KafkaTopicDesc kafkaTopicDesc;
        private VmPoolServiceBlockingStub vmPoolClient;
        private LzyChannelManagerPrivateBlockingStub channelsClient;
        private GraphExecutorBlockingStub graphsClient;
        private StorageClient storageClient;
        private ExecuteGraphState state;

        public ExecuteGraphBuilder setKafkaConfig(KafkaConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public ExecuteGraphBuilder setKafkaTopicDesc(ExecutionDao.KafkaTopicDesc kafkaTopicDesc) {
            this.kafkaTopicDesc = kafkaTopicDesc;
            return this;
        }

        public ExecuteGraphBuilder setVmPoolClient(VmPoolServiceBlockingStub vmPoolClient) {
            this.vmPoolClient = vmPoolClient;
            return this;
        }

        public ExecuteGraphBuilder setChannelsClient(LzyChannelManagerPrivateBlockingStub channelsClient) {
            this.channelsClient = channelsClient;
            return this;
        }

        public ExecuteGraphBuilder setGraphsClient(GraphExecutorBlockingStub graphsClient) {
            this.graphsClient = graphsClient;
            return this;
        }

        public ExecuteGraphBuilder setStorageClient(StorageClient storageClient) {
            this.storageClient = storageClient;
            return this;
        }

        public ExecuteGraphBuilder setState(ExecuteGraphState state) {
            this.state = state;
            return this;
        }

        @Override
        public ExecuteGraph build() {
            return new ExecuteGraph(this);
        }
    }
}
