package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.ActionsManager;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.graph.DataFlowGraph;
import ai.lzy.service.workflow.finish.AbortExecutionAction;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class ExecuteGraphAction extends OperationRunnerBase {
    private final String userId;
    private final String wfName;
    private final String execId;

    private final WorkflowDao wfDao;
    private final ExecutionDao execDao;
    private final GraphDao graphDao;

    private final String idempotencyKey;
    private String poolsZone;
    private final List<String> operationsPoolSpec;
    private final VmPoolServiceBlockingStub vmPoolClient;

    private String graphId;
    private DataFlowGraph dataFlowGraph;
    private final KafkaConfig kafkaConfig;
    private final RenewableJwt internalUserCredentials;
    private final ActionsManager actionsManager;
    private final List<LWF.Operation> operations;
    private final List<String> portalInputSlotNames;
    private final Map<String, LWF.DataDescription> slot2description;
    private final GraphExecutorApi.GraphExecuteRequest.Builder builder;

    private final StorageClientFactory storageClients;
    private final GraphExecutorBlockingStub graphsClient;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;

    private final Function<StatusRuntimeException, StepResult> failAction;

    public ExecuteGraphAction(String id, String descr, String userId, String wfName, String execId, Storage storage,
                              OperationDao operationsDao, WorkflowDao wfDao, ExecutionDao execDao, GraphDao graphDao,
                              OperationsExecutor executor, String poolsZone, List<String> operationsPoolSpec,
                              Map<String, LWF.DataDescription> slot2description, List<LWF.Operation> operations,
                              KafkaConfig kafkaConfig, RenewableJwt internalUserCredentials,
                              @Nullable String idempotencyKey, StorageClientFactory storageClients,
                              VmPoolServiceBlockingStub vmPoolClient,
                              LzyChannelManagerPrivateBlockingStub channelsClient,
                              GraphExecutorBlockingStub graphsClient, ActionsManager actionsManager)
    {
        super(id, descr, storage, operationsDao, executor);
        this.userId = userId;
        this.wfName = wfName;
        this.execId = execId;
        this.wfDao = wfDao;
        this.execDao = execDao;
        this.graphDao = graphDao;
        this.poolsZone = poolsZone;
        this.operationsPoolSpec = operationsPoolSpec;
        this.slot2description = slot2description;
        this.operations = operations;
        this.kafkaConfig = kafkaConfig;
        this.internalUserCredentials = internalUserCredentials;
        this.actionsManager = actionsManager;
        this.portalInputSlotNames = new ArrayList<>();
        this.builder = GraphExecutorApi.GraphExecuteRequest.newBuilder();
        this.idempotencyKey = idempotencyKey;
        this.vmPoolClient = vmPoolClient;
        this.storageClients = storageClients;
        this.graphsClient = graphsClient;
        this.channelsClient = channelsClient;
        this.failAction = sre -> fail(sre.getStatus()) ? StepResult.FINISH : StepResult.RESTART;
    }

    private void setPoolsZone(String zone) {
        this.poolsZone = zone;
    }

    private void setDataFlowGraph(DataFlowGraph dfgraph) {
        this.dataFlowGraph = dfgraph;
    }

    private void setGraphId(String id) {
        this.graphId = id;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(checkCache(), findZone(), buildDataflowGraph(), buildGraph(), executeGraph());
    }

    private Supplier<StepResult> checkCache() {
        return new CheckCache(execDao, wfName, execId, storageClients, operations, this::complete, failAction, log(),
            logPrefix());
    }

    private Supplier<StepResult> findZone() {
        return new FindVmPoolZone(wfName, execId, poolsZone, operationsPoolSpec, vmPoolClient, this::setPoolsZone,
            failAction, log(), logPrefix());
    }

    private Supplier<StepResult> buildDataflowGraph() {
        return new BuildDataFlowGraph(wfName, execId, operations, this::setDataFlowGraph, failAction, log(),
            logPrefix());
    }

    private Supplier<StepResult> buildGraph() {
        return new BuildGraph(execDao, userId, wfName, execId, poolsZone, internalUserCredentials, kafkaConfig,
            dataFlowGraph, operations, slot2description, portalInputSlotNames, builder, idempotencyKey, channelsClient,
            failAction, log(), logPrefix());
    }

    private Supplier<StepResult> executeGraph() {
        return new ExecuteGraph(graphsClient, builder, this::setGraphId, failAction, log(), logPrefix());
    }

    private StepResult complete() {
        var pack = Any.pack((graphId != null) ? LWFS.ExecuteGraphResponse.newBuilder().setGraphId(graphId).build()
            : LWFS.ExecuteGraphResponse.getDefaultInstance());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    graphDao.save(new GraphDao.GraphDescription(graphId, execId, portalInputSlotNames), tx);
                    completeOperation(null, pack, tx);
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful ExecuteGraph op: {}.{}", logPrefix(), e.getMessage(),
                (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    private boolean fail(Status status) {
        log().error("{} Fail ExecuteGraph operation: {}", logPrefix(), status.getDescription());

        boolean[] success = {false};
        var stopOp = Operation.create(userId, "Stop execution: execId='%s'".formatted(execId),
            AbortExecutionAction.timeout, null, null);
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    success[0] = wfDao.deactivate(userId, wfName, execId, tx);
                    if (success[0]) {
                        operationsDao().create(stopOp, tx);
                    }
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
                    "{ execId: {} }", logPrefix(), execId);
                actionsManager.abortExecutionAction(stopOp.id(), stopOp.description(), null, execId,
                    Status.INTERNAL.withDescription("error on execute graph"));
            } catch (Exception e) {
                log().warn("{} Cannot schedule action to abort execution that has graph not executed properly: " +
                    "{ execId: {}, error: {} }", logPrefix(), execId, e.getMessage(), e);
            }
        }

        return true;
    }
}
