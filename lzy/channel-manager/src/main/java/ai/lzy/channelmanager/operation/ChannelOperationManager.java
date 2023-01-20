package ai.lzy.channelmanager.operation;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.operation.action.BindAction;
import ai.lzy.channelmanager.operation.action.ChannelAction;
import ai.lzy.channelmanager.operation.action.DestroyAction;
import ai.lzy.channelmanager.operation.action.UnbindAction;
import ai.lzy.channelmanager.operation.state.BindActionState;
import ai.lzy.channelmanager.operation.state.DestroyActionState;
import ai.lzy.channelmanager.operation.state.UnbindActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.ProtoObjectMapper;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;

import static ai.lzy.channelmanager.ChannelManagerApp.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

@Singleton
public class ChannelOperationManager {

    private static final Logger LOG = LogManager.getLogger(ChannelOperationManager.class);

    private final ObjectMapper objectMapper;
    private final ChannelOperationExecutor executor;
    private final ChannelManagerDataSource storage;
    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelController channelController;
    private final SlotConnectionManager slotConnectionManager;
    private final GrainedLock lockManager;
    private final LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub workflowPrivateApi;

    public ChannelOperationManager(@Named("ChannelManagerWorkflowGrpcChannel") ManagedChannel workflowGrpcChannel,
                                   @Named("ChannelManagerIamToken") RenewableJwt internalUserCredentials,
                                   ChannelOperationExecutor executor,
                                   ChannelManagerDataSource storage, ChannelDao channelDao,
                                   @Named("ChannelManagerOperationDao") OperationDao operationDao,
                                   ChannelOperationDao channelOperationDao, ChannelController channelController,
                                   SlotConnectionManager slotConnectionManager, GrainedLock lockManager)
    {
        this.objectMapper = new ProtoObjectMapper();
        this.executor = executor;
        this.storage = storage;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
        this.slotConnectionManager = slotConnectionManager;
        this.lockManager = lockManager;

        this.workflowPrivateApi = newBlockingClient(
            LzyWorkflowPrivateServiceGrpc.newBlockingStub(workflowGrpcChannel),
            APP, () -> internalUserCredentials.get().token());
    }

    public ChannelOperation newBindOperation(String operationId, Instant startedAt, Instant deadline,
                                             String executionId, String channelId, String endpointUri)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.BIND,
            toJson(new BindActionState(executionId, channelId, endpointUri, null, null)));
    }

    public ChannelOperation newUnbindOperation(String operationId, Instant startedAt, Instant deadline,
                                               String executionId, String channelId, String endpointUri)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.UNBIND,
            toJson(new UnbindActionState(executionId, channelId, endpointUri)));
    }

    public ChannelOperation newDestroyOperation(String operationId, Instant startedAt, Instant deadline,
                                                String executionId, List<String> channelsToDestroy)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.DESTROY,
            toJson(new DestroyActionState(executionId, new HashSet<>(channelsToDestroy), new HashSet<>())));
    }

    public ChannelAction getAction(ChannelOperation operation) {
        return switch (operation.type()) {
            case BIND -> new BindAction(operation.id(), operation.deadline(),
                fromJson(operation.stateJson(), BindActionState.class),
                objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager, workflowPrivateApi);
            case UNBIND -> new UnbindAction(operation.id(), operation.deadline(),
                fromJson(operation.stateJson(), UnbindActionState.class),
                objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager, workflowPrivateApi);
            case DESTROY -> new DestroyAction(operation.id(), operation.deadline(),
                fromJson(operation.stateJson(), DestroyActionState.class),
                objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager, workflowPrivateApi);
        };
    }

    public void restoreActiveOperations() {
        final List<ChannelOperation> operations;
        LOG.info("");
        LOG.info("Restore active operations");
        try {
            operations = channelOperationDao.getActiveOperations(null);
        } catch (Exception e) {
            LOG.error("Restore active operations failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        int restored = 0;
        for (final ChannelOperation operation : operations) {
            executor.submit(getAction(operation));
            LOG.info("Restore active operations: operation {} scheduled", operation.id());
            restored++;
        }

        LOG.info("Restore active operations done, {} restored", restored);
    }

    @VisibleForTesting
    public void interruptActions() {
        executor.shutdownNow();
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T fromJson(String obj, Class<T> type) {
        try {
            return objectMapper.readValue(obj, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
