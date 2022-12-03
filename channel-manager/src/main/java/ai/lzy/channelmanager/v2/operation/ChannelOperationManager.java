package ai.lzy.channelmanager.v2.operation;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.dao.ChannelDao;
import ai.lzy.channelmanager.v2.dao.ChannelOperationDao;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.operation.action.BindAction;
import ai.lzy.channelmanager.v2.operation.action.ChannelAction;
import ai.lzy.channelmanager.v2.operation.action.DestroyAction;
import ai.lzy.channelmanager.v2.operation.action.UnbindAction;
import ai.lzy.channelmanager.v2.operation.state.BindActionState;
import ai.lzy.channelmanager.v2.operation.state.DestroyActionState;
import ai.lzy.channelmanager.v2.operation.state.UnbindActionState;
import ai.lzy.longrunning.dao.OperationDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;

public class ChannelOperationManager {

    private final ObjectMapper objectMapper;
    private final ChannelManagerDataSource storage;
    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelController channelController;
    private final SlotConnectionManager slotConnectionManager;
    private final GrainedLock lockManager;

    public ChannelOperationManager(ObjectMapper objectMapper, ChannelManagerDataSource storage, ChannelDao channelDao,
                                   OperationDao operationDao, ChannelOperationDao channelOperationDao,
                                   ChannelController channelController,
                                   SlotConnectionManager slotConnectionManager, GrainedLock lockManager) {
        this.objectMapper = objectMapper;
        this.storage = storage;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
        this.slotConnectionManager = slotConnectionManager;
        this.lockManager = lockManager;
    }

    public ChannelOperation newBindOperation(String operationId, Instant startedAt, Instant deadline,
                                             String channelId, String endpointUri)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.BIND,
            toJson(new BindActionState(channelId, endpointUri, null, null)));
    }

    public ChannelOperation newUnbindOperation(String operationId, Instant startedAt, Instant deadline,
                                             String channelId, String endpointUri)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.UNBIND,
            toJson(new UnbindActionState(channelId, endpointUri)));
    }

    public ChannelOperation newDestroyOperation(String operationId, Instant startedAt, Instant deadline,
                                                List<String> channelsToDestroy)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.UNBIND,
            toJson(new DestroyActionState(new HashSet<>(channelsToDestroy), new HashSet<>())));
    }

    public ChannelAction getAction(ChannelOperation operation) {
        return switch (operation.type()) {
            case BIND -> new BindAction(operation.id(),
                fromJson(operation.stateJson(), BindActionState.class),
                objectMapper, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager);
            case UNBIND -> new UnbindAction(operation.id(),
                fromJson(operation.stateJson(), UnbindActionState.class),
                objectMapper, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager);
            case DESTROY -> new DestroyAction(operation.id(),
                fromJson(operation.stateJson(), DestroyActionState.class),
                objectMapper, storage, channelDao, operationDao, channelOperationDao,
                channelController, slotConnectionManager, lockManager);
        };
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
