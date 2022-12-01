package ai.lzy.channelmanager.v2.operation;

import ai.lzy.channelmanager.v2.operation.action.BindAction;
import ai.lzy.channelmanager.v2.operation.action.ChannelAction;
import ai.lzy.channelmanager.v2.operation.action.DestroyAction;
import ai.lzy.channelmanager.v2.operation.action.UnbindAction;
import ai.lzy.channelmanager.v2.operation.state.BindActionState;
import ai.lzy.channelmanager.v2.operation.state.DestroyActionState;
import ai.lzy.channelmanager.v2.operation.state.UnbindActionState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;

public class ChannelOperationManager {

    private final ObjectMapper objectMapper;

    public ChannelOperationManager(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ChannelOperation newBindOperation(String operationId, Instant startedAt, Instant deadline,
                                             String channelId, String endpointUri)
    {
        return new ChannelOperation(operationId, startedAt, deadline, ChannelOperation.Type.BIND,
            toJson(new BindActionState(channelId, endpointUri, null)));
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
            toJson(new DestroyActionState(new HashSet<>(channelsToDestroy), new HashSet<>()));
    }

    public ChannelAction getAction(ChannelOperation operation) {
        return switch (operation.type()) {
            case BIND -> new BindAction(state, channelController, channelDao, operationDao, channelOperationDao);
            case UNBIND -> new UnbindAction();
            case DESTROY -> new DestroyAction();
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
