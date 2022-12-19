package ai.lzy.channelmanager.dao;

import ai.lzy.channelmanager.model.Channel;
import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.slot.SlotInstance;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class InMemChannelDao implements ChannelDao {

    private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> channelsByEndpoints = new ConcurrentHashMap<>();

    @Override
    public void insertChannel(String channelId, String executionId, String workflowName, String userId,
                              Channel.Spec channelSpec, @Nullable TransactionHandle transaction) throws SQLException
    {
        if (channels.get(channelId) != null) {
            throw new AlreadyExistsException("Channel " + channelId + " already exists");
        }

        channels.put(channelId, new Channel(channelId, channelSpec, executionId, workflowName, userId));
    }

    @Override
    public void markChannelDestroying(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        Channel channel = channels.get(channelId);
        channels.computeIfPresent(channelId, (id, ch) -> new Channel(
            ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
            ch.getEndpoints(), ch.getConnections(), Channel.LifeStatus.DESTROYING));
    }

    @Override
    public void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        channels.remove(channelId);
    }

    @Override
    public void insertBindingEndpoint(SlotInstance slot, Endpoint.SlotOwner owner,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        Endpoint endpoint = new Endpoint(slot, owner, Endpoint.LifeStatus.BINDING);
        channels.computeIfPresent(endpoint.getChannelId(), (id, ch) -> {
            final List<Endpoint> endpoints = ch.getEndpoints();
            endpoints.add(endpoint);
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                endpoints, ch.getConnections(), ch.getLifeStatus());
        });
        channelsByEndpoints.put(endpoint.getUri().toString(), endpoint.getChannelId());
    }

    @Override
    public void markEndpointBound(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        String channelId = channelsByEndpoints.get(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.getEndpoints().stream().map(e -> {
                if (endpointUri.equals(e.getUri().toString())) {
                    return new Endpoint(e.getSlot(), e.getSlotOwner(), Endpoint.LifeStatus.BOUND);
                }
                return e;
            }).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                endpoints, ch.getConnections(), ch.getLifeStatus());
        });
    }

    @Override
    public void markEndpointUnbinding(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        String channelId = channelsByEndpoints.get(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.getEndpoints().stream().map(e -> {
                if (endpointUri.equals(e.getUri().toString())) {
                    return new Endpoint(e.getSlot(), e.getSlotOwner(), Endpoint.LifeStatus.UNBINDING);
                }
                return e;
            }).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                endpoints, ch.getConnections(), ch.getLifeStatus());
        });
    }

    @Override
    public void markAllEndpointsUnbinding(String channelId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.getEndpoints().stream().map(e ->
                new Endpoint(e.getSlot(), e.getSlotOwner(), Endpoint.LifeStatus.UNBINDING)).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                endpoints, ch.getConnections(), ch.getLifeStatus());
        });
    }

    @Override
    public void removeEndpoint(String endpointUri, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        String channelId = channelsByEndpoints.remove(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.getEndpoints().stream()
                .filter(e -> !e.getUri().toString().equals(endpointUri))
                .toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                endpoints, ch.getConnections(), ch.getLifeStatus());
        });
    }

    @Override
    public void insertConnection(String channelId, Connection connection,
                                 @Nullable TransactionHandle transaction) throws SQLException
    {
        if (connection.status() != Connection.LifeStatus.CONNECTING) {
            throw new IllegalArgumentException("Expected " + Connection.LifeStatus.CONNECTING + " connection, "
                                               + "actual status " + connection.status());
        }
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = new ArrayList<>(ch.getConnections());
            connections.add(connection);
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                ch.getEndpoints(), connections, ch.getLifeStatus());
        });
    }

    @Override
    public void markConnectionAlive(String channelId, String senderUri, String receiverUri,
                                    @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.getConnections().stream().map(c -> {
                if (senderUri.equals(c.sender().getUri().toString())
                    && receiverUri.equals(c.receiver().getUri().toString()))
                {
                    return new Connection(c.sender(), c.receiver(), Connection.LifeStatus.CONNECTED);
                }
                return c;
            }).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                ch.getEndpoints(), connections, ch.getLifeStatus());
        });
    }

    @Override
    public void markConnectionDisconnecting(String channelId, String senderUri, String receiverUri,
                                            @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.getConnections().stream().map(c -> {
                if (senderUri.equals(c.sender().getUri().toString())
                    && receiverUri.equals(c.receiver().getUri().toString()))
                {
                    return new Connection(c.sender(), c.receiver(), Connection.LifeStatus.DISCONNECTING);
                }
                return c;
            }).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                ch.getEndpoints(), connections, ch.getLifeStatus());
        });
    }

    @Override
    public void removeConnection(String channelId, String senderUri, String receiverUri,
                                 @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.getConnections().stream().filter(c ->
                !(senderUri.equals(c.sender().getUri().toString())
                  && receiverUri.equals(c.receiver().getUri().toString()))
            ).toList();
            return new Channel(ch.getId(), ch.getSpec(), ch.getExecutionId(), ch.getWorkflowName(), ch.getUserId(),
                ch.getEndpoints(), connections, ch.getLifeStatus());
        });
    }

    @Nullable
    @Override
    public Channel findChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        return channels.get(channelId);
    }

    @Nullable
    @Override
    public Channel findChannel(String channelId, Channel.LifeStatus lifeStatus, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final Channel channel = channels.get(channelId);
        if (channel == null) {
            return null;
        }
        if (channel.getLifeStatus().equals(lifeStatus)) {
            return channel;
        }
        return null;
    }

    @Override
    public List<Channel> listChannels(String executionId, @Nullable TransactionHandle transaction) throws SQLException
    {
        return channels.values().stream()
            .filter(ch -> ch.getExecutionId().equals(executionId))
            .collect(Collectors.toList());
    }

    @Override
    public List<Channel> listChannels(String executionId, Channel.LifeStatus lifeStatus,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        return channels.values().stream()
            .filter(ch -> ch.getExecutionId().equals(executionId))
            .filter(ch -> ch.getLifeStatus().equals(lifeStatus))
            .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public Endpoint findEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        if (!channelsByEndpoints.containsKey(endpointUri)) {
            return null;
        }
        String channelId = channelsByEndpoints.get(endpointUri);
        return channels.get(channelId).getEndpoints().stream()
            .filter(e -> e.getUri().toString().equals(endpointUri))
            .findFirst().orElse(null);
    }

}
