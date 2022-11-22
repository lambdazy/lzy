package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class InMemChannelStorage implements ChannelStorage {

    private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> channelsByEndpoints = new ConcurrentHashMap<>();


    @Override
    public void insertChannel(String channelId, String executionId, ChannelSpec channelSpec,
                              @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.put(channelId, new Channel(channelId, channelSpec, executionId));
    }

    @Override
    public void markChannelDestroying(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        Channel channel = channels.get(channelId);
        channels.computeIfPresent(channelId, (id, ch) -> new Channel(ch.id(), ch.spec(), ch.executionId(),
            ch.endpoints(), ch.connections(), Channel.LifeStatus.DESTROYING));
    }

    @Override
    public void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        channels.remove(channelId);
    }

    @Override
    public void insertBindingEndpoint(Endpoint endpoint, @Nullable TransactionHandle transaction) throws SQLException {
        if (endpoint.status() != Endpoint.LifeStatus.BINDING) {
            throw new IllegalArgumentException("Expected " + Endpoint.LifeStatus.BINDING + " endpoint, actual status " + endpoint.status());
        }
        channels.computeIfPresent(endpoint.channelId(), (id, ch) -> {
            final List<Endpoint> endpoints = ch.endpoints();
            endpoints.add(endpoint);
            return new Channel(ch.id(), ch.spec(), ch.executionId(), endpoints, ch.connections(), ch.lifeStatus());
        });
        channelsByEndpoints.put(endpoint.uri().toString(), endpoint.channelId());
    }

    @Override
    public void markEndpointBound(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        String channelId = channelsByEndpoints.get(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.endpoints().stream().map(e -> {
                if (endpointUri.equals(e.uri().toString()))
                    return Endpoint.fromSlot(e.slot(), e.slotOwner(), Endpoint.LifeStatus.BOUND);
                return e;
            }).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), endpoints, ch.connections(), ch.lifeStatus());
        });
    }

    @Override
    public void markEndpointUnbinding(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        String channelId = channelsByEndpoints.get(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.endpoints().stream().map(e -> {
                if (endpointUri.equals(e.uri().toString()))
                    return Endpoint.fromSlot(e.slot(), e.slotOwner(), Endpoint.LifeStatus.UNBINDING);
                return e;
            }).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), endpoints, ch.connections(), ch.lifeStatus());
        });
    }

    @Override
    public void markAllEndpointsUnbinding(String channelId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.endpoints().stream().map(e ->
                Endpoint.fromSlot(e.slot(), e.slotOwner(), Endpoint.LifeStatus.UNBINDING)).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), endpoints, ch.connections(), ch.lifeStatus());
        });
    }

    @Override
    public void removeEndpointWithoutConnections(String endpointUri, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        String channelId = channelsByEndpoints.remove(endpointUri);
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Endpoint> endpoints = ch.endpoints().stream()
                .filter(e -> !e.uri().toString().equals(endpointUri))
                .toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), endpoints, ch.connections(), ch.lifeStatus());
        });
    }

    @Override
    public void insertConnection(String channelId, Connection connection,
                                 @Nullable TransactionHandle transaction) throws SQLException
    {
        if (connection.status() != Connection.LifeStatus.CONNECTING) {
            throw new IllegalArgumentException("Expected " + Connection.LifeStatus.CONNECTING + " connection, actual status " + connection.status());
        }
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.connections();
            connections.add(connection);
            return new Channel(ch.id(), ch.spec(), ch.executionId(), ch.endpoints(), connections, ch.lifeStatus());
        } );
    }

    @Override
    public void markConnectionAlive(String channelId, String senderUri, String receiverUri,
                                    @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.connections().stream().map(c -> {
                if (senderUri.equals(c.sender().uri().toString()) && receiverUri.equals(c.receiver().uri().toString()))
                    return new Connection(c.sender(), c.receiver(), Connection.LifeStatus.CONNECTED);
                return c;
            }).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), ch.endpoints(), connections, ch.lifeStatus());
        });
    }

    @Override
    public void markConnectionDisconnecting(String channelId, String senderUri, String receiverUri,
                                            @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.connections().stream().map(c -> {
                if (senderUri.equals(c.sender().uri().toString()) && receiverUri.equals(c.receiver().uri().toString()))
                    return new Connection(c.sender(), c.receiver(), Connection.LifeStatus.DISCONNECTING);
                return c;
            }).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), ch.endpoints(), connections, ch.lifeStatus());
        });
    }

    @Override
    public void removeEndpointConnection(String channelId, String senderUri, String receiverUri,
                                         @Nullable TransactionHandle transaction) throws SQLException
    {
        channels.computeIfPresent(channelId, (id, ch) -> {
            final List<Connection> connections = ch.connections().stream().filter(c ->
                !(senderUri.equals(c.sender().uri().toString()) && receiverUri.equals(c.receiver().uri().toString()))
            ).toList();
            return new Channel(ch.id(), ch.spec(), ch.executionId(), ch.endpoints(), connections, ch.lifeStatus());
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
        if (channel.lifeStatus().equals(lifeStatus)) {
            return channel;
        }
        return null;
    }

    @Override
    public List<Channel> listChannels(String executionId, @Nullable TransactionHandle transaction) throws SQLException
    {
        return channels.values().stream()
            .filter(ch -> ch.executionId().equals(executionId))
            .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public Endpoint findEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        String channelId = channelsByEndpoints.get(endpointUri);
        return channels.get(channelId).endpoints().stream()
            .filter(e -> e.uri().toString().equals(endpointUri))
            .findFirst().orElse(null);
    }
}
