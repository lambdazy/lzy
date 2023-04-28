package ai.lzy.channelmanager.model.channel;

import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.model.Endpoint.SlotOwner.PORTAL;

public class Channel {
    private final String id;
    private final Channel.Spec spec;
    private final String executionId;
    private final String workflowName;
    private final String userId;
    private final Senders senders;
    private final Receivers receivers;
    private final List<Connection> connections;
    private final LifeStatus lifeStatus;

    private Channel(String id, Channel.Spec spec, String executionId, String workflowName, String userId,
                    List<Endpoint> endpoints, List<Connection> connections, LifeStatus lifeStatus)
    {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.workflowName = workflowName;
        this.userId = userId;
        this.senders = Senders.fromList(endpoints.stream()
            .filter(e -> e.getSlotDirection() == Slot.Direction.OUTPUT)
            .toList());
        this.receivers = Receivers.fromList(endpoints.stream()
            .filter(e -> e.getSlotDirection() == Slot.Direction.INPUT)
            .toList());
        this.connections = connections;
        this.lifeStatus = lifeStatus;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public Channel.Spec getSpec() {
        return spec;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getUserId() {
        return userId;
    }

    public Senders getSenders() {
        return senders;
    }

    public Receivers getReceivers() {
        return receivers;
    }

    public Senders getActiveSenders() {
        return Senders.fromList(senders.asList().stream()
            .filter(s -> s.getStatus() == Endpoint.LifeStatus.BINDING || s.getStatus() == Endpoint.LifeStatus.BOUND)
            .toList());
    }

    public Receivers getActiveReceivers() {
        return Receivers.fromList(receivers.asList().stream()
            .filter(s -> s.getStatus() == Endpoint.LifeStatus.BINDING || s.getStatus() == Endpoint.LifeStatus.BOUND)
            .toList());
    }

    public List<Endpoint> getEndpoints() {
        final List<Endpoint> endpoints = new ArrayList<>();
        endpoints.addAll(senders.asList());
        endpoints.addAll(receivers.asList());
        return endpoints;
    }

    @Nullable
    public Endpoint getEndpoint(URI endpointUri) {
        return getEndpoints().stream()
            .filter(e -> endpointUri.equals(e.getUri()))
            .findFirst().orElse(null);
    }

    public List<Connection> getConnections() {
        return connections;
    }

    @Nullable
    public Connection getConnection(URI senderUri, URI receiverUri) {
        return connections.stream()
            .filter(c -> senderUri.equals(c.sender().getUri()) && receiverUri.equals(c.receiver().getUri()))
            .findFirst().orElse(null);
    }

    public List<Connection> getConnectionsOfEndpoint(URI endpointUri) {
        return connections.stream()
            .filter(c -> endpointUri.equals(c.sender().getUri()) || endpointUri.equals(c.receiver().getUri()))
            .collect(Collectors.toList());
    }

    public LifeStatus getLifeStatus() {
        return lifeStatus;
    }

    @Nullable
    public Endpoint findSenderToConnect(Endpoint receiver) {
        if (!getConnectionsOfEndpoint(receiver.getUri()).isEmpty()) {
            return null;
        }
        return switch (receiver.getSlotOwner()) {
            case PORTAL -> senders.workerEndpoint();
            case WORKER -> senders.portalEndpoint() == null ? senders.workerEndpoint() : senders.portalEndpoint();
        };
    }

    public List<Endpoint> findReceiversToConnect(Endpoint sender) {
        return receivers.asList().stream()
            .filter(receiver -> getConnectionsOfEndpoint(receiver.getUri()).isEmpty())
            .filter(receiver -> !(sender.getSlotOwner() == PORTAL && receiver.getSlotOwner() == PORTAL))
            .collect(Collectors.toList());
    }

    public record Spec(
        String name,
        DataScheme contentType
    ) {}

    public enum LifeStatus {
        ALIVE,
        DESTROYING,
    }

    public static class Builder {
        private String id;
        private String executionId;
        private String workflowName;
        private String userId;
        private Channel.Spec spec;
        private LifeStatus lifeStatus;
        private final HashSet<Endpoint> endpoints = new HashSet<>();
        private final HashSet<Connection> connections = new HashSet<>();

        private Builder() { }

        public Channel build() {
            return new Channel(id, spec, executionId, workflowName, userId,
                endpoints.stream().toList(), connections.stream().toList(), lifeStatus);
        }

        public Builder setChannelId(String value) {
            this.id = value;
            return this;
        }

        public Builder setExecutionId(String value) {
            this.executionId = value;
            return this;
        }

        public Builder setWorkflowName(String value) {
            this.workflowName = value;
            return this;
        }

        public Builder setUserId(String value) {
            this.userId = value;
            return this;
        }

        public Builder setChannelSpec(Spec value) {
            this.spec = value;
            return this;
        }

        public Builder setChannelLifeStatus(LifeStatus value) {
            this.lifeStatus = value;
            return this;
        }

        public Builder addEndpoint(Endpoint value) {
            endpoints.add(value);
            return this;
        }

        public Builder addConnection(Connection value) {
            connections.add(value);
            return this;
        }

    }

}
