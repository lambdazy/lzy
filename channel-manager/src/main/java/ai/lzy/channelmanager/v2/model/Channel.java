package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.model.slot.Slot;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.channelmanager.v2.model.Endpoint.SlotOwner.PORTAL;

public class Channel {
    private final String id;
    private final ChannelSpec spec;
    private final String executionId;
    private final Senders senders;
    private final Receivers receivers;
    private final List<Connection> connections;
    private final LifeStatus lifeStatus;

    public Channel(String id, ChannelSpec spec, String executionId) {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.senders = new Senders();
        this.receivers = new Receivers();
        this.connections = new ArrayList<>();
        this.lifeStatus = LifeStatus.ALIVE;
    }

    public Channel(String id, ChannelSpec spec, String executionId, List<Endpoint> endpoints,
                   List<Connection> connections, LifeStatus lifeStatus)
    {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.senders = Senders.fromList(endpoints.stream()
            .filter(e -> e.getSlotDirection() == Slot.Direction.OUTPUT)
            .toList());
        this.receivers = Receivers.fromList(endpoints.stream()
            .filter(e -> e.getSlotDirection() == Slot.Direction.INPUT)
            .toList());
        this.connections = connections;
        this.lifeStatus = lifeStatus;
    }

    public String getId() {
        return id;
    }

    public ChannelSpec getSpec() {
        return spec;
    }

    public String getExecutionId() {
        return executionId;
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
            case PORTAL -> senders.workerEndpoint;
            case WORKER -> senders.portalEndpoint == null ? senders.workerEndpoint : senders.portalEndpoint;
        };
    }

    public List<Endpoint> findReceiversToConnect(Endpoint sender) {
        return receivers.asList().stream()
            .filter(receiver -> getConnectionsOfEndpoint(receiver.getUri()).isEmpty())
            .filter(receiver -> !(sender.getSlotOwner() == PORTAL && receiver.getSlotOwner() == PORTAL))
            .collect(Collectors.toList());
    }

    public static class Senders {

        @Nullable
        private final Endpoint workerEndpoint;

        @Nullable
        private final Endpoint portalEndpoint;

        public Senders() {
            this.workerEndpoint = null;
            this.portalEndpoint = null;
        }

        public Senders(@Nullable Endpoint portalEndpoint, @Nullable Endpoint workerEndpoint) {
            this.workerEndpoint = workerEndpoint;
            this.portalEndpoint = portalEndpoint;
        }

        private static Senders fromList(List<Endpoint> senders) {
            Endpoint portalEndpoint = null;
            Endpoint workerEndpoint = null;
            for (final Endpoint sender : senders) {
                if (sender.getSlotDirection() != Slot.Direction.OUTPUT) {
                    throw new IllegalArgumentException("Wrong endpoint direction");
                }
                switch (sender.getSlotOwner()) {
                    case PORTAL -> {
                        if (portalEndpoint != null) {
                            throw new IllegalArgumentException("Multiple portal endpoints");
                        }
                        portalEndpoint = sender;
                    }
                    case WORKER -> {
                        if (workerEndpoint != null) {
                            throw new IllegalArgumentException("Multiple worker endpoints");
                        }
                        workerEndpoint = sender;
                    }
                }
            }
            return new Senders(portalEndpoint, workerEndpoint);
        }

        public List<Endpoint> asList() {
            List<Endpoint> senders = new ArrayList<>();
            if (workerEndpoint != null) {
                senders.add(workerEndpoint);
            }
            if (portalEndpoint != null) {
                senders.add(portalEndpoint);
            }
            return senders;
        }

        @Nullable
        public Endpoint workerEndpoint() {
            return workerEndpoint;
        }

        @Nullable
        public Endpoint portalEndpoint() {
            return portalEndpoint;
        }

    }

    public static class Receivers {

        private final List<Endpoint> workerEndpoints;

        @Nullable
        private final Endpoint portalEndpoint;

        public Receivers() {
            workerEndpoints = new ArrayList<>();
            portalEndpoint = null;
        }

        public Receivers(@Nullable Endpoint portalEndpoint, List<Endpoint> workerEndpoints) {
            this.workerEndpoints = new ArrayList<>(workerEndpoints);
            this.portalEndpoint = portalEndpoint;
        }

        private static Receivers fromList(List<Endpoint> receivers) {
            Endpoint portalEndpoint = null;
            final List<Endpoint> workerEndpoints = new ArrayList<>();
            for (final Endpoint receiver : receivers) {
                if (receiver.getSlotDirection() != Slot.Direction.INPUT) {
                    throw new IllegalArgumentException("Wrong endpoint direction");
                }
                switch (receiver.getSlotOwner()) {
                    case PORTAL -> {
                        if (portalEndpoint != null) {
                            throw new IllegalArgumentException("Multiple portal endpoints");
                        }
                        portalEndpoint = receiver;
                    }
                    case WORKER -> workerEndpoints.add(receiver);
                }
            }
            return new Receivers(portalEndpoint, workerEndpoints);
        }

        public List<Endpoint> asList() {
            List<Endpoint> receivers = new ArrayList<>(workerEndpoints);
            if (portalEndpoint != null) {
                receivers.add(portalEndpoint);
            }
            return receivers;
        }

        public List<Endpoint> workerEndpoints() {
            return workerEndpoints;
        }

        @Nullable
        public Endpoint portalEndpoint() {
            return portalEndpoint;
        }

    }

    public enum LifeStatus {
        ALIVE,
        DESTROYING,
    }

}
