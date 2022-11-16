package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.model.slot.Slot;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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

    public Channel(String id, ChannelSpec spec, String executionId, List<Endpoint> endpoints, List<Connection> connections, LifeStatus lifeStatus) {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.senders = Senders.fromList(endpoints.stream().filter(e -> e.slotDirection() == Slot.Direction.OUTPUT).toList());
        this.receivers = Receivers.fromList(endpoints.stream().filter(e -> e.slotDirection() == Slot.Direction.INPUT).toList());
        this.connections = connections;
        this.lifeStatus = lifeStatus;
    }

    public String id() {
        return id;
    }

    public ChannelSpec spec() {
        return spec;
    }

    public String executionId() {
        return executionId;
    }

    public Senders senders() {
        return senders;
    }

    public List<Endpoint> existedSenders() {
    }

    public Receivers receivers() {
        return receivers;
    }

    public List<Endpoint> existedReceivers() {
    }


    public List<Endpoint> endpoints() {
        final List<Endpoint> endpoints = new ArrayList<>();
        endpoints.addAll(senders.asList());
        endpoints.addAll(receivers.asList());
        return endpoints;
    }

    public List<Connection> connections() {
        return connections;
    }

    public LifeStatus lifeStatus() {
        return lifeStatus;
    }

    public Endpoint endpoint(URI endpointUri) {

    }

    public List<Endpoint> endpoints(Slot.Direction slotDirection) {

    }

    @Nullable
    public Connection connection(URI senderUri, URI receiverUri) {
        return
    }

    public List<Connection> connections(URI endpointUri) {
        return
    }

    @Nullable
    public Endpoint findSenderToConnect(Endpoint receiver) {
        return switch (receiver.slotOwner()) {
            case PORTAL -> senders.workerEndpoint;
            case WORKER -> senders.portalEndpoint == null ? senders.workerEndpoint : senders.portalEndpoint;
        };
    }


    public List<Endpoint> findReceiversToConnect(Endpoint sender) {
        List<Endpoint> endpoints = receivers.workerEndpoints.stream()
            .filter(e -> connections().ofEndpoint(e).isEmpty())
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        if (!Endpoint.SlotOwner.PORTAL.equals(sender.slotOwner()) && receivers.portalEndpoint != null) {
            endpoints.add(receivers.portalEndpoint);
        }
        return endpoints;
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
                if (sender.slotDirection() != Slot.Direction.OUTPUT) {
                    throw new IllegalArgumentException("Wrong endpoint direction");
                }
                switch (sender.slotOwner()) {
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
            if (workerEndpoint != null) senders.add(workerEndpoint);
            if (portalEndpoint != null) senders.add(portalEndpoint);
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
                if (receiver.slotDirection() != Slot.Direction.INPUT) {
                    throw new IllegalArgumentException("Wrong endpoint direction");
                }
                switch (receiver.slotOwner()) {
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
            if (portalEndpoint != null) receivers.add(portalEndpoint);
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

    public static class Connections {

        Map<URI, Set<URI>> connections;

        public Set<Endpoint> ofEndpoint(Endpoint endpoint) {
            return null;
        }

    }

    public enum LifeStatus {
        ALIVE,
        DESTROYING,
    }

}
