package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.channel.ChannelSpec;

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
    private final Connections connections;

    public Channel(String id, ChannelSpec spec, String executionId) {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.senders = new Senders();
        this.receivers = new Receivers();
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

    public Connections connections() {
        return connections;
    }

    public Endpoint endpoint(URI endpointUri) {

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

    private static class Senders {

        @Nullable
        private final Endpoint workerEndpoint = null;

        @Nullable
        private final Endpoint portalEndpoint = null;

    }

    private static class Receivers {

        private final List<Endpoint> workerEndpoints = new ArrayList<>();

        @Nullable
        private final Endpoint portalEndpoint = null;

    }

    public static class Connections {

        Map<URI, Set<URI>> connections;

        public Set<Endpoint> ofEndpoint(Endpoint endpoint) {
            return null;
        }

    }

}
