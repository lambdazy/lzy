package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.graph.ChannelGraph;
import ai.lzy.channelmanager.graph.LocalChannelGraph;
import ai.lzy.model.slot.SlotStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class ChannelImpl implements Channel {

    private static final Logger LOG = LogManager.getLogger(ChannelImpl.class);

    private final String id;
    private final String executionId;
    private final ChannelGraph channelGraph;
    private final ChannelSpec spec;
    private final ChannelController controller; // pluggable channel logic

    public static Builder newBuilder() {
        return new Builder();
    }

    private ChannelImpl(
        String id, String executionId, ChannelSpec spec, ChannelController controller, ChannelGraph channelGraph
    ) {
        this.id = id;
        this.executionId = executionId;
        this.spec = spec;
        this.controller = controller;
        this.channelGraph = channelGraph;
    }

    @Override
    public ChannelSpec spec() {
        return spec;
    }

    @Override
    public void destroy() {
        try {
            controller.executeDestroy(channelGraph);
        } catch (ChannelException e) {
            LOG.warn("Exception during channel " + spec.name() + " destruction", e);
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String executionId() {
        return executionId;
    }

    @Override
    public String name() {
        return spec.name();
    }

    @Override
    public Stream<Endpoint> bind(Endpoint endpoint) throws ChannelException {
        return controller.executeBind(channelGraph, endpoint);
    }

    @Override
    public void unbind(Endpoint endpoint) throws ChannelException {
        if (!channelGraph.hasBound(endpoint)) {
            LOG.warn(MessageFormat.format(
                "Slot {0} is not bound to the channel {1}",
                endpoint.uri(), spec.name()
            ));
            return;
        }
        controller.executeUnBind(channelGraph, endpoint);
    }

    @Override
    public Stream<Endpoint> bound() {
        return Stream.concat(
            channelGraph.senders().stream(),
            channelGraph.receivers().stream()
        );
    }

    @Override
    public Stream<Endpoint> bound(Endpoint endpoint) {
        return channelGraph.adjacent(endpoint).stream();
    }

    @Override
    public boolean hasBound(Endpoint endpoint) {
        return channelGraph.hasBound(endpoint);
    }

    @Override
    public Stream<SlotStatus> slotsStatus() {
        return bound().map(Endpoint::status).filter(Objects::nonNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ChannelImpl channel = (ChannelImpl) o;
        return id.equals(channel.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public static class Builder implements Channel.Builder {

        private static final Logger LOG = LogManager.getLogger(ChannelImpl.Builder.class);

        private String id;
        private String executionId;
        private ChannelSpec spec;
        private ChannelController controller;
        private final Map<String, Endpoint> sendersByUri = new HashMap<>();
        private final Map<String, Endpoint> receiversByUri = new HashMap<>();
        private final Map<String, HashSet<String>> edgesUri = new HashMap<>();

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setExecutionId(String executionId) {
            this.executionId = executionId;
            return this;
        }

        public Builder setSpec(ChannelSpec spec) {
            this.spec = spec;
            return this;
        }

        public Builder setController(ChannelController controller) {
            this.controller = controller;
            return this;
        }

        public Builder addSender(Endpoint senderEndpoint) {
            Endpoint prev = sendersByUri.put(senderEndpoint.uri().toString(), senderEndpoint);
            if (prev != null) {
                LOG.warn("Sender endpoint {} was rewritten by endpoint {}",
                    prev.slotInstance(), senderEndpoint.slotInstance());
            }
            return this;
        }

        public Builder addReceiver(Endpoint receiverEndpoint) {
            Endpoint prev = receiversByUri.put(receiverEndpoint.uri().toString(), receiverEndpoint);
            if (prev != null) {
                LOG.warn("Receiver endpoint {} was rewritten by endpoint {}",
                    prev.slotInstance(), receiverEndpoint.slotInstance());
            }
            return this;
        }

        public Builder addEdge(String senderUri, String receiverUri) {
            boolean wasAdded = edgesUri.computeIfAbsent(senderUri, k -> new HashSet<>()).add(receiverUri);
            if (!wasAdded) {
                LOG.warn("Edge from sender {} to receiver {} was already exists", senderUri, receiverUri);
            }
            return this;
        }

        public ChannelImpl build() {
            final Map<Endpoint, HashSet<Endpoint>> edges = new HashMap<>();
            edgesUri.forEach((senderUri, receivers) ->
                receivers.forEach(receiverUri -> {
                    var sender = sendersByUri.get(senderUri);
                    var receiver = receiversByUri.get(receiverUri);
                    edges.computeIfAbsent(sender, k -> new HashSet<>()).add(receiver);
                })
            );

            return new ChannelImpl(
                id, executionId, spec, controller, new LocalChannelGraph(
                    id, sendersByUri.values().stream(), receiversByUri.values().stream(), edges
                )
            );
        }

    }

}
