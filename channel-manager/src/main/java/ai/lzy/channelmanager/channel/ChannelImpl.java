package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.graph.ChannelGraph;
import ai.lzy.channelmanager.graph.LocalChannelGraph;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.v1.Channels;
import java.net.URI;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ChannelImpl implements Channel {
    private static final Logger LOG = LogManager.getLogger(ChannelImpl.class);
    private final String id;
    private final ChannelGraph channelGraph;
    private final ChannelSpec spec;
    private final ChannelController controller; // pluggable channel logic

    private ChannelImpl(String id, ChannelSpec spec, ChannelController controller, ChannelGraph channelGraph) {
        this.id = id;
        this.spec = spec;
        this.controller = controller;
        this.channelGraph = channelGraph;
    }

    public static Builder newBuilder() {
        return new Builder();
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
    public String name() {
        return spec.name();
    }

    @Override
    public void bind(Endpoint endpoint) throws ChannelException {
        controller.executeBind(channelGraph, endpoint);
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

        private String id;
        private ChannelSpec spec;
        private ChannelController controller;
        private final Map<String, Endpoint> sendersByUri = new HashMap<>();
        private final Map<String, Endpoint> receiversByUri = new HashMap<>();
        private final Map<Endpoint, HashSet<Endpoint>> edges = new HashMap<>();

        public Builder setId(String id) {
            this.id = id;
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
            sendersByUri.put(senderEndpoint.uri().toString(), senderEndpoint);
            return this;
        }

        public Builder addReceiver(Endpoint receiverEndpoint) {
            receiversByUri.put(receiverEndpoint.uri().toString(), receiverEndpoint);
            return this;
        }

        public Builder addEdge(String senderUri, String receiverUri) {
            final Endpoint sender = sendersByUri.get(senderUri);
            final Endpoint receiver = receiversByUri.get(receiverUri);
            edges.computeIfAbsent(sender, k -> new HashSet<>()).add(receiver);
            return this;
        }

        public ChannelImpl build() {
            return new ChannelImpl(
                id, spec, controller, new LocalChannelGraph(
                    id, sendersByUri.values().stream(), receiversByUri.values().stream(), edges
                )
            );
        }

    }

}
