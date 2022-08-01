package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.graph.ChannelGraph;
import ai.lzy.channelmanager.graph.LocalChannelGraph;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.channel.ChannelSpec;
import java.text.MessageFormat;
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

    public ChannelImpl(String id, ChannelSpec spec, ChannelController controller) {
        this.id = id;
        this.spec = spec;
        this.controller = controller;
        this.channelGraph = new LocalChannelGraph(this);
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
}
