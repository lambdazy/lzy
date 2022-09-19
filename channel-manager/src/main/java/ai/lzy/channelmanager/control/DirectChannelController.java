package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.graph.ChannelGraph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class DirectChannelController implements ChannelController {
    private static final Logger LOG = LogManager.getLogger(DirectChannelController.class);

    @Override
    public Stream<Endpoint> executeBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeBind, slot: {} to {}",
            endpoint.uri(), channelGraph.ownerChannelId());

        List<Endpoint> connected = new ArrayList<>();
        switch (endpoint.slotSpec().direction()) {
            case INPUT -> { // Receiver
                if (!channelGraph.senders().isEmpty()) {
                    channelGraph.link(channelGraph.firstSender(), endpoint);
                    connected.add(channelGraph.firstSender());
                } else {
                    channelGraph.addReceiver(endpoint);
                }
            }
            case OUTPUT -> { // Sender
                final Set<Endpoint> inputs = channelGraph.senders();
                if (!inputs.isEmpty() && !inputs.contains(endpoint)) {
                    throw new ChannelException("Direct channel can not have two senders");
                }
                // TODO(d-kruchinin): is here bug? iterate over all receivers
                if (!channelGraph.receivers().isEmpty()) {
                    channelGraph.link(endpoint, channelGraph.firstReceiver());
                    connected.add(channelGraph.firstReceiver());
                } else {
                    channelGraph.addSender(endpoint);
                }
            }
            default -> throw new IllegalStateException("Unexpected slot direction: " + endpoint.slotSpec().direction());
        }
        return connected.stream();
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeUnbind, slot: {} from: {}",
            endpoint.uri(), channelGraph.ownerChannelId());

        switch (endpoint.slotSpec().direction()) {
            case INPUT -> channelGraph.removeReceiver(endpoint);
            case OUTPUT -> channelGraph.removeSender(endpoint);
            default -> throw new IllegalStateException("Unexpected slot direction: " + endpoint.slotSpec().direction());
        }
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) {
        LOG.info("destroying " + channelGraph.ownerChannelId());

        channelGraph.receivers().forEach(Endpoint::destroy);
        channelGraph.senders().forEach(Endpoint::destroy);
    }
}
