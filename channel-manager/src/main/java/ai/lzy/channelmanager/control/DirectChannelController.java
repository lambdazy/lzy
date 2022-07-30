package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.graph.ChannelGraph;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DirectChannelController implements ChannelController {
    private static final Logger LOG = LogManager.getLogger(DirectChannelController.class);

    @Override
    public void executeBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeBind, slot: {} to {}",
            endpoint.uri(), channelGraph.owner().name());

        switch (endpoint.slotSpec().direction()) {
            case INPUT -> { // Receiver
                if (!channelGraph.senders().isEmpty()) {
                    channelGraph.link(channelGraph.firstSender(), endpoint);
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
                } else {
                    channelGraph.addSender(endpoint);
                }
            }
            default -> throw new IllegalStateException("Unexpected slot direction: " + endpoint.slotSpec().direction());
        }
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeUnbind, slot: {} from: {}",
            endpoint.uri(), channelGraph.owner().name());

        switch (endpoint.slotSpec().direction()) {
            case INPUT -> channelGraph.removeReceiver(endpoint);
            case OUTPUT -> channelGraph.removeSender(endpoint);
            default -> throw new IllegalStateException("Unexpected slot direction: " + endpoint.slotSpec().direction());
        }
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) {
        LOG.info("destroying " + channelGraph.owner().name());

        channelGraph.receivers().forEach(Endpoint::destroy);
        channelGraph.senders().forEach(Endpoint::destroy);
    }
}
