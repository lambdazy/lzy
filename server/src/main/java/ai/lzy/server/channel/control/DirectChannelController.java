package ai.lzy.server.channel.control;

import ai.lzy.server.channel.ChannelController;
import ai.lzy.server.channel.ChannelException;
import ai.lzy.server.channel.ChannelGraph;
import ai.lzy.server.channel.Endpoint;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DirectChannelController implements ChannelController {
    private static final Logger LOG = LogManager.getLogger(DirectChannelController.class);

    @Override
    public void executeBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeBind, slot: {} to {}",
            endpoint.uri(), channelGraph.owner().name());

        switch (endpoint.slot().direction()) {
            case INPUT: { // Receiver
                if (!channelGraph.senders().isEmpty()) {
                    channelGraph.link(channelGraph.firstSender(), endpoint);
                } else {
                    channelGraph.addReceiver(endpoint);
                }
                break;
            }
            case OUTPUT: { // Sender
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
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + endpoint.slot().direction());
        }
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint endpoint) throws ChannelException {
        LOG.info("DirectChannelController::executeUnbind, slot: {} from: {}",
            endpoint.uri(), channelGraph.owner().name());

        switch (endpoint.slot().direction()) {
            case INPUT: { // Receiver
                channelGraph.removeReceiver(endpoint);
                break;
            }
            case OUTPUT: { // Sender
                channelGraph.removeSender(endpoint);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + endpoint.slot().direction());
        }
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        LOG.info("destroying " + channelGraph.owner().name());

        channelGraph.receivers().forEach(Endpoint::destroy);
        channelGraph.senders().forEach(Endpoint::destroy);
    }
}
