package ai.lzy.server.local;

import ai.lzy.server.channel.Channel;
import ai.lzy.server.channel.ChannelException;
import ai.lzy.server.channel.ChannelGraph;
import ai.lzy.server.channel.ChannelGraphException;
import ai.lzy.server.channel.Endpoint;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.Slot;

public class LocalChannelGraph implements ChannelGraph {
    private static final Logger LOG = LogManager.getLogger(LocalChannelGraph.class);
    private final Set<Endpoint> senders = new HashSet<>();
    private final Set<Endpoint> receivers = new HashSet<>();
    private final Map<Endpoint, HashSet<Endpoint>> edges = new HashMap<>();
    private final Channel owner;

    public LocalChannelGraph(Channel channel) {
        this.owner = channel;
    }

    private static void checkConsistency(Endpoint sender, Endpoint receiver) {
        if (sender.slot().direction() != Slot.Direction.OUTPUT) {
            throw new ChannelGraphException(
                "Endpoint input: " + sender + " was passed as sender but has direction " + sender.slot().direction()
            );
        }
        if (receiver.slot().direction() != Slot.Direction.INPUT) {
            throw new ChannelGraphException(
                "Endpoint output: " + receiver + " was passed as receiver but has direction " + receiver.slot()
                    .direction()
            );
        }
    }

    @Override
    public Channel owner() {
        return owner;
    }

    @Override
    public Set<Endpoint> senders() {
        return new HashSet<>(senders);
    }

    @Override
    public Set<Endpoint> receivers() {
        return new HashSet<>(receivers);
    }

    @Override
    public synchronized void addSender(@NotNull Endpoint sender) {
        if (senders.contains(sender)) {
            LOG.warn("Endpoint input: " + sender + " already connected to channelGraph");
        }
        senders.add(sender);
    }

    @Override
    public synchronized void addReceiver(@NotNull Endpoint receiver) {
        if (receivers.contains(receiver)) {
            LOG.warn("Endpoint output: " + receiver + " already connected to channelGraph");
        }
        receivers.add(receiver);
    }

    @Override
    public void link(@NotNull Endpoint sender, @NotNull Endpoint receiver) {
        LOG.info("Linking sender " + sender + " to receiver " + receiver);

        checkConsistency(sender, receiver);

        final int rc = receiver.connect(sender.uri());
        if (rc != 0) {
            throw new ChannelException(MessageFormat.format(
                "Failure rc:{2} while connecting sender:{0} to receiver:{1}",
                sender, receiver, rc
            ));
        }

        senders.add(sender);
        receivers.add(receiver);
        final Set<Endpoint> adjacent = edges.get(sender);

        if (adjacent == null) {
            edges.put(sender, new HashSet<>(Collections.singletonList(receiver)));
            return;
        }

        if (adjacent.contains(receiver)) {
            LOG.warn("Endpoint input: " + sender + " has already been linked to endpoint output: " + receiver);
        }
        adjacent.add(receiver);
    }

    @Override
    public void removeSender(@NotNull Endpoint sender) {
        LOG.info("Removing sender " + sender);

        if (!senders.contains(sender)) {
            throw new ChannelGraphException(String.format("Endpoint %s is not sender", sender));
        }
        if (numConnections(sender) != 0) {
            LOG.warn("Attempt to unlink sender {} from channelGraph while there are connected receivers", sender);
            for (Endpoint receiver : edges.get(sender)) {
                removeReceiver(receiver);
            }
        }
        senders.remove(sender);
        edges.remove(sender);
        final int rc = sender.destroy();
        if (rc > 0) {
            throw new ChannelException("Failed to destroy " + sender);
        }
    }

    @Override
    public void removeReceiver(@NotNull Endpoint receiver) {
        LOG.info("Removing receiver " + receiver);

        if (!receivers.contains(receiver)) {
            throw new ChannelGraphException(String.format("Endpoint %s is not receiver", receiver));
        }

        int rcSum = receiver.disconnect();
        rcSum += receiver.destroy();
        if (rcSum > 0) {
            throw new ChannelException("Failed to unbind " + receiver);
        }

        for (final Endpoint adjSender : getSenders(receiver)) {
            final HashSet<Endpoint> adjacent = edges.get(adjSender);
            adjacent.remove(receiver);
            if (adjacent.isEmpty()) {
                LOG.info("No connections from sender: " + adjSender + " disconnecting it");
                ForkJoinPool.commonPool().execute(() -> {
                    if (adjSender.disconnect() != 0) {
                        LOG.warn("Failed to disconnect sender: " + adjSender);
                    }
                });
            }
        }
        receivers.remove(receiver);
    }

    @Override
    public boolean hasBound(@NotNull Endpoint endpoint) {
        return senders.contains(endpoint) || receivers.contains(endpoint);
    }

    private Set<Endpoint> getSenders(Endpoint receiver) {
        assert receivers.contains(receiver);
        return edges.entrySet().stream()
            .filter(entry -> entry.getValue().contains(receiver))
            .map(Map.Entry::getKey)
            .collect(Collectors.toCollection(HashSet::new));
    }

    private long numConnections(Endpoint endpoint) {
        if (senders.contains(endpoint)) {
            return !edges.containsKey(endpoint) ? 0 : edges.get(endpoint).size();
        }
        return edges.entrySet().stream()
            .filter(entry -> entry.getValue().contains(endpoint))
            .count();
    }
}
