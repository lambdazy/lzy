package ai.lzy.channelmanager.deprecated.graph;

import ai.lzy.channelmanager.deprecated.channel.ChannelException;
import ai.lzy.channelmanager.deprecated.channel.Endpoint;
import ai.lzy.model.slot.Slot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;

public class LocalChannelGraph implements ChannelGraph {
    private static final Logger LOG = LogManager.getLogger(LocalChannelGraph.class);
    private final Set<Endpoint> senders;
    private final Set<Endpoint> receivers;
    private final Map<Endpoint, HashSet<Endpoint>> edges;
    private final String ownerChannelId;

    public LocalChannelGraph(String channelId) {
        this(channelId, Stream.empty(), Stream.empty(), Map.of());
    }

    public LocalChannelGraph(
        String channelId,
        Stream<Endpoint> senders,
        Stream<Endpoint> receivers,
        Map<Endpoint, HashSet<Endpoint>> edges
    ) {
        this.ownerChannelId = channelId;
        this.senders = senders.collect(Collectors.toSet());
        this.receivers = receivers.collect(Collectors.toSet());
        this.edges = edges;
    }

    private static void checkConsistency(Endpoint sender, Endpoint receiver) {
        if (sender.slotSpec().direction() != Slot.Direction.OUTPUT) {
            throw new ChannelGraphException(
                "Endpoint input: " + sender + " was passed as sender but has direction " + sender.slotSpec().direction()
            );
        }
        if (receiver.slotSpec().direction() != Slot.Direction.INPUT) {
            throw new ChannelGraphException(
                "Endpoint output: " + receiver + " was passed as receiver but has direction " + receiver.slotSpec()
                    .direction()
            );
        }
    }

    @Override
    public String ownerChannelId() {
        return ownerChannelId;
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
    public Set<Endpoint> adjacent(Endpoint endpoint) {
        if (senders.contains(endpoint)) {
            return edges.getOrDefault(endpoint, new HashSet<>());
        }
        if (receivers.contains(endpoint)) {
            return getSenders(endpoint);
        }
        return new HashSet<>();
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
    public void link(@NotNull Endpoint sender, @NotNull Endpoint receiver) throws ChannelException {
        LOG.info("Linking sender " + sender + " to receiver " + receiver);

        checkConsistency(sender, receiver);

        final int rc = receiver.connect(sender.slotInstance());
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
    public void removeSender(@NotNull Endpoint sender) throws ChannelException {
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
    public void removeReceiver(@NotNull Endpoint receiver) throws ChannelException {
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
                if (adjSender.disconnect() != 0) {
                    LOG.warn("Failed to disconnect sender: " + adjSender);
                }
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
