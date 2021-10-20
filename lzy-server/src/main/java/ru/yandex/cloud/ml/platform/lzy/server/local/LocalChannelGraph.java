package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.lang.NonNull;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelGraph;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelGraphException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class LocalChannelGraph implements ChannelGraph {
    private static final Logger LOG = LogManager.getLogger(LocalChannelGraph.class);
    private final Set<Endpoint> senders = new HashSet<>();
    private final Set<Endpoint> receivers = new HashSet<>();
    private final Map<Endpoint, HashSet<Endpoint>> edges = new HashMap<>();

    @Override
    public Set<Endpoint> senders() {
        return senders;
    }

    @Override
    public Set<Endpoint> receivers() {
        return receivers;
    }

    @Override
    public synchronized void addSender(@NonNull Endpoint sender) {
        if (senders.contains(sender)) {
            LOG.warn("Endpoint input: " + sender + " already connected to channelGraph");
        }
        senders.add(sender);
    }

    @Override
    public synchronized void addReceiver(@NonNull Endpoint receiver) {
        if (receivers.contains(receiver)) {
            LOG.warn("Endpoint output: " + receiver + " already connected to channelGraph");
        }
        receivers.add(receiver);
    }

    @Override
    public void link(@NonNull Endpoint sender, @NonNull Endpoint receiver) {
        LOG.info("Linking sender " + sender + " to receiver " + receiver);

        checkConsistency(sender, receiver);

        int rc;
        if (sender instanceof TerminalEndpoint) {
            rc = sender.connect(receiver);
        } else {
            rc = receiver.connect(sender);
        }
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
    public void removeSender(@NonNull Endpoint sender) {
        LOG.info("Removing sender " + sender);

        if (!senders.contains(sender)) {
            throw new ChannelGraphException(String.format("Endpoint %s is not sender", sender));
        }
        if (numConnections(sender) != 0) {
            throw new ChannelGraphException(
                "Attempt to unlink sender " + sender + " from channelGraph while there are connected receivers"
            );
        }
        senders.remove(sender);
        edges.remove(sender);
        final int rc = sender.destroy();
        if (rc > 0) {
            throw new ChannelException("Failed to destroy " + sender);
        }
    }

    @Override
    public void removeReceiver(@NonNull Endpoint receiver) {
        LOG.info("Removing receiver " + receiver);

        if (!receivers.contains(receiver)) {
            throw new ChannelGraphException(String.format("Endpoint %s is not receiver", receiver));
        }

        int rcSum = receiver.disconnect();
        rcSum += receiver.destroy();
        if (rcSum > 0) {
            throw new ChannelException("Failed to unbind " + receiver);
        }

        for (final Endpoint adjSender: getSenders(receiver)) {
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
    public void destroySender(@NonNull Endpoint sender) {
        senders.remove(sender);
        edges.remove(sender);
        sender.destroy();
    }

    @Override
    public void destroyReceiver(@NonNull Endpoint receiver) {
        receivers.remove(receiver);
        for (Set<Endpoint> adjEndpoints: edges.values()) {
            adjEndpoints.remove(receiver);
        }
        receiver.destroy();
    }

    @Override
    public boolean hasBound(@NonNull Endpoint endpoint) {
        return senders.contains(endpoint) || receivers.contains(endpoint);
    }

    private static void checkConsistency(Endpoint sender, Endpoint receiver) {
        if (sender.slot().direction() != Slot.Direction.OUTPUT) {
            throw new ChannelGraphException(
                "Endpoint input: " + sender + " was passed as sender but has direction " + sender.slot().direction()
            );
        }
        if (receiver.slot().direction() != Slot.Direction.INPUT) {
            throw new ChannelGraphException(
                "Endpoint output: " + receiver + " was passed as receiver but has direction " + receiver.slot().direction()
            );
        }
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
            return edges.get(endpoint).size();
        }
        return edges.entrySet().stream()
            .filter(entry -> entry.getValue().contains(endpoint))
            .count();
    }
}
