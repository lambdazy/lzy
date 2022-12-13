package ai.lzy.channelmanager.deprecated.graph;

import ai.lzy.channelmanager.deprecated.channel.ChannelException;
import ai.lzy.channelmanager.deprecated.channel.Endpoint;

import java.util.Set;
import javax.validation.constraints.NotNull;

public interface ChannelGraph {
    String ownerChannelId();

    Set<Endpoint> senders();
    Set<Endpoint> receivers();
    Set<Endpoint> adjacent(Endpoint endpoint);

    default Endpoint firstSender() {
        return senders().iterator().next();
    }
    default Endpoint firstReceiver() {
        return receivers().iterator().next();
    }

    void addSender(@NotNull Endpoint sender);
    void addReceiver(@NotNull Endpoint receiver);

    void link(@NotNull Endpoint from, @NotNull Endpoint to) throws ChannelException;
    void removeSender(@NotNull Endpoint sender) throws ChannelException;
    void removeReceiver(@NotNull Endpoint receiver) throws ChannelException;

    boolean hasBound(@NotNull Endpoint endpoint);
}
