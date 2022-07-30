package ai.lzy.channelmanager.graph;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;

import java.util.Set;
import javax.validation.constraints.NotNull;

public interface ChannelGraph {
    Channel owner();

    Set<Endpoint> senders();
    Set<Endpoint> receivers();

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
