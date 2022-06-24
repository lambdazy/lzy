package ai.lzy.server.channel;

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

    void link(@NotNull Endpoint from, @NotNull Endpoint to);
    void removeSender(@NotNull Endpoint sender);
    void removeReceiver(@NotNull Endpoint receiver);

    boolean hasBound(@NotNull Endpoint endpoint);
}
