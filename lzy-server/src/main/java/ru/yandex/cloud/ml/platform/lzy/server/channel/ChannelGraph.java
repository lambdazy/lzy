package ru.yandex.cloud.ml.platform.lzy.server.channel;

import org.springframework.lang.NonNull;

import java.util.Set;

public interface ChannelGraph {
    Set<Endpoint> senders();
    Set<Endpoint> receivers();

    default Endpoint firstSender() {
        return senders().iterator().next();
    }

    default Endpoint firstReceiver() {
        return receivers().iterator().next();
    }

    void addSender(@NonNull Endpoint sender);
    void addReceiver(@NonNull Endpoint receiver);

    void link(@NonNull Endpoint from, @NonNull Endpoint to);

    void removeSender(@NonNull Endpoint sender);
    void removeReceiver(@NonNull Endpoint receiver);

    boolean hasBound(@NonNull Endpoint endpoint);
}
