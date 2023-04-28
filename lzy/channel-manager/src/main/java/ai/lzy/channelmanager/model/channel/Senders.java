package ai.lzy.channelmanager.model.channel;

import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.model.slot.Slot;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class Senders {

    @Nullable
    private final Endpoint workerEndpoint;

    @Nullable
    private final Endpoint portalEndpoint;

    private Senders(@Nullable Endpoint portalEndpoint, @Nullable Endpoint workerEndpoint) {
        this.workerEndpoint = workerEndpoint;
        this.portalEndpoint = portalEndpoint;
    }

    public static Senders fromList(List<Endpoint> senders) {
        Endpoint portalEndpoint = null;
        Endpoint workerEndpoint = null;
        for (final Endpoint sender : senders) {
            if (sender.getSlotDirection() != Slot.Direction.OUTPUT) {
                throw new IllegalArgumentException("Wrong endpoint direction");
            }
            switch (sender.getSlotOwner()) {
                case PORTAL -> {
                    if (portalEndpoint != null) {
                        throw new IllegalArgumentException("Multiple portal endpoints");
                    }
                    portalEndpoint = sender;
                }
                case WORKER -> {
                    if (workerEndpoint != null) {
                        throw new IllegalArgumentException("Multiple worker endpoints");
                    }
                    workerEndpoint = sender;
                }
            }
        }
        return new Senders(portalEndpoint, workerEndpoint);
    }

    public List<Endpoint> asList() {
        List<Endpoint> senders = new ArrayList<>();
        if (workerEndpoint != null) {
            senders.add(workerEndpoint);
        }
        if (portalEndpoint != null) {
            senders.add(portalEndpoint);
        }
        return senders;
    }

    @Nullable
    public Endpoint workerEndpoint() {
        return workerEndpoint;
    }

    @Nullable
    public Endpoint portalEndpoint() {
        return portalEndpoint;
    }

}
