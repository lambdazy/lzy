package ai.lzy.channelmanager.model.channel;

import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.model.slot.Slot;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class Receivers {

    private final List<Endpoint> workerEndpoints;

    @Nullable
    private final Endpoint portalEndpoint;

    private Receivers(@Nullable Endpoint portalEndpoint, List<Endpoint> workerEndpoints) {
        this.workerEndpoints = new ArrayList<>(workerEndpoints);
        this.portalEndpoint = portalEndpoint;
    }

    public static Receivers fromList(List<Endpoint> receivers) {
        Endpoint portalEndpoint = null;
        final List<Endpoint> workerEndpoints = new ArrayList<>();
        for (final Endpoint receiver : receivers) {
            if (receiver.getSlotDirection() != Slot.Direction.INPUT) {
                throw new IllegalArgumentException("Wrong endpoint direction");
            }
            switch (receiver.getSlotOwner()) {
                case PORTAL -> {
                    if (portalEndpoint != null) {
                        throw new IllegalArgumentException("Multiple portal endpoints");
                    }
                    portalEndpoint = receiver;
                }
                case WORKER -> workerEndpoints.add(receiver);
            }
        }
        return new Receivers(portalEndpoint, workerEndpoints);
    }

    public List<Endpoint> asList() {
        List<Endpoint> receivers = new ArrayList<>(workerEndpoints);
        if (portalEndpoint != null) {
            receivers.add(portalEndpoint);
        }
        return receivers;
    }

    public List<Endpoint> workerEndpoints() {
        return workerEndpoints;
    }

    @Nullable
    public Endpoint portalEndpoint() {
        return portalEndpoint;
    }

}
