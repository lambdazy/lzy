package ai.lzy.channelmanager.channel.v2;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.model.slot.SlotInstance;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class Channel {
    private final String id;
    private final ChannelSpec spec;
    private final String executionId;
    private final Senders senders;
    private final Receivers receivers;

    public Channel(String id, ChannelSpec spec, String executionId) {
        this.id = id;
        this.spec = spec;
        this.executionId = executionId;
        this.senders = new Senders();
        this.receivers = new Receivers();
    }

    public String id() {
        return id;
    }

    public ChannelSpec spec() {
        return spec;
    }

    public String executionId() {
        return executionId;
    }


    private static class Senders {

        @Nullable
        private final SlotInstance workerSlot = null;

        @Nullable
        private final SlotInstance portalSlot = null;

    }

    private static class Receivers {

        private final List<SlotInstance> workerSlots = new ArrayList<>();

        @Nullable
        private final SlotInstance portalSlot = null;

    }
}
