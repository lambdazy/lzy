package ai.lzy.server.task;

import java.net.URI;
import javax.annotation.Nullable;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.basic.SlotStatus;

public class PreparingSlotStatus implements SlotStatus {
    private final String channelName;
    private final Task task;
    private final Slot definition;
    private final String user;

    public PreparingSlotStatus(String user, Task task, Slot definition, String chName) {
        this.user = user;
        this.channelName = chName;
        this.task = task;
        this.definition = definition;
    }

    @Nullable
    @Override
    public String channelId() {
        return channelName;
    }

    @Override
    public String tid() {
        return task.tid();
    }

    @Override
    public Slot slot() {
        return definition;
    }

    @Override
    public URI connected() {
        return null;
    }

    @Override
    public long pointer() {
        return 0;
    }

    @Override
    public State state() {
        return State.PREPARING;
    }
}
