package ru.yandex.cloud.ml.platform.lzy.server.task;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import javax.annotation.Nullable;
import java.net.URI;

public class PreparingSlotStatus implements SlotStatus {
    private final String channelName;
    private final Task task;
    private final Slot definition;

    public PreparingSlotStatus(Task task, Slot definition, String chName) {
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
    public Task task() {
        return task;
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
