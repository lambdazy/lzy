package ru.yandex.cloud.ml.platform.lzy.server.task;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.UUID;

public class PreparingSlotStatus implements SlotStatus {
    private final UUID channel;
    private final Task task;
    private final Slot definition;

    public PreparingSlotStatus(Task task, Slot definition, UUID channel) {
        this.channel = channel;
        this.task = task;
        this.definition = definition;
    }

    @Nullable
    @Override
    public UUID channelId() {
        return channel;
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
